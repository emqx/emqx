%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_channel_client).

-moduledoc """
Even though we already use a pool of `ehttpc` workers, we still need this extra client
process to serialize the API calls, to avoid clobbering the continuation token for each
channel.

Therefore, we spawn one of these clients for each channel used.
""".

%% API
-export([
    start_link/1,

    ensure_channel_opened/1,
    open_channel/1,
    append_rows/2
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% `ecpool_worker' API
-export([connect/1]).

%% Internal exports for this application
-export([channel_name/2]).

%% Internal exports, only for mocking in tests
-export([
    do_open_channel/4,
    do_append_rows/4
]).

-include("emqx_bridge_snowflake.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Calls/casts/infos/continues
-record(append_rows, {records :: [map()]}).
-record(open_channel, {}).
-record(ensure_channel_opened, {}).

-define(continuation_token, continuation_token).
-define(open_channel_error, open_channel_error).
-define(undefined, undefined).

-type start_opts() :: #{
    ecpool_worker_id := pos_integer(),
    ?action_res_id := binary(),
    ?jwt_config := map(),
    ?max_retries := non_neg_integer(),
    ?request_ttl := infinity | non_neg_integer(),
    ?setup_pool_id := binary(),
    ?setup_pool_state := #{
        ?database := binary(),
        ?schema := binary(),
        ?pipe := binary()
    },
    ?write_pool_id := binary()
}.
-type open_channel_error() :: #{response := {_, _, _} | {_, _}, any() => term()}.
-type append_rows_error() :: {recoverable_error | unrecoverable_error, map()} | term().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(start_opts()) -> gen_server:start_ret().
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec ensure_channel_opened(pid()) -> ok | {error, open_channel_error()}.
ensure_channel_opened(Pid) ->
    gen_server:call(Pid, #ensure_channel_opened{}, infinity).

-spec open_channel(pid()) -> ok | {error, open_channel_error()}.
open_channel(Pid) ->
    gen_server:call(Pid, #open_channel{}, infinity).

-spec append_rows(pid(), [map()]) -> {ok, map()} | {error, append_rows_error()}.
append_rows(Pid, Records) ->
    gen_server:call(Pid, #append_rows{records = Records}, infinity).

%%------------------------------------------------------------------------------
%% `ecpool_worker' API
%%------------------------------------------------------------------------------

connect(Opts0) ->
    Opts = maps:from_list(Opts0),
    ?MODULE:start_link(Opts).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    #{
        ecpool_worker_id := Id,
        ?action_res_id := ActionResId,
        ?jwt_config := JWTConfig,
        ?max_retries := MaxRetries,
        ?request_ttl := RequestTTL,
        ?setup_pool_id := SetupPoolId,
        ?setup_pool_state := #{
            ?database := Database,
            ?schema := Schema,
            ?pipe := Pipe
        } = SetupPoolState0,
        ?write_pool_id := WritePoolId
    } = Opts,
    ChannelName = channel_name(ActionResId, Id),
    TemplateContext = #{
        channel => uri_quote(ChannelName),
        database => uri_quote(Database),
        schema => uri_quote(Schema),
        pipe => uri_quote(Pipe)
    },
    OpenChannelPathTemplate =
        emqx_template:parse(
            <<
                "/v2/streaming/databases/${database}/schemas/"
                "${schema}/pipes/${pipe}/channels/${channel}"
            >>
        ),
    OpenChannelPath = emqx_template:render_strict(OpenChannelPathTemplate, TemplateContext),
    AppendRowsPathTemplate =
        emqx_template:parse(
            <<
                "/v2/streaming/data/databases/${database}/schemas/"
                "${schema}/pipes/${pipe}/channels/${channel}/rows"
            >>
        ),
    AppendRowsPath = emqx_template:render_strict(AppendRowsPathTemplate, TemplateContext),
    SetupPoolState = maps:without([?open_channel_path_template], SetupPoolState0),
    State = #{
        ?id => Id,
        ?append_rows_path => AppendRowsPath,
        ?open_channel_path => OpenChannelPath,
        ?continuation_token => <<"">>,
        ?channel_name => ChannelName,
        ?jwt_config => JWTConfig,
        ?max_retries => MaxRetries,
        ?open_channel_error => ?undefined,
        ?request_ttl => RequestTTL,
        ?setup_pool_id => SetupPoolId,
        ?setup_pool_state => SetupPoolState,
        ?write_pool_id => WritePoolId
    },
    {ok, State, {continue, #open_channel{}}}.

handle_continue(#open_channel{}, State0) ->
    {_Reply, State} = do_open_channel(State0),
    {noreply, State}.

handle_call(#ensure_channel_opened{}, _From, State) ->
    %% Just a sync call to ensure the process is initialized; we open the channel in
    %% `handle_continue`.
    #{?open_channel_error := Error} = State,
    Reply =
        maybe
            ?undefined ?= Error,
            ok
        end,
    {reply, Reply, State};
handle_call(#open_channel{}, _From, State0) ->
    {Reply, State} = do_open_channel(State0),
    {reply, Reply, State};
handle_call(#append_rows{records = Records}, _From, State0) ->
    {Reply, State} = do_append_rows(State0, Records),
    {reply, Reply, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports for this application
%%------------------------------------------------------------------------------

channel_name(ActionResId, N) ->
    <<ActionResId/binary, ":", (integer_to_binary(N))/binary>>.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_open_channel(State0) ->
    #{
        ?channel_name := ChannelName,
        ?setup_pool_id := SetupPoolId,
        ?open_channel_path := OpenChannelPath,
        ?setup_pool_state := #{
            ?jwt_config := JWTConfig,
            ?request_ttl := RequestTTL,
            ?max_retries := MaxRetries
        }
    } = State0,
    JWTToken = emqx_connector_jwt:ensure_jwt(JWTConfig),
    AuthnHeader = [<<"BEARER ">>, JWTToken],
    Headers = [
        {<<"X-Snowflake-Authorization-Token-Type">>, <<"KEYPAIR_JWT">>},
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Accept">>, <<"application/json">>},
        {<<"User-Agent">>, <<"emqx">>},
        {<<"Authorization">>, AuthnHeader}
    ],
    Body = <<"{}">>,
    Req = {OpenChannelPath, Headers, Body},
    ?tp(debug, "snowflake_streaming_open_channel_request", #{
        setup_pool_id => SetupPoolId,
        channel_name => ChannelName,
        path => OpenChannelPath
    }),
    Resp = ?MODULE:do_open_channel(SetupPoolId, Req, RequestTTL, MaxRetries),
    maybe
        {ok, 200, _, BodyRaw} ?= emqx_bridge_http_connector:transform_result(Resp),
        #{<<"next_continuation_token">> := ContinuationToken} =
            Response0 = emqx_utils_json:decode(BodyRaw),
        ?tp(debug, "snowflake_streaming_open_channel_success", #{
            channel_name => ChannelName,
            response => Response0
        }),
        State = State0#{
            ?continuation_token := ContinuationToken,
            ?open_channel_error := ?undefined
        },
        {ok, State}
    else
        {ok, Code, RespHeaders, RespBodyRaw} ->
            RespBody =
                case emqx_utils_json:safe_decode(RespBodyRaw) of
                    {ok, JSONErr} -> JSONErr;
                    {error, _} -> RespBodyRaw
                end,
            Error =
                {error, #{
                    reason => <<"unexpected_response_opening_channel">>,
                    response => {Code, RespHeaders, RespBody}
                }},
            State1 = State0#{?open_channel_error := Error},
            {Error, State1};
        Response ->
            Error =
                {error, #{
                    reason => <<"unexpected_response_opening_channel">>,
                    response => Response
                }},
            State1 = State0#{?open_channel_error := Error},
            {Error, State1}
    end.

%% Internal export exposed ONLY for mocking
do_open_channel(HTTPPool, Req, RequestTTL, MaxRetries) ->
    ehttpc:request(HTTPPool, put, Req, RequestTTL, MaxRetries).

do_append_rows(State0, Records) ->
    #{
        ?id := Id,
        ?append_rows_path := AppendRowsPath0,
        ?continuation_token := ContinuationToken,
        ?channel_name := ChannelName,
        ?jwt_config := JWTConfig,
        ?request_ttl := RequestTTL,
        ?max_retries := MaxRetries,
        ?write_pool_id := WritePoolId
    } = State0,
    JWTToken = emqx_connector_jwt:ensure_jwt(JWTConfig),
    Headers = [
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Accept">>, <<"application/json">>},
        {<<"User-Agent">>, <<"emqx">>},
        {<<"Authorization">>, [<<"Bearer ">>, JWTToken]}
    ],
    Body = lists:map(fun(R) -> [emqx_utils_json:encode(R), $\n] end, Records),
    AppendRowsPath = iolist_to_binary([
        AppendRowsPath0,
        <<"?continuationToken=">>,
        uri_quote(ContinuationToken)
    ]),
    Req = {AppendRowsPath, Headers, Body},
    ?tp(debug, "snowflake_streaming_append_rows_request", #{
        write_pool_id => WritePoolId,
        channel => ChannelName,
        path => AppendRowsPath,
        records => Records
    }),
    Resp = ?MODULE:do_append_rows({WritePoolId, Id}, Req, RequestTTL, MaxRetries),
    maybe
        {ok, 200, _Headers, BodyRaw} ?= emqx_bridge_http_connector:transform_result(Resp),
        #{<<"next_continuation_token">> := NextContinuationToken} =
            RespBody = emqx_utils_json:decode(BodyRaw),
        ?SLOG(debug, #{msg => "snowflake_streaming_append_rows_success", response => RespBody}),
        State = State0#{?continuation_token := NextContinuationToken},
        {{ok, Body}, State}
    else
        {error, Reason} ->
            {{error, Reason}, State0};
        Response ->
            Error =
                {error,
                    {unrecoverable_error, #{
                        reason => <<"append_rows_unexpected_response">>, response => Response
                    }}},
            {Error, State0}
    end.

%% Internal export exposed ONLY for mocking
do_append_rows(HTTPPoolAndId, Req, RequestTTL, MaxRetries) ->
    ehttpc:request(HTTPPoolAndId, post, Req, RequestTTL, MaxRetries).

uri_quote(X) -> uri_string:quote(X).
