%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_couchbase_connector).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_couchbase.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    resource_type/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3
    %% on_batch_query/3
]).

%% API
-export([
    query/3
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SUCCESS(STATUS_CODE), (STATUS_CODE >= 200 andalso STATUS_CODE < 300)).

%% Ad-hoc requests
-record(sql_query, {sql :: sql(), opts :: ad_hoc_query_opts()}).

-type connector_config() :: #{
    connect_timeout := pos_integer(),
    password := emqx_secret:t(binary()),
    pipelining := pos_integer(),
    pool_size := pos_integer(),
    server := binary(),
    username := binary()
}.
-type connector_state() :: #{
    installed_actions := #{action_resource_id() => action_state()}
}.

-type action_config() :: #{
    parameters := #{
        sql := binary(),
        max_retries := non_neg_integer()
    },
    resource_opts := #{request_ttl := timeout()}
}.
-type action_state() :: #{
    args_template := emqx_template_sql:row_template(),
    max_retries := non_neg_integer(),
    request_ttl := timeout(),
    sql := iolist()
}.

-type query() :: action_query() | ad_hoc_query().
-type action_query() :: {_Tag :: channel_id(), _Data :: map()}.
-type ad_hoc_query() :: #sql_query{}.

-type sql() :: iolist().
-type ad_hoc_query_opts() :: map().

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec resource_type() -> atom().
resource_type() ->
    couchbase.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{
        server := Server,
        connect_timeout := ConnectTimeout,
        password := Password,
        pipelining := Pipelining,
        pool_size := PoolSize,
        username := Username
    } = ConnConfig,
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?SERVER_OPTIONS),
    State = #{
        installed_actions => #{},
        password => Password,
        username => Username
    },
    {Transport, TransportOpts0} =
        case maps:get(ssl, ConnConfig) of
            #{enable := true} = TLSConfig ->
                TLSOpts = emqx_tls_lib:to_client_opts(TLSConfig),
                {tls, TLSOpts};
            _ ->
                {tcp, []}
        end,
    TransportOpts = emqx_utils:ipv6_probe(TransportOpts0),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30_000},
        {pool_type, random},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, TransportOpts},
        {enable_pipelining, Pipelining}
    ],
    case ehttpc_sup:start_pool(ConnResId, PoolOpts) of
        {ok, _} ->
            {ok, State};
        {error, {already_started, _}} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = ehttpc_sup:stop_pool(ConnResId),
    ?tp("couchbase_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, _ConnState) ->
    health_check_pool_workers(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ActionResId, ActionConfig) ->
    ActionState = create_action(ActionConfig),
    ConnState = emqx_utils_maps:deep_put([installed_actions, ActionResId], ConnState0, ActionState),
    {ok, ConnState}.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId, ConnState0 = #{installed_actions := InstalledActions0}, ActionResId
) when
    is_map_key(ActionResId, InstalledActions0)
->
    #{installed_actions := InstalledActions0} = ConnState0,
    InstalledActions = maps:remove(ActionResId, InstalledActions0),
    ConnState = ConnState0#{installed_actions := InstalledActions},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ActionResId) ->
    {ok, ConnState}.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ActionResId,
    _ConnState = #{installed_actions := InstalledActions}
) when is_map_key(ActionResId, InstalledActions) ->
    %% Is it possible to infer table existence and whatnot from an arbitrary statement?
    ?status_connected;
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(ConnResId, {Tag, Data}, #{installed_actions := InstalledActions} = ConnState) when
    is_map_key(Tag, InstalledActions)
->
    ?tp("couchbase_on_query_enter", #{}),
    ActionState = maps:get(Tag, InstalledActions),
    run_action(ConnResId, Data, ActionState, ConnState);
on_query(ConnResId, #sql_query{sql = SQL, opts = Opts}, ConnState) ->
    run_query(ConnResId, SQL, Opts, ConnState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

%% -spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
%%     {ok, _Result} | {error, _Reason}.
%% on_batch_query(_ConnResId, [{Tag, _} | Rest], #{installed_actions := InstalledActions}) when
%%     is_map_key(Tag, InstalledActions)
%% ->
%%     ActionState = maps:get(Tag, InstalledActions),
%%     todo;
%% on_batch_query(_ConnResId, Batch, _ConnState) ->
%%     {error, {unrecoverable_error, {bad_batch, Batch}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec query(connector_resource_id(), _SQL :: iolist(), _Opts :: map()) -> _TODO.
query(ConnResId, SQL, Opts) ->
    emqx_resource:simple_sync_query(ConnResId, #sql_query{sql = SQL, opts = Opts}).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec create_action(action_config()) -> action_state().
create_action(ActionConfig) ->
    #{
        parameters := #{
            sql := SQL0,
            max_retries := MaxRetries
        },
        resource_opts := #{request_ttl := RequestTTL}
    } = ActionConfig,
    {SQL, ArgsTemplate} = emqx_template_sql:parse_prepstmt(SQL0, #{parameters => '$n'}),
    #{
        args_template => ArgsTemplate,
        max_retries => MaxRetries,
        request_ttl => RequestTTL,
        sql => SQL
    }.

-spec health_check_pool_workers(connector_resource_id()) ->
    ?status_connected | ?status_connecting | ?status_disconnected.
health_check_pool_workers(ConnResId) ->
    Timeout = emqx_resource_pool:health_check_timeout(),
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(ConnResId)],
    try
        emqx_utils:pmap(fun(Worker) -> ehttpc:health_check(Worker, Timeout) end, Workers, Timeout)
    of
        [] ->
            ?status_connecting;
        [_ | _] = Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ping(ConnResId, Timeout);
                [{error, Reason} | _] ->
                    ?SLOG(info, #{
                        msg => "couchbase_health_check_failed",
                        reason => emqx_utils:redact(Reason),
                        connector => ConnResId
                    }),
                    ?status_disconnected
            end
    catch
        exit:timeout ->
            ?SLOG(info, #{
                msg => "couchbase_health_check_failed",
                reason => timeout,
                connector => ConnResId
            }),
            ?status_disconnected
    end.

ping(ConnResId, RequestTTL) ->
    MaxRetries = 3,
    Request = {<<"/admin/ping">>, []},
    Response = ehttpc:request(ConnResId, get, Request, RequestTTL, MaxRetries),
    case Response of
        {ok, 200, _Headers, Body0} ->
            case maybe_decode_json(Body0) of
                #{<<"status">> := <<"OK">>} ->
                    ?status_connected;
                _ ->
                    ?tp("couchbase_bad_ping_response", #{response => Response}),
                    ?status_disconnected
            end;
        _ ->
            ?tp("couchbase_bad_ping_response", #{response => Response}),
            ?status_disconnected
    end.

render_args(Context, ArgsTemplate) ->
    %% Missing values will become `null'.
    {Rendered, _Missing} = emqx_template_sql:render_prepstmt(ArgsTemplate, Context),
    Rendered.

auth_header(ConnState) ->
    #{
        username := Username,
        password := Password0
    } = ConnState,
    Password = emqx_secret:unwrap(Password0),
    BasicAuth = base64:encode(<<Username/binary, ":", Password/binary>>),
    {<<"Authorization">>, [<<"Basic ">>, BasicAuth]}.

run_action(ConnResId, Data, ActionState, ConnState) ->
    #{
        args_template := ArgsTemplate,
        sql := SQL,
        request_ttl := RequestTTL,
        max_retries := MaxRetries
    } = ActionState,
    Args = render_args(Data, ArgsTemplate),
    do_query(ConnResId, SQL, Args, RequestTTL, MaxRetries, ConnState).

run_query(ConnResId, SQL, Opts, ConnState) ->
    RequestTTL = maps:get(request_ttl, Opts, timer:seconds(15)),
    MaxRetries = maps:get(max_retries, Opts, 3),
    Args = maps:get(args, Opts, undefined),
    do_query(ConnResId, SQL, Args, RequestTTL, MaxRetries, ConnState).

do_query(ConnResId, SQL, Args, RequestTTL, MaxRetries, ConnState) ->
    Body0 = #{statement => iolist_to_binary(SQL)},
    Body1 = emqx_utils_maps:put_if(Body0, args, Args, Args =/= undefined),
    Body = emqx_utils_json:encode(Body1),
    Request = {
        <<"/query/service">>,
        [
            auth_header(ConnState),
            {<<"Content-Type">>, <<"application/json">>}
        ],
        Body
    },
    Response0 = ehttpc:request(ConnResId, post, Request, RequestTTL, MaxRetries),
    Response = map_response(Response0),
    ?tp("couchbase_response", #{response => Response, request => Request}),
    Response.

maybe_decode_json(Raw) ->
    case emqx_utils_json:safe_decode(Raw, [return_maps]) of
        {ok, JSON} ->
            JSON;
        {error, _} ->
            Raw
    end.

map_response({error, Reason}) when
    Reason =:= econnrefused;
    Reason =:= timeout;
    Reason =:= normal;
    Reason =:= {shutdown, normal};
    Reason =:= {shutdown, closed}
->
    ?tp("couchbase_query_error", #{reason => Reason}),
    {error, {recoverable_error, Reason}};
map_response({error, {closed, _Message} = Reason}) ->
    %% _Message = "The connection was lost."
    ?tp("couchbase_query_error", #{reason => Reason}),
    {error, {recoverable_error, Reason}};
map_response({error, Reason}) ->
    ?tp("couchbase_query_error", #{reason => Reason}),
    {error, {unrecoverable_error, Reason}};
map_response({ok, StatusCode, Headers}) when ?SUCCESS(StatusCode) ->
    ?tp("couchbase_query_success", #{}),
    {ok, #{status_code => StatusCode, headers => Headers}};
map_response({ok, StatusCode, Headers, Body0}) when ?SUCCESS(StatusCode) ->
    %% couchbase returns status = 200 with "status: errors" and "errors" key in body...  🫠
    case maybe_decode_json(Body0) of
        #{<<"status">> := <<"success">>} = Body ->
            ?tp("couchbase_query_success", #{}),
            {ok, #{status_code => StatusCode, headers => Headers, body => Body}};
        Body ->
            ?tp("couchbase_query_error", #{reason => {StatusCode, Headers, Body}}),
            {error,
                {unrecoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}}
    end;
map_response({ok, StatusCode, Headers}) ->
    ?tp("couchbase_query_error", #{reason => {StatusCode, Headers}}),
    {error, {unrecoverable_error, #{status_code => StatusCode, headers => Headers}}};
map_response({ok, StatusCode, Headers, Body0}) ->
    Body = maybe_decode_json(Body0),
    ?tp("couchbase_query_error", #{reason => {StatusCode, Headers, Body}}),
    {error,
        {unrecoverable_error, #{
            status_code => StatusCode, headers => Headers, body => Body
        }}}.
