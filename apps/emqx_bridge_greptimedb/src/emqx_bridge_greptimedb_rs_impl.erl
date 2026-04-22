%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_rs_impl).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

%% `emqx_resource` API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4
]).

%% API
-export([]).

%% Internal exports
-export([reply_callback/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AUTO_RECONNECT_S, 2).

%% Allocatable resources
-define(greptimedb_client, greptimedb_client).

-define(client, client).
-define(database, database).
-define(installed_channels, installed_channels).
-define(write_syntax, write_syntax).
-define(precision, precision).

-type connector_config() :: #{
    dbname := database(),
    server := binary(),
    username => binary(),
    password => emqx_secret:t(binary())
}.
-type connector_state() :: #{
    ?client := greptimedb_rs:client(),
    ?database := binary(),
    ?installed_channels := #{channel_id() => channel_state()}
}.

-type database() :: binary().

-type channel_config() :: #{
    parameters := #{
        write_syntax := [map()],
        precision := ns | us | ms | s
    }
}.
-type channel_state() :: #{}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    greptimedb.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    async_if_possible.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{dbname := Database} = ConnConfig,
    ensure_consistent_ssl_opts(ConnConfig),
    ClientOpts = client_opts(ConnResId, ConnConfig),
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, ?greptimedb_client, ConnResId),
    maybe
        {ok, Client} ?= greptimedb_rs:start_client(ClientOpts),
        ConnState = #{
            ?client => Client,
            ?database => Database,
            ?installed_channels => #{}
        },
        {ok, ConnState}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = greptimedb_rs:stop_client(#{pool_name => ConnResId}),
    ?tp("greptimedb_rs_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(_ConnResId, #{?client := Client} = _ConnState) ->
    %% todo: make lib expose a way to health check it.
    try greptimedb_rs:query(Client, <<"select 1">>) of
        {ok, _} ->
            ?status_connected;
        {error, Reason} ->
            {?status_disconnected, Reason}
    catch
        Kind:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "greptimedb_rs_connector_health_check_exception",
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {?status_disconnected, {Kind, Reason}}
    end.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ChanResId, ActionConfig) ->
    #{
        parameters := #{
            write_syntax := WriteSyntaxTemplate,
            precision := Precision
        }
    } = ActionConfig,
    ActionState = #{
        ?write_syntax => emqx_bridge_greptimedb_utils:parse_write_syntax(
            WriteSyntaxTemplate, Precision
        ),
        ?precision => Precision
    },
    ConnState = maps:update_with(
        ?installed_channels,
        fun(Cs) -> Cs#{ChanResId => ActionState} end,
        ConnState0
    ),
    {ok, ConnState}.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId,
    ConnState0 = #{?installed_channels := InstalledChannels0},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels0)
->
    InstalledChannels = maps:remove(ChanResId, InstalledChannels0),
    ConnState = ConnState0#{?installed_channels := InstalledChannels},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ChanResId,
    _ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    %% there seems to be no particular status for an action of this kind
    ?status_connected;
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    case parse_batch_data(ConnResId, Database, [{ChanResId, Data}], WriteSyntax) of
        {ok, TablesToPoints} ->
            [Result] = do_insert_sync(TablesToPoints, ConnState),
            Result;
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_query_async(
    connector_resource_id(),
    {message_tag(), map()},
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) -> {ok, pid()} | {error, term()}.
on_query_async(
    ConnResId,
    {ChanResId, #{} = Data},
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    case parse_batch_data(ConnResId, Database, [{ChanResId, Data}], WriteSyntax) of
        {ok, TablesToPoints} ->
            do_insert_async(ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs);
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ChanResId, _Data} | _] = Batch,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    case parse_batch_data(ConnResId, Database, Batch, WriteSyntax) of
        {ok, TablesToPoints} ->
            do_insert_sync(TablesToPoints, ConnState);
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

-spec on_batch_query_async(
    connector_resource_id(),
    [query()],
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) ->
    ok | {error, term()}.
on_batch_query_async(
    ConnResId,
    [{ChanResId, _Data} | _] = Batch,
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{?database := Database} = ConnState,
    #{ChanResId := ActionState} = InstalledChannels,
    #{?write_syntax := WriteSyntax} = ActionState,
    case parse_batch_data(ConnResId, Database, Batch, WriteSyntax) of
        {ok, TablesToPoints} ->
            do_insert_async(ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs);
        {error, ErrorPoints} ->
            {error, ErrorPoints}
    end;
on_batch_query_async(_ConnResId, Queries, _ReplyFnAndArgs, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

reply_callback(Context0, Result) ->
    #{iter := Iter0} = Context0,
    case maps:next(Iter0) of
        none ->
            handle_last_async_result(Context0, Result);
        {NextTable, PointsWithIndices, Iter} ->
            Context = Context0#{iter := Iter},
            handle_async_result_continuation(Context, Result, NextTable, PointsWithIndices)
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

bin(X) -> emqx_utils_conv:bin(X).

parse_batch_data(ConnResId, Database, Batch, WriteSyntax) ->
    maybe
        {ok, Points0} ?=
            emqx_bridge_greptimedb_utils:parse_batch_data(
                ConnResId, Database, Batch, WriteSyntax, native
            ),
        %% We need to reassemble the results later in the same order.
        Points1 = lists:enumerate(Points0),
        TablesToPoints0 = maps:groups_from_list(
            fun({_, {#{table := Table}, _}}) -> Table end,
            fun({I, {_, Points}}) ->
                lists:map(fun(P) -> {I, P} end, Points)
            end,
            Points1
        ),
        TablesToPoints = maps:map(
            fun(_, Pss) -> lists:append(Pss) end,
            TablesToPoints0
        ),
        {ok, TablesToPoints}
    end.

do_insert_sync(TablesToPoints, ConnState) ->
    #{?client := Client} = ConnState,
    Results0 =
        maps:fold(
            fun(Table, PointsWithIndices, Acc0) ->
                {Indices, Points} = lists:unzip(PointsWithIndices),
                Res0 = greptimedb_rs:insert(Client, Table, Points),
                Res = lists:map(fun(I) -> {I, Res0} end, Indices),
                [Res | Acc0]
            end,
            [],
            TablesToPoints
        ),
    Results1 = lists:flatten(Results0),
    Results2 = lists:keysort(1, Results1),
    {_, Results} = lists:unzip(Results2),
    ?tp("greptime_rs_sync_batch_reply", #{results => Results}),
    Results.

do_insert_async(_ChanResId, TablesToPoints, _ConnState, ReplyFnAndArgs) when
    map_size(TablesToPoints) == 0
->
    %% Should be impossible, since we only trigger action with non-empty batches.
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, {ok, #{}}),
    {ok, self()};
do_insert_async(ChanResId, TablesToPoints, ConnState, ReplyFnAndArgs0) ->
    #{?client := Client} = ConnState,
    Iter0 = maps:iterator(TablesToPoints),
    %% Already checked that map is non-empty.
    {Table, PointsWithIndices, Iter} = maps:next(Iter0),
    {Indices, Points} = lists:unzip(PointsWithIndices),
    IsBatch =
        case {map_size(TablesToPoints), PointsWithIndices} of
            {1, [_]} ->
                false;
            _ ->
                true
        end,
    Context = #{
        res_id => ChanResId,
        iter => Iter,
        last_indices => Indices,
        final_cont => ReplyFnAndArgs0,
        final_res => [],
        client => Client,
        is_batch => IsBatch
    },
    ReplyFnAndArgs = {fun ?MODULE:reply_callback/2, [Context]},
    greptimedb_rs:insert_async(Client, Table, Points, ReplyFnAndArgs).

handle_last_async_result(Context, Result) ->
    #{
        last_indices := LastIndices,
        final_cont := ReplyFnAndArgs,
        final_res := ResAcc0,
        is_batch := IsBatch
    } = Context,
    ResultsWithIndices = lists:map(fun(I) -> {I, Result} end, LastIndices),
    ResAcc1 = ResultsWithIndices ++ ResAcc0,
    ResAcc2 = lists:keysort(1, ResAcc1),
    {_, FinalResult0} = lists:unzip(ResAcc2),
    case IsBatch of
        true ->
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, FinalResult0);
        false ->
            [FinalResult1] = FinalResult0,
            emqx_resource:apply_reply_fun(ReplyFnAndArgs, FinalResult1)
    end.

handle_async_result_continuation(Context0, Result, NextTable, PointsWithIndices) ->
    #{
        client := Client,
        last_indices := LastIndices,
        final_res := ResAcc0
    } = Context0,
    {NextIndices, Points} = lists:unzip(PointsWithIndices),
    ResultsWithIndices = lists:map(fun(I) -> {I, Result} end, LastIndices),
    ResAcc = ResultsWithIndices ++ ResAcc0,
    Context = Context0#{last_indices := NextIndices, final_res := ResAcc},
    ReplyFnAndArgs = {fun ?MODULE:reply_callback/2, [Context]},
    greptimedb_rs:insert_async(Client, NextTable, Points, ReplyFnAndArgs).

%% @doc Build the options map for `greptimedb_rs:start_client/1'.
client_opts(ConnResId, ConnConfig) ->
    #{server := Server0, dbname := Database} = ConnConfig,
    %% Replicate greptime connector behavior: 4001 is the default port.
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server0, #{
        default_port => 4001
    }),
    Server = iolist_to_binary([Host, ":", integer_to_binary(Port)]),
    Opts0 = #{
        pool_name => ConnResId,
        pool_size => erlang:system_info(dirty_io_schedulers),
        pool_type => random,
        auto_reconnect => ?AUTO_RECONNECT_S,
        endpoints => [bin(Server)],
        dbname => Database
    },
    Opts1 = maybe_put_auth(ConnConfig, Opts0),
    Opts2 = maybe_put_tls(ConnConfig, Opts1),
    maybe_put_optional_keys([ts_column, ttl], ConnConfig, Opts2).

maybe_put_auth(#{username := Username, password := Password}, Opts) ->
    Opts#{username => Username, password => Password};
maybe_put_auth(_ConnConfig, Opts) ->
    Opts.

maybe_put_tls(#{ssl := #{enable := true} = SSLOpts}, Opts) ->
    TLSOpts = extract_tls_opts(SSLOpts),
    maps:merge(Opts, TLSOpts#{tls => true});
maybe_put_tls(_ConnConfig, Opts) ->
    Opts.

extract_tls_opts(SSLOpts) ->
    TLSCertOpts = extract_tls_cert_opts(SSLOpts),
    maybe_put_tls_verify_opt(SSLOpts, TLSCertOpts).

extract_tls_cert_opts(SSLOpts) ->
    TLSOpts0 = maps:fold(
        fun emqx_utils_maps:rename/3,
        SSLOpts,
        #{
            cacertfile => ca_cert,
            certfile => client_cert,
            keyfile => client_key
        }
    ),
    TLSOpts1 = maps:with([ca_cert, client_cert, client_key], TLSOpts0),
    TLSOpts2 = maps:filter(
        fun(_, V) -> not is_blank_or_undefined(V) end,
        TLSOpts1
    ),
    maps:map(fun(_, V) -> bin(V) end, TLSOpts2).

maybe_put_tls_verify_opt(SSLOpts, TLSOpts) ->
    case maps:find(verify, SSLOpts) of
        {ok, Verify0} ->
            case normalize_tls_verify(Verify0) of
                undefined -> TLSOpts;
                Verify -> TLSOpts#{verify => Verify}
            end;
        error ->
            case maps:find(<<"verify">>, SSLOpts) of
                {ok, Verify0} ->
                    case normalize_tls_verify(Verify0) of
                        undefined -> TLSOpts;
                        Verify -> TLSOpts#{verify => Verify}
                    end;
                error ->
                    TLSOpts
            end
    end.

normalize_tls_verify(verify_none) ->
    verify_none;
normalize_tls_verify(<<"verify_none">>) ->
    verify_none;
normalize_tls_verify("verify_none") ->
    verify_none;
normalize_tls_verify(verify_peer) ->
    verify_peer;
normalize_tls_verify(<<"verify_peer">>) ->
    verify_peer;
normalize_tls_verify("verify_peer") ->
    verify_peer;
normalize_tls_verify(_) ->
    undefined.

maybe_put_optional_keys(Keys, Source, Target) ->
    lists:foldl(
        fun(Key, Acc) ->
            case Source of
                #{Key := Val} -> Acc#{Key => Val};
                _ -> Acc
            end
        end,
        Target,
        Keys
    ).

ensure_consistent_ssl_opts(#{ssl := #{enable := true} = SSLOpts} = _ConnConfig) ->
    case normalize_tls_verify(get_ssl_verify_opt(SSLOpts, verify_peer)) of
        verify_none ->
            ok;
        _ ->
            AnyMissing =
                lists:any(
                    fun(Key) -> is_blank_or_missing(Key, SSLOpts) end,
                    [cacertfile, certfile, keyfile]
                ),
            case AnyMissing of
                true ->
                    throw(<<
                        "cacertfile, certfile and keyfile SSL options must be configured"
                        " when SSL is enabled."
                    >>);
                false ->
                    ok
            end
    end;
ensure_consistent_ssl_opts(_ConnConfig) ->
    ok.

get_ssl_verify_opt(SSLOpts, Default) ->
    case maps:find(verify, SSLOpts) of
        {ok, Verify} ->
            Verify;
        error ->
            case maps:find(<<"verify">>, SSLOpts) of
                {ok, Verify} -> Verify;
                error -> Default
            end
    end.

is_blank_or_missing(Key, Cfg) ->
    is_blank_or_undefined(maps:get(Key, Cfg, undefined)).

is_blank_or_undefined(undefined) ->
    true;
is_blank_or_undefined(null) ->
    true;
is_blank_or_undefined(V) when is_binary(V) ->
    byte_size(V) =:= 0;
is_blank_or_undefined(V) when is_list(V) ->
    V =:= [];
is_blank_or_undefined(_) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

client_opts_tls_verify_none_without_cert_paths_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{
            enable => true,
            verify => verify_none,
            cacertfile => <<>>,
            certfile => <<>>,
            keyfile => <<>>
        }
    },
    Opts = client_opts(test_res_id, ConnConfig),
    ?assertEqual(true, maps:get(tls, Opts)),
    ?assertEqual(verify_none, maps:get(verify, Opts)),
    ?assertNot(maps:is_key(ca_cert, Opts)),
    ?assertNot(maps:is_key(client_cert, Opts)),
    ?assertNot(maps:is_key(client_key, Opts)).

client_opts_tls_verify_peer_keeps_certs_test() ->
    ConnConfig = #{
        server => <<"127.0.0.1:5001">>,
        dbname => <<"public">>,
        ssl => #{
            enable => true,
            verify => verify_peer,
            cacertfile => <<"/tmp/ca.crt">>,
            certfile => <<"/tmp/client.crt">>,
            keyfile => <<"/tmp/client.key">>
        }
    },
    Opts = client_opts(test_res_id, ConnConfig),
    ?assertEqual(true, maps:get(tls, Opts)),
    ?assertEqual(verify_peer, maps:get(verify, Opts)),
    ?assertEqual(<<"/tmp/ca.crt">>, maps:get(ca_cert, Opts)),
    ?assertEqual(<<"/tmp/client.crt">>, maps:get(client_cert, Opts)),
    ?assertEqual(<<"/tmp/client.key">>, maps:get(client_key, Opts)).

-endif.
