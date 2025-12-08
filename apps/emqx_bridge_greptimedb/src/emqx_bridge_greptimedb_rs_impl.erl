%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    #{
        server := Server,
        dbname := Database
    } = ConnConfig,
    ClientOpts0 = #{
        pool_name => ConnResId,
        pool_size => erlang:system_info(dirty_io_schedulers),
        pool_type => random,
        auto_reconnect => ?AUTO_RECONNECT_S,
        endpoints => [bin(Server)],
        dbname => Database
    },
    ClientOpts1 =
        case ConnConfig of
            #{username := Username, password := Password} ->
                maps:merge(
                    ClientOpts0,
                    #{
                        username => Username,
                        password => Password
                    }
                );
            _ ->
                ClientOpts0
        end,
    ClientOpts =
        case ConnConfig of
            #{ssl := #{enable := true} = SSLOpts} ->
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
                TLSOpts2 = maps:map(fun(_, V) -> bin(V) end, TLSOpts1),
                TLSOpts = TLSOpts2#{tls => true},
                maps:merge(ClientOpts1, TLSOpts);
            _ ->
                ClientOpts1
        end,
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
on_get_status(_ConnResId, _ConnState) ->
    %% todo: make lib expose a way to health check it.
    ?status_connected.

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
    %% todo: make lib expose a way to health check it.
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
    Context = #{
        res_id => ChanResId,
        iter => Iter,
        last_indices => Indices,
        final_cont => ReplyFnAndArgs0,
        final_res => [],
        client => Client
    },
    ReplyFnAndArgs = {fun ?MODULE:reply_callback/2, [Context]},
    greptimedb_rs:insert_async(Client, Table, Points, ReplyFnAndArgs).

handle_last_async_result(Context, Result) ->
    #{
        last_indices := LastIndices,
        final_cont := ReplyFnAndArgs,
        final_res := ResAcc0
    } = Context,
    ResultsWithIndices = lists:map(fun(I) -> {I, Result} end, LastIndices),
    ResAcc1 = ResultsWithIndices ++ ResAcc0,
    ResAcc2 = lists:keysort(1, ResAcc1),
    {_, FinalResult} = lists:unzip(ResAcc2),
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, FinalResult).

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
