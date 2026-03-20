%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_quasardb_impl).

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
    on_batch_query/3
]).

%% API
-export([]).

%% `ecpool_worker' API and callbacks
-export([
    connect/1,
    disconnect/1,

    do_health_check_connector/1
]).

-ifdef(TEST).
-export([conn_str/1]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AUTO_RECONNECT_INTERVAL_S, 2).

%% Allocatable resources

-define(installed_channels, installed_channels).
-define(health_check_timeout, health_check_timeout).

-define(health_check_table, health_check_table).
-define(insert, insert).
-define(values_template, values_template).

-define(undefined, undefined).

-type connector_config() :: #{
    dsn := binary(),
    uri := binary(),
    pool_size := pos_integer(),
    resource_opts := map(),
    _ => _
}.
-type connector_state() :: #{
    ?health_check_timeout := pos_integer(),
    ?installed_channels := #{channel_id() => channel_state()}
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{
    ?health_check_table := ?undefined | binary(),
    ?insert := binary(),
    ?values_template := emqx_template:t()
}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    quasardb.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    #{
        uri := URI,
        dsn := DSN,
        pool_size := PoolSize,
        connect_timeout := ConnectTimeout,
        resource_opts := #{health_check_timeout := HCTimeout}
    } = ConnConfig,
    Username = maps:get(username, ConnConfig, undefined),
    Password = maps:get(password, ConnConfig, undefined),
    ClusterPublicKey = maps:get(cluster_public_key, ConnConfig, undefined),
    DriverOpts = [{timeout, ConnectTimeout}],
    ConnectOptions0 = [
        {uri, URI},
        {username, Username},
        {password, Password},
        {cluster_public_key, ClusterPublicKey},
        {dsn, DSN},
        {pool_size, PoolSize},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL_S},
        {on_disconnect, {?MODULE, disconnect, []}},
        {driver_options, DriverOpts}
    ],
    ConnectOptions = lists:filter(fun({_K, V}) -> V /= undefined end, ConnectOptions0),
    ConnState = #{
        ?health_check_timeout => HCTimeout,
        ?installed_channels => #{}
    },
    case emqx_resource_pool:start(ConnResId, ?MODULE, ConnectOptions) of
        ok ->
            {ok, ConnState};
        {error, Reason} ->
            {error, Reason}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    ?tp("quasardb_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, ConnState) ->
    #{?health_check_timeout := HCTimeout} = ConnState,
    Opts = #{
        check_fn => fun ?MODULE:do_health_check_connector/1,
        timeout => HCTimeout
    },
    emqx_resource_pool:common_health_check_workers(ConnResId, Opts).

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
    maybe
        {ok, ChanState} ?= create_action(ActionConfig),
        #{?installed_channels := InstalledChannels0} = ConnState0,
        InstalledChannels = InstalledChannels0#{ChanResId => ChanState},
        ConnState = ConnState0#{?installed_channels := InstalledChannels},
        {ok, ConnState}
    end.

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
    ConnResId,
    ChanResId,
    _ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    #{ChanResId := ChanState} = InstalledChannels,
    maybe_check_table_existence(ConnResId, ChanState);
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = _ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    do_query(ConnResId, [Data], ChanState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ChanResId, _} | _] = Queries,
    #{?installed_channels := InstalledChannels} = _ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    #{ChanResId := ChanState} = InstalledChannels,
    Batch = [Data || {_, Data} <- Queries],
    do_query(ConnResId, Batch, ChanState);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `ecpool_worker' API
%%------------------------------------------------------------------------------

connect(Opts) ->
    ConnectStr = conn_str(Opts),
    %% Note: we don't use `emqx_secret:wrap/1` here because its return type is opaque, and
    %% dialyzer then complains that it's being fed to a function that doesn't expect
    %% something opaque...
    ConnectStrWrapped = fun() -> ConnectStr end,
    DriverOpts = proplists:get_value(driver_options, Opts, []),
    odbc:connect(ConnectStrWrapped, DriverOpts).

disconnect(ConnectionPid) ->
    odbc:disconnect(ConnectionPid).

do_health_check_connector(ConnectionPid) ->
    case odbc:sql_query(ConnectionPid, "select 1") of
        {selected, _, _} ->
            ok;
        {error, Reason} ->
            {error, Reason};
        Error ->
            {error, Error}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

str(X) -> emqx_utils_conv:str(X).

conn_str(Opts) ->
    lists:concat(conn_str(Opts, [])).

conn_str([], Acc) ->
    lists:join(";", Acc);
conn_str([{dsn, DSN} | Opts], Acc) ->
    conn_str(Opts, ["dsn=" ++ str(DSN) | Acc]);
conn_str([{uri, URI} | Opts], Acc) ->
    conn_str(Opts, ["uri=" ++ str(URI) | Acc]);
conn_str([{username, UID} | Opts], Acc) ->
    conn_str(Opts, ["uid=" ++ str(UID) | Acc]);
conn_str([{password, Password} | Opts], Acc) ->
    conn_str(Opts, ["pwd=" ++ str(emqx_secret:unwrap(Password)) | Acc]);
conn_str([{cluster_public_key, ClusterPublicKey} | Opts], Acc) ->
    conn_str(Opts, ["key=" ++ str(ClusterPublicKey) | Acc]);
conn_str([{_, _} | Opts], Acc) ->
    conn_str(Opts, Acc).

create_action(ActionConfig) ->
    #{
        parameters := #{
            sql := SQL
        } = Params
    } = ActionConfig,
    HCTable = maps:get(health_check_table, Params, ?undefined),
    case emqx_utils_sql:get_statement_type(SQL) of
        insert ->
            case emqx_utils_sql:split_insert(SQL) of
                {ok, {Insert, Values}} ->
                    ValuesTemplate = emqx_template:parse(Values),
                    ChanState = #{
                        ?health_check_table => HCTable,
                        ?insert => Insert,
                        ?values_template => ValuesTemplate
                    },
                    {ok, ChanState};
                {ok, {_InsertPart, _Values, OnClause}} ->
                    {error, {unsupported_clause, OnClause}};
                {error, Reason} ->
                    {error, {failed_to_parse, Reason}}
            end;
        {error, Reason} ->
            {error, {failed_to_parse_type, Reason}};
        Type ->
            {error, {invalid_sql_type, Type}}
    end.

do_query(ConnResId, Data, ChanState) ->
    #{
        ?insert := Insert,
        ?values_template := ValuesTemplate
    } = ChanState,
    Values0 = lists:map(fun(X) -> render_value(X, ValuesTemplate) end, Data),
    Values = lists:join(",", Values0),
    SQL = [Insert, " values ", Values],
    Res = ecpool:pick_and_do(
        ConnResId,
        fun(ConnectionPid) ->
            odbc:sql_query(ConnectionPid, SQL)
        end,
        handover
    ),
    handle_insert_result(Res).

maybe_check_table_existence(_ConnResId, #{?health_check_table := ?undefined} = _ChanState) ->
    ?status_connected;
maybe_check_table_existence(ConnResId, #{?health_check_table := Table} = _ChanState) ->
    SQL = [<<"show table ">>, Table],
    Res = ecpool:pick_and_do(
        ConnResId,
        fun(ConnectionPid) ->
            odbc:sql_query(ConnectionPid, SQL)
        end,
        handover
    ),
    case Res of
        {selected, _, _} ->
            %% Table exists and "happens" to return a definition that OTP ODBC driver
            %% understands...
            ?status_connected;
        {error, "Unknown column type"} ->
            %% Yes...  When the table exists, depending on its column definitions, the OTP
            %% ODBC driver returns an `Unknown column type` error...
            ?status_connected;
        {error, Msg} when is_list(Msg) ->
            case
                re:run(
                    Msg,
                    <<"entry matching the provided alias cannot be found">>,
                    [{capture, none}]
                )
            of
                match ->
                    {?status_disconnected, {unhealthy_target, <<"Table not found">>}};
                nomatch ->
                    {?status_disconnected, iolist_to_binary(Msg)}
            end;
        Error ->
            {?status_disconnected, Error}
    end.

render_value(X, ValuesTemplate) ->
    %% Unknown/undefined values are rendered as null
    VarTrans = fun
        (undefined) -> <<"null">>;
        (B) when is_binary(B) -> B;
        (Y) -> emqx_utils_conv:bin(Y)
    end,
    {Rendered, _} = emqx_template:render(ValuesTemplate, {emqx_jsonish, X}, #{var_trans => VarTrans}),
    Rendered.

handle_insert_result({error, Msg}) when is_list(Msg) ->
    case re:run(Msg, <<"SQLSTATE IS: 01000">>, [{capture, none}]) of
        match ->
            %% Yes...  A successful result for this integration is interpreted by the OTP
            %% ODBC wrapper as an error...  with the following message:
            %%
            %% "General warning: no results SQLSTATE IS: 01000"
            ok;
        nomatch ->
            {error, {unrecoverable_error, Msg}}
    end;
handle_insert_result(X) ->
    %% Impossible?
    {error, {unrecoverable_error, X}}.
