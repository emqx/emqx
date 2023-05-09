-module(emqx_bridge_greptimedb_connector).
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4
]).

-define(GREPTIMEDB_DEFAULT_PORT, 4001).

-define(GREPTIMEDB_HOST_OPTIONS, #{
    default_port => ?GREPTIMEDB_DEFAULT_PORT
}).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
callback_mode() -> async_if_possible.

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(_InstId, #{client := Client}) ->
    greptimedb:stop_client(Client).

on_get_status(_InstId, _State) ->
    %% FIXME
    connected.

on_query(_InstanceId, {send_message, _Message}, _State) ->
    todo.

on_query_async(_InstanceId, {send_message, _Message}, _ReplyFunAndArgs0, _State) ->
    todo.

on_batch_query(
    _ResourceID,
    _BatchReq,
    _State
) ->
    todo.

on_batch_query_async(
    _InstId,
    _BatchData,
    {_ReplyFun, _Args},
    _State
) ->
    todo.

%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting GreptimeDB connector",
        connector => InstId,
        config => emqx_utils:redact(Config),
        client_config => emqx_utils:redact(ClientConfig)
    }),
    try
        case greptimedb:start_client(ClientConfig) of
            {ok, Client} ->
                {ok, #{client => Client}};
            {error, Reason} ->
                ?tp(greptimedb_connector_start_failed, #{error => Reason}),
                ?SLOG(warning, #{
                    msg => "failed_to_start_greptimedb_connector",
                    connector => InstId,
                    reason => Reason
                }),
                {error, Reason}
        end
    catch
        E:R:S ->
            ?tp(greptimedb_connector_start_exception, #{error => {E, R}}),
            ?SLOG(warning, #{
                msg => "start greptimedb connector error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            }),
            {error, R}
    end.

client_config(
    InstId,
    _Config = #{
        server := Server
    }
) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?GREPTIMEDB_HOST_OPTIONS),
    [
        {endpoints, [{http, str(Host), Port}]},
        {pool_size, erlang:system_info(schedulers)},
        {pool, InstId},
        {pool_type, random}
    ].

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
