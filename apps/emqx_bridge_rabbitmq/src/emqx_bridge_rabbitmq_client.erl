%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_client).

%% Single-host amqp_client wrapper with multi-node connect failover.

-export([
    host_options/0,
    servers_from_config/1,
    rotate_servers/2,
    start_connection/2
]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("emqx/include/logger.hrl").

-define(HOST_OPTIONS, #{
    default_port => 5672,
    ssrf_check => true
}).

-type host_port() :: {string(), inet:port_number()}.
-type amqp_params() :: #amqp_params_network{}.

host_options() -> ?HOST_OPTIONS.

%% `servers` wins when set; otherwise legacy `server`+`port`.
servers_from_config(#{servers := Servers}) when
    Servers =/= undefined, Servers =/= <<>>, Servers =/= ""
->
    parse_servers(Servers);
servers_from_config(#{server := Host, port := Port}) ->
    [{str(Host), Port}].

rotate_servers([], _WorkerId) ->
    [];
rotate_servers(Servers, WorkerId) when is_integer(WorkerId), WorkerId > 0 ->
    Offset = (WorkerId - 1) rem length(Servers),
    {Left, Right} = lists:split(Offset, Servers),
    Right ++ Left;
rotate_servers(Servers, _WorkerId) ->
    Servers.

-spec start_connection([host_port()], amqp_params()) ->
    {ok, pid()} | {error, term()}.
start_connection(Servers, AmqpParamsBase) ->
    do_start_connection(Servers, AmqpParamsBase, []).

parse_servers(BinServers) ->
    [
        {str(Host), Port}
     || #{hostname := Host, port := Port} <- emqx_schema:parse_servers(BinServers, ?HOST_OPTIONS)
    ].

do_start_connection([], _AmqpParamsBase, Tried) ->
    {error, #{reason => all_nodes_failed, tried => lists:reverse(Tried)}};
do_start_connection([{Host, Port} | Rest], AmqpParamsBase, Tried) ->
    Params = AmqpParamsBase#amqp_params_network{host = Host, port = Port},
    case amqp_connection:start(Params) of
        {ok, Conn} ->
            {ok, Conn};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "rabbitmq_connection_node_failed",
                host => Host,
                port => Port,
                reason => Reason
            }),
            do_start_connection(Rest, AmqpParamsBase, [{Host, Port, Reason} | Tried])
    end.

str(X) -> emqx_utils_conv:str(X).
