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

-type host_port() :: {string(), inet:port_number()}.
-type amqp_params() :: #amqp_params_network{}.

host_options() -> host_options(5672).

%% `server` is normalized to the canonical `servers` field by the schema.
servers_from_config(#{servers := Servers, port := DefaultPort}) ->
    parse_servers(Servers, DefaultPort).

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

parse_servers(BinServers, DefaultPort) ->
    [
        {emqx_utils_conv:str(Host), Port}
     || #{hostname := Host, port := Port} <-
            emqx_schema:parse_servers(BinServers, host_options(DefaultPort))
    ].

host_options(DefaultPort) -> #{default_port => DefaultPort, ssrf_check => true}.

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
