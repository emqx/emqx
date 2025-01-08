%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_cth).

-compile(export_all).
-compile(nowarn_export_all).

%%

-spec connect_client_unlink(_ClientID :: binary(), node()) -> _Pid :: emqtt:client().
connect_client_unlink(ClientId, Node) ->
    Client = connect_client(ClientId, Node),
    _ = erlang:unlink(Client),
    Client.

-spec connect_client(_ClientID :: binary(), node()) -> _Pid :: emqtt:client().
connect_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

-spec disconnect_client(emqtt:client()) -> ok.
disconnect_client(Pid) ->
    emqtt:disconnect(Pid).

tcp_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.
