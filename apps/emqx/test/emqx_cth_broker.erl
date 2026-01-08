%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cth_broker).

-compile(export_all).
-compile(nowarn_export_all).

-spec connection_info(_Info, pid() | emqx_types:clientid()) -> _Value.
connection_info(Info, Client) ->
    {ConnMod, ChanPid} = connection_chanmod(Client),
    get_connection_info(Info, ConnMod, sys:get_state(ChanPid)).

-spec connection_stats(pid() | emqx_types:clientid()) -> _Value.
connection_stats(Client) ->
    {ConnMod, ChanPid} = connection_chanmod(Client),
    ConnMod:stats(ChanPid).

-spec connection_state(pid() | emqx_types:clientid()) -> _Value.
connection_state(Client) ->
    {ConnMod, ChanPid} = connection_chanmod(Client),
    ConnMod:get_state(ChanPid).

connection_chanmod(Client) when is_pid(Client) ->
    connection_chanmod(emqtt_info(clientid, Client));
connection_chanmod(ClientId) ->
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ConnMod = emqx_cm:do_get_chann_conn_mod(ClientId, ChanPid),
    {ConnMod, ChanPid}.

get_connection_info(connmod, ConnMod, _State) ->
    ConnMod;
get_connection_info(Info, emqx_connection, State) ->
    emqx_connection:info(Info, State);
get_connection_info(Info, emqx_socket_connection, State) ->
    emqx_socket_connection:info(Info, State);
get_connection_info(Info, emqx_ws_connection, {_WSState, ConnState, _}) ->
    emqx_ws_connection:info(Info, ConnState).

emqtt_info(Key, Client) ->
    proplists:get_value(Key, emqtt:info(Client), undefined).
