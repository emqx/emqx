%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Kafka connection configuration
-module(emqx_bridge_kafka_impl).

-export([
    hosts/1,
    make_client_id/2,
    sasl/1,
    socket_opts/1
]).

-include_lib("emqx/include/logger.hrl").

%% Parse comma separated host:port list into a [{Host,Port}] list
hosts(Hosts) when is_binary(Hosts) ->
    hosts(binary_to_list(Hosts));
hosts([#{hostname := _, port := _} | _] = Servers) ->
    %% already parsed
    [{Hostname, Port} || #{hostname := Hostname, port := Port} <- Servers];
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(Hosts).

%% Client ID is better to be unique to make it easier for Kafka side trouble shooting.
make_client_id(BridgeType0, BridgeName0) ->
    BridgeType = to_bin(BridgeType0),
    BridgeName = to_bin(BridgeName0),
    iolist_to_binary([BridgeType, ":", BridgeName, ":", atom_to_list(node())]).

sasl(none) ->
    undefined;
sasl(#{mechanism := Mechanism, username := Username, password := Secret}) ->
    {Mechanism, Username, Secret};
sasl(#{
    kerberos_principal := Principal,
    kerberos_keytab_file := KeyTabFile
}) ->
    {callback, brod_gssapi, {gssapi, KeyTabFile, Principal}}.

%% Extra socket options, such as sndbuf size etc.
socket_opts(Opts) when is_map(Opts) ->
    socket_opts(maps:to_list(Opts));
socket_opts(Opts) when is_list(Opts) ->
    socket_opts_loop(Opts, []).

socket_opts_loop([], Acc) ->
    lists:reverse(Acc);
socket_opts_loop([{tcp_keepalive, KeepAlive} | Rest], Acc) ->
    Acc1 = tcp_keepalive(KeepAlive) ++ Acc,
    socket_opts_loop(Rest, Acc1);
socket_opts_loop([{T, Bytes} | Rest], Acc) when
    T =:= sndbuf orelse T =:= recbuf orelse T =:= buffer
->
    Acc1 = [{T, Bytes} | adjust_socket_buffer(Bytes, Acc)],
    socket_opts_loop(Rest, Acc1);
socket_opts_loop([Other | Rest], Acc) ->
    socket_opts_loop(Rest, [Other | Acc]).

%% https://www.erlang.org/doc/man/inet.html
%% For TCP it is recommended to have val(buffer) >= val(recbuf)
%% to avoid performance issues because of unnecessary copying.
adjust_socket_buffer(Bytes, Opts) ->
    case lists:keytake(buffer, 1, Opts) of
        false ->
            [{buffer, Bytes} | Opts];
        {value, {buffer, Bytes1}, Acc1} ->
            [{buffer, max(Bytes1, Bytes)} | Acc1]
    end.

tcp_keepalive(String) ->
    emqx_schema:tcp_keepalive_opts(String).

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) when is_list(L) ->
    list_to_binary(L);
to_bin(B) when is_binary(B) ->
    B.
