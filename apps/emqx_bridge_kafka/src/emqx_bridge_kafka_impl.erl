%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Kafka connection configuration
-module(emqx_bridge_kafka_impl).

-export([
    hosts/1,
    make_client_id/2,
    sasl/1
]).

%% Parse comma separated host:port list into a [{Host,Port}] list
hosts(Hosts) when is_binary(Hosts) ->
    hosts(binary_to_list(Hosts));
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(Hosts).

%% Client ID is better to be unique to make it easier for Kafka side trouble shooting.
make_client_id(KafkaType0, BridgeName0) ->
    KafkaType = to_bin(KafkaType0),
    BridgeName = to_bin(BridgeName0),
    iolist_to_binary([KafkaType, ":", BridgeName, ":", atom_to_list(node())]).

sasl(none) ->
    undefined;
sasl(#{mechanism := Mechanism, username := Username, password := Password}) ->
    {Mechanism, Username, emqx_secret:wrap(Password)};
sasl(#{
    kerberos_principal := Principal,
    kerberos_keytab_file := KeyTabFile
}) ->
    {callback, brod_gssapi, {gssapi, KeyTabFile, Principal}}.

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) when is_list(L) ->
    list_to_binary(L);
to_bin(B) when is_binary(B) ->
    B.
