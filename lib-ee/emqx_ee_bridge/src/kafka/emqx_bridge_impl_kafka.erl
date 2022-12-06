%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Kafka connection configuration
-module(emqx_bridge_impl_kafka).

-export([
    hosts/1,
    make_client_id/1,
    sasl/1
]).

%% Parse comma separated host:port list into a [{Host,Port}] list
hosts(Hosts) when is_binary(Hosts) ->
    hosts(binary_to_list(Hosts));
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(Hosts).

%% Client ID is better to be unique to make it easier for Kafka side trouble shooting.
make_client_id(BridgeName) when is_atom(BridgeName) ->
    make_client_id(atom_to_list(BridgeName));
make_client_id(BridgeName) ->
    iolist_to_binary([BridgeName, ":", atom_to_list(node())]).

sasl(none) ->
    undefined;
sasl(#{mechanism := Mechanism, username := Username, password := Password}) ->
    {Mechanism, Username, emqx_secret:wrap(Password)};
sasl(#{
    kerberos_principal := Principal,
    kerberos_keytab_file := KeyTabFile
}) ->
    {callback, brod_gssapi, {gssapi, KeyTabFile, Principal}}.
