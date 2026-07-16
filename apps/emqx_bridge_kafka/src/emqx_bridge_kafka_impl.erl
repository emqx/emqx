%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Kafka connection configuration
-module(emqx_bridge_kafka_impl).

-export([
    hosts/1,
    sasl/1,
    socket_opts/1,
    register_oauth2/2
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

sasl(none) ->
    undefined;
sasl(msk_iam) ->
    {callback, brod_oauth, #{
        token_callback => fun emqx_bridge_kafka_msk_iam_authn:token_callback/1
    }};
sasl(#{mechanism := oauth, grant_type := client_credentials} = Opts) ->
    Extensions = emqx_utils_maps:binary_key_map(maps:get(extensions, Opts, #{})),
    {callback, brod_oauth, #{
        token_callback => fun(#{client_id := KafkaClientId}) ->
            case emqx_connector_oauth2:get_token(KafkaClientId) of
                {ok, Token} -> {ok, #{token => Token}};
                {error, Reason} -> {error, Reason}
            end
        end,
        extensions => Extensions
    }};
sasl(#{mechanism := Mechanism, username := Username, password := Secret}) ->
    {Mechanism, Username, Secret};
sasl(#{
    kerberos_principal := Principal,
    kerberos_keytab_file := KeyTabFile
}) ->
    {callback, brod_gssapi, {gssapi, KeyTabFile, Principal}}.

%% Registers the OAuth2 client-credentials config (if any) with the shared
%% `emqx_connector_oauth2` token cache, keyed by the brod/wolff client id.
%% Must be called before the client connects, so the token callback invoked
%% during the SASL handshake can retrieve the token.  No-op for non-oauth auth.
-spec register_oauth2(term(), term()) -> ok.
register_oauth2(ClientId, #{mechanism := oauth, grant_type := client_credentials} = Opts) ->
    Config = #{
        token_endpoint => maps:get(endpoint_uri, Opts),
        client_id => maps:get(client_id, Opts),
        client_secret => maps:get(client_secret, Opts),
        scope => maps:get(scope, Opts, undefined),
        timeout => maps:get(timeout, Opts, 5_000)
    },
    emqx_connector_oauth2:register(ClientId, Config);
register_oauth2(_ClientId, _Auth) ->
    ok.

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
