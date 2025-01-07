%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_kerberos_client).

-export([
    auth_args/2,
    auth_args/3,
    auth_args/4,
    auth_init/1,
    auth_handle/3
]).

-include_lib("emqx/include/emqx_mqtt.hrl").

%% This must match the server principal
%% For this test, the server principal is "mqtt/erlang.emqx.net@KDC.EMQX.NET"
server_fqdn() -> <<"erlang.emqx.net">>.

realm() -> <<"KDC.EMQX.NET">>.

server_principal() ->
    bin(["mqtt/", server_fqdn(), "@", realm()]).

auth_args(ClientKeytab, CLientPrincipal) ->
    auth_args(ClientKeytab, CLientPrincipal, <<"GSSAPI-KERBEROS">>).

auth_args(ClientKeytab, CLientPrincipal, Method) ->
    auth_args(ClientKeytab, CLientPrincipal, Method, undefined).

auth_args(ClientKeytab, CLientPrincipal, Method, FirstToken) ->
    [
        #{
            client_keytab => ClientKeytab,
            client_principal => CLientPrincipal,
            server_fqdn => server_fqdn(),
            server_principal => server_principal(),
            method => Method,
            first_token => FirstToken
        }
    ].

auth_init(#{
    client_keytab := KeytabFile,
    client_principal := ClientPrincipal,
    server_fqdn := ServerFQDN,
    server_principal := ServerPrincipal,
    method := Method,
    first_token := FirstToken
}) ->
    [ClientName, _Realm] = binary:split(ClientPrincipal, <<"@">>),
    ok = sasl_auth:kinit(KeytabFile, ClientPrincipal),
    {ok, ClientHandle} = sasl_auth:client_new(<<"mqtt">>, ServerFQDN, ServerPrincipal, ClientName),
    {ok, {sasl_continue, FirstClientToken}} = sasl_auth:client_start(ClientHandle),
    InitialProps =
        case FirstToken of
            undefined -> props(FirstClientToken, Method);
            Token -> props(Token, Method)
        end,
    State = #{client_handle => ClientHandle, step => 1},
    {InitialProps, State}.

auth_handle(
    #{
        step := 1,
        client_handle := ClientHandle
    } = AuthState,
    Reason,
    Props
) ->
    ct:pal("step-1: auth packet received:\n  rc: ~p\n  props:\n  ~p", [Reason, Props]),
    case {Reason, Props} of
        {continue_authentication, #{'Authentication-Data' := ServerToken}} ->
            {ok, {sasl_continue, ClientToken}} =
                sasl_auth:client_step(ClientHandle, ServerToken),
            OutProps = props(ClientToken),
            NewState = AuthState#{step := 2},
            {continue, {?RC_CONTINUE_AUTHENTICATION, OutProps}, NewState};
        _ ->
            {stop, protocol_error}
    end;
auth_handle(
    #{
        step := 2,
        client_handle := ClientHandle
    },
    Reason,
    Props
) ->
    ct:pal("step-2: auth packet received:\n  rc: ~p\n  props:\n  ~p", [Reason, Props]),
    case {Reason, Props} of
        {continue_authentication, #{'Authentication-Data' := ServerToken}} ->
            {ok, {sasl_ok, ClientToken}} =
                sasl_auth:client_step(ClientHandle, ServerToken),
            OutProps = props(ClientToken),
            NewState = #{done => erlang:system_time()},
            {continue, {?RC_CONTINUE_AUTHENTICATION, OutProps}, NewState};
        _ ->
            {stop, protocol_error}
    end.

props(Data) ->
    props(Data, <<"GSSAPI-KERBEROS">>).

props(Data, Method) ->
    #{
        'Authentication-Method' => Method,
        'Authentication-Data' => Data
    }.

bin(X) -> iolist_to_binary(X).
