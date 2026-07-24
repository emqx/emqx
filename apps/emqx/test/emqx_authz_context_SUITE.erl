%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_make(_) ->
    ClientInfo = clientinfo(#{
        is_superuser => true,
        peername => {{127, 0, 0, 1}, 1883},
        client_attrs => #{<<"role">> => <<"reader">>},
        custom_authz_field => custom_value
    }),
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        ?assertEqual(ClientInfo, emqx_authz_context:make(ClientInfo))
    end),
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        Context = emqx_authz_context:make(ClientInfo),
        ?assertMatch(
            #{
                is_superuser := true,
                peerport := 1883,
                client_attrs := #{<<"role">> := <<"reader">>}
            },
            Context
        ),
        ?assertNot(maps:is_key(password, Context)),
        ?assertNot(maps:is_key(custom_authz_field, Context))
    end).

t_message_carrier(_) ->
    ClientInfo = clientinfo(#{
        is_superuser => true,
        client_attrs => #{<<"role">> => <<"reader">>},
        conn_state => connected
    }),
    Msg0 = emqx_message:make(<<"clientid">>, <<"$delayed/1/topic">>, <<"payload">>),
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        Msg = emqx_authz_context:maybe_attach(ClientInfo, Msg0),
        ?assertEqual(undefined, emqx_authz_context:get(Msg))
    end),
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        Msg = emqx_authz_context:maybe_attach(ClientInfo, Msg0),
        Context = emqx_authz_context:get(Msg),
        ?assertMatch(#{is_superuser := true}, Context),
        ?assertNot(maps:is_key(password, Context)),
        ?assertNot(maps:is_key(conn_state, Context)),
        ?assertEqual(undefined, emqx_authz_context:get(emqx_authz_context:remove(Msg))),
        NormalMsg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
        ?assertEqual(
            undefined,
            emqx_authz_context:get(emqx_authz_context:maybe_attach(ClientInfo, NormalMsg))
        )
    end).

clientinfo(InitProps) ->
    maps:merge(
        #{
            zone => default,
            listener => 'tcp:default',
            protocol => mqtt,
            peerhost => {127, 0, 0, 1},
            sockport => 1883,
            clientid => <<"clientid">>,
            username => <<"username">>,
            password => <<"passwd">>,
            is_bridge => false,
            is_superuser => false,
            mountpoint => undefined
        },
        InitProps
    ).
