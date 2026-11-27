%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_channel_tests).

-include_lib("eunit/include/eunit.hrl").

-define(LISTENER, <<"coap:dtls:default">>).

%% Client info of a channel that was just created for a new connection.
fresh_clientinfo() ->
    #{
        zone => default,
        listener => ?LISTENER,
        protocol => coap,
        peerhost => {10, 0, 0, 2},
        peername => {{10, 0, 0, 2}, 40002},
        sockport => 5684,
        clientid => <<"random-id">>,
        username => undefined,
        is_bridge => false,
        is_superuser => false,
        enable_authn => true,
        mountpoint => <<>>
    }.

%% Client info that the token check returns for the session to resume.
resume_clientinfo() ->
    #{
        zone => default,
        listener => ?LISTENER,
        protocol => coap,
        peerhost => {10, 0, 0, 1},
        peername => {{10, 0, 0, 1}, 40001},
        sockport => 5684,
        clientid => <<"client1">>,
        username => <<"user1">>,
        is_bridge => false,
        is_superuser => false,
        enable_authn => true,
        mountpoint => <<"mp/user1/">>,
        auth_expire_at => 1000,
        acl => #{rules => #{pub => [<<"t/user1/#">>]}},
        client_attrs => #{<<"group">> => <<"g1">>}
    }.

merge(ClientInfo0, ResumeClientInfo) ->
    emqx_coap_channel:merge_takeover_clientinfo(<<"client1">>, ClientInfo0, ResumeClientInfo).

takeover_in_fresh_channel_test() ->
    Resume = resume_clientinfo(),
    Fresh = fresh_clientinfo(),
    Result = merge(Fresh, Resume),
    ?assertEqual(<<"client1">>, maps:get(clientid, Result)),
    lists:foreach(
        fun(Key) ->
            ?assertEqual({Key, maps:get(Key, Resume)}, {Key, maps:get(Key, Result, missing)})
        end,
        [acl, client_attrs, username, is_superuser, auth_expire_at, mountpoint, enable_authn]
    ),
    lists:foreach(
        fun(Key) ->
            ?assertEqual({Key, maps:get(Key, Fresh)}, {Key, maps:get(Key, Result, missing)})
        end,
        [peerhost, peername, sockport, listener, protocol, zone]
    ).

takeover_in_channel_of_another_client_test() ->
    Other = (fresh_clientinfo())#{
        clientid => <<"client2">>,
        username => <<"user2">>,
        password => <<"secret2">>,
        acl => #{rules => #{pub => [<<"t/user2/#">>]}},
        client_attrs => #{<<"group">> => <<"g2">>}
    },
    Resume = maps:without([acl, client_attrs], resume_clientinfo()),
    Result = merge(Other, Resume),
    ?assertEqual(<<"client1">>, maps:get(clientid, Result)),
    ?assertEqual(<<"user1">>, maps:get(username, Result)),
    ?assertNot(maps:is_key(password, Result)),
    ?assertNot(maps:is_key(acl, Result)),
    ?assertNot(maps:is_key(client_attrs, Result)).

peercert_infos_follow_the_receiving_connection_test() ->
    Resume = (resume_clientinfo())#{dn => <<"CN=client1">>, cn => <<"client1">>},
    Result1 = merge(fresh_clientinfo(), Resume),
    ?assertNot(maps:is_key(dn, Result1)),
    ?assertNot(maps:is_key(cn, Result1)),
    Fresh = (fresh_clientinfo())#{dn => <<"CN=other">>, cn => <<"other">>},
    Result2 = merge(Fresh, Resume),
    ?assertEqual(<<"CN=other">>, maps:get(dn, Result2)),
    ?assertEqual(<<"other">>, maps:get(cn, Result2)).

unknown_authn_result_key_is_kept_test() ->
    Resume = (resume_clientinfo())#{some_authn_output => <<"v1">>},
    Fresh = (fresh_clientinfo())#{other_key => <<"v2">>},
    Result = merge(Fresh, Resume),
    ?assertEqual(<<"v1">>, maps:get(some_authn_output, Result)),
    ?assertNot(maps:is_key(other_key, Result)).
