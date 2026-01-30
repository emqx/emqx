%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_channel_tests).

-include_lib("eunit/include/eunit.hrl").

set_tns_in_log_meta_test_() ->
    NoTns = #{
        clientid => <<"id1">>,
        client_attrs => #{<<"not_tns">> => <<"tns1">>},
        username => <<"user1">>
    },
    NoTnsFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"id1">>,
                username := <<"user1">>
            },
            M
        ),
        ?assertNot(maps:is_key(tns, M))
    end,

    Prefixed = #{
        clientid => <<"tns1-id1">>,
        client_attrs => #{<<"tns">> => <<"tns1">>},
        username => <<"user2">>
    },
    PrefixedFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"tns1-id1">>,
                username := <<"user2">>
            },
            M
        ),
        ?assertNot(maps:is_key(tns, M))
    end,

    Username = #{
        clientid => <<"id1">>,
        client_attrs => #{<<"tns">> => <<"user3">>},
        username => <<"user3">>
    },
    UsernameFn =
        fun(M) ->
            ?assertMatch(
                #{
                    clientid := <<"id1">>,
                    username := <<"user3">>
                },
                M
            ),
            ?assertNot(maps:is_key(tns, M))
        end,

    TnsAdded = #{
        clientid => <<"id4">>,
        client_attrs => #{<<"tns">> => <<"tns1">>},
        username => <<"user4">>
    },
    TnsAddedFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"id4">>,
                username := <<"user4">>,
                tns := <<"tns1">>
            },
            M
        )
    end,

    Test = fun(CInfo, TestFn) ->
        Ch0 = emqx_channel:dummy(),
        Ch1 = emqx_channel:set_field(clientinfo, CInfo, Ch0),
        ok = emqx_channel:set_log_meta(dummy, Ch1),
        TestFn(logger:get_process_metadata())
    end,
    [
        {spawn, {Title, ?_test(Test(CInfo, TestFn))}}
     || {Title, CInfo, TestFn} <- [
            {"tns-added", TnsAdded, TnsAddedFn},
            {"username as tns", Username, UsernameFn},
            {"tns prefixed clientid", Prefixed, PrefixedFn},
            {"no tns", NoTns, NoTnsFn}
        ]
    ].
