%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_banned_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

t_add_delete(_) ->
    Banned = #banned{
        who = emqx_banned:who(clientid, <<"TestClient">>),
        by = <<"banned suite">>,
        reason = <<"test">>,
        at = erlang:system_time(second),
        until = erlang:system_time(second) + 1
    },
    {ok, _} = emqx_banned:create(Banned),
    {error, {already_exist, Banned}} = emqx_banned:create(Banned),
    ?assertEqual(1, emqx_banned:info(size)),
    {error, {already_exist, Banned}} =
        emqx_banned:create(Banned#banned{until = erlang:system_time(second) + 100}),
    ?assertEqual(1, emqx_banned:info(size)),

    ok = emqx_banned:delete(emqx_banned:who(clientid, <<"TestClient">>)),
    ?assertEqual(0, emqx_banned:info(size)).

t_check(_) ->
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(clientid, <<"BannedClient">>)}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(username, <<"BannedUser">>)}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(peerhost, {192, 168, 0, 1})}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(peerhost, <<"192.168.0.2">>)}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(clientid_re, <<"BannedClientRE.*">>)}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(username_re, <<"BannedUserRE.*">>)}),
    {ok, _} = emqx_banned:create(#banned{who = emqx_banned:who(peerhost_net, <<"192.168.3.0/24">>)}),

    ?assertEqual(7, emqx_banned:info(size)),
    ClientInfoBannedClientId = #{
        clientid => <<"BannedClient">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoBannedUsername = #{
        clientid => <<"client">>,
        username => <<"BannedUser">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoBannedAddr1 = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {192, 168, 0, 1}
    },
    ClientInfoBannedAddr2 = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {192, 168, 0, 2}
    },
    ClientInfoBannedClientIdRE = #{
        clientid => <<"BannedClientRE1">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoBannedUsernameRE = #{
        clientid => <<"client">>,
        username => <<"BannedUserRE1">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoBannedAddrNet = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {192, 168, 3, 1}
    },
    ClientInfoValidFull = #{
        clientid => <<"client">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoValidEmpty = #{},
    ClientInfoValidOnlyClientId = #{clientid => <<"client1">>},
    ?assert(emqx_banned:check(ClientInfoBannedClientId)),
    ?assert(emqx_banned:check(ClientInfoBannedUsername)),
    ?assert(emqx_banned:check(ClientInfoBannedAddr1)),
    ?assert(emqx_banned:check(ClientInfoBannedAddr2)),
    ?assert(emqx_banned:check(ClientInfoBannedClientIdRE)),
    ?assert(emqx_banned:check(ClientInfoBannedUsernameRE)),
    ?assert(emqx_banned:check(ClientInfoBannedAddrNet)),
    ?assertNot(emqx_banned:check(ClientInfoValidFull)),
    ?assertNot(emqx_banned:check(ClientInfoValidEmpty)),
    ?assertNot(emqx_banned:check(ClientInfoValidOnlyClientId)),

    ?assert(emqx_banned:check_clientid(<<"BannedClient">>)),
    ?assert(emqx_banned:check_clientid(<<"BannedClientRE">>)),

    ok = emqx_banned:delete(emqx_banned:who(clientid, <<"BannedClient">>)),
    ok = emqx_banned:delete(emqx_banned:who(username, <<"BannedUser">>)),
    ok = emqx_banned:delete(emqx_banned:who(peerhost, {192, 168, 0, 1})),
    ok = emqx_banned:delete(emqx_banned:who(peerhost, <<"192.168.0.2">>)),
    ok = emqx_banned:delete(emqx_banned:who(clientid_re, <<"BannedClientRE.*">>)),
    ok = emqx_banned:delete(emqx_banned:who(username_re, <<"BannedUserRE.*">>)),
    ok = emqx_banned:delete(emqx_banned:who(peerhost_net, <<"192.168.3.0/24">>)),
    ?assertNot(emqx_banned:check(ClientInfoBannedClientId)),
    ?assertNot(emqx_banned:check(ClientInfoBannedUsername)),
    ?assertNot(emqx_banned:check(ClientInfoBannedAddr1)),
    ?assertNot(emqx_banned:check(ClientInfoBannedAddr2)),
    ?assertNot(emqx_banned:check(ClientInfoBannedClientIdRE)),
    ?assertNot(emqx_banned:check(ClientInfoBannedUsernameRE)),
    ?assertNot(emqx_banned:check(ClientInfoBannedAddrNet)),
    ?assertNot(emqx_banned:check(ClientInfoValidFull)),

    ?assertNot(emqx_banned:check_clientid(<<"BannedClient">>)),
    ?assertNot(emqx_banned:check_clientid(<<"BannedClientRE">>)),

    ?assertEqual(0, emqx_banned:info(size)).

t_unused(_) ->
    Who1 = emqx_banned:who(clientid, <<"BannedClient1">>),
    Who2 = emqx_banned:who(clientid, <<"BannedClient2">>),

    ?assertMatch(
        {ok, _},
        emqx_banned:create(#banned{
            who = Who1,
            until = erlang:system_time(second)
        })
    ),
    ?assertMatch(
        {ok, _},
        emqx_banned:create(#banned{
            who = Who2,
            until = erlang:system_time(second) - 1
        })
    ),
    ?assertEqual(ignored, gen_server:call(emqx_banned, unexpected_req)),
    ?assertEqual(ok, gen_server:cast(emqx_banned, unexpected_msg)),
    %% expiry timer
    timer:sleep(500),

    ok = emqx_banned:delete(Who1),
    ok = emqx_banned:delete(Who2).

t_kick(_) ->
    ClientId = <<"client">>,
    snabbkaffe:start_trace(),

    Now = erlang:system_time(second),
    Who = emqx_banned:who(clientid, ClientId),

    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    Trace = snabbkaffe:collect_trace(),
    snabbkaffe:stop(),
    emqx_banned:delete(Who),
    ?assertEqual(1, length(?of_kind(kick_session_due_to_banned, Trace))).

t_session_taken(_) ->
    erlang:process_flag(trap_exit, true),
    Topic = <<"t/banned">>,
    ClientId2 = emqx_guid:to_hexstr(emqx_guid:gen()),
    MsgNum = 3,
    Connect = fun() ->
        ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
        {ok, C} = emqtt:start_link([
            {clientid, ClientId},
            {proto_ver, v5},
            {clean_start, false},
            {properties, #{'Session-Expiry-Interval' => 120}}
        ]),
        case emqtt:connect(C) of
            {ok, _} ->
                ok;
            {error, econnrefused} ->
                throw(mqtt_listener_not_ready)
        end,
        {ok, _, [0]} = emqtt:subscribe(C, Topic, []),
        C
    end,

    Publish = fun() ->
        lists:foreach(
            fun(_) ->
                Msg = emqx_message:make(ClientId2, Topic, <<"payload">>),
                emqx_broker:safe_publish(Msg)
            end,
            lists:seq(1, MsgNum)
        )
    end,
    emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() ->
            try
                C = Connect(),
                emqtt:disconnect(C),
                true
            catch
                throw:mqtt_listener_not_ready ->
                    false
            end
        end,
        15_000
    ),

    C2 = Connect(),
    Publish(),
    ?assertEqual(MsgNum, length(receive_messages(MsgNum + 1))),
    ok = emqtt:disconnect(C2),

    Publish(),

    Now = erlang:system_time(second),
    Who = emqx_banned:who(clientid, ClientId2),
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    C3 = Connect(),
    ?assertEqual(0, length(receive_messages(MsgNum + 1))),
    emqx_banned:delete(Who),
    {ok, #{}, [0]} = emqtt:unsubscribe(C3, Topic),
    ok = emqtt:disconnect(C3).

t_full_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"full.csv">>))),
    FullDatas = lists:sort([
        {banned, {username, <<"u1">>}, <<"boot">>, <<"reason 2">>, 1635170027, 1761400427},
        {banned, {clientid, <<"c1">>}, <<"boot">>, <<"reason 1">>, 1635170027, 1761400427}
    ]),
    ?assertMatch(FullDatas, lists:sort(get_banned_list())),

    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"full2.csv">>))),
    ?assertMatch(FullDatas, lists:sort(get_banned_list())),
    ok.

t_optional_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"optional.csv">>))),
    Keys = lists:sort([{username, <<"u1">>}, {clientid, <<"c1">>}]),
    ?assertMatch(Keys, lists:sort([element(2, Data) || Data <- get_banned_list()])),
    ok.

t_omitted_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"omitted.csv">>))),
    Keys = lists:sort([{username, <<"u1">>}, {clientid, <<"c1">>}]),
    ?assertMatch(Keys, lists:sort([element(2, Data) || Data <- get_banned_list()])),
    ok.

t_error_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(
        {error, enoent}, emqx_banned:init_from_csv(mk_bootstrap_file(<<"not_exists.csv">>))
    ),
    ?assertEqual(
        ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"error.csv">>))
    ),
    Keys = [{clientid, <<"c1">>}],
    ?assertMatch(Keys, [element(2, Data) || Data <- get_banned_list()]),
    ok.

t_until_expiry(_) ->
    Who = #{<<"as">> => clientid, <<"who">> => <<"t_until_expiry">>},

    {ok, Banned} = emqx_banned:parse(Who),
    {ok, _} = emqx_banned:create(Banned),

    [Data] = emqx_banned:look_up(Who),
    ?assertEqual(Banned, Data),
    ?assertMatch(#{until := infinity}, emqx_banned:format(Data)),

    emqx_banned:clear(),
    ok.

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count - 1, [Msg | Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n", [Other]),
            receive_messages(Count, Msgs)
    after 1200 ->
        Msgs
    end.

mk_bootstrap_file(File) ->
    Dir = code:lib_dir(emqx),
    filename:join([Dir, "test", "data", "banned", File]).

get_banned_list() ->
    Tabs = emqx_banned:tables(),
    lists:foldl(
        fun(Tab, Acc) ->
            Acc ++ ets:tab2list(Tab)
        end,
        [],
        Tabs
    ).
