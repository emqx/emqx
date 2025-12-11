%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_banned_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Legacy format macro (pre OTP-28)
-define(LEGACY_RE_PATTERN(COMPILED, PATTERN), {COMPILED, PATTERN}).

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
    Records = [
        #{
            reason => <<"reason 1">>,
            at => to_local("2021-10-25T21:53:47+08:00"),
            until => to_local("2125-10-25T21:53:47+08:00"),
            as => clientid,
            who => <<"c1">>,
            by => <<"boot">>
        },
        #{
            reason => <<"reason 2">>,
            at => to_local("2021-10-25T21:53:47+08:00"),
            until => to_local("2125-10-25T21:53:47+08:00"),
            as => username,
            who => <<"u1">>,
            by => <<"boot">>
        }
    ],
    ?assertEqual(Records, get_banned_list()),

    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"full2.csv">>))),
    ?assertEqual(Records, get_banned_list()),
    ok.

t_optional_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"optional.csv">>))),
    ?assertMatch(
        [
            #{as := clientid, who := <<"c1">>},
            #{as := username, who := <<"u1">>}
        ],
        get_banned_list()
    ),
    ok.

t_omitted_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"omitted.csv">>))),
    ?assertMatch(
        [
            #{as := clientid, who := <<"c1">>},
            #{as := username, who := <<"u1">>}
        ],
        get_banned_list()
    ),
    ok.

t_error_bootstrap_file(_) ->
    emqx_banned:clear(),
    ?assertEqual(
        {error, enoent}, emqx_banned:init_from_csv(mk_bootstrap_file(<<"not_exists.csv">>))
    ),
    ?assertEqual(
        ok, emqx_banned:init_from_csv(mk_bootstrap_file(<<"error.csv">>))
    ),
    ?assertMatch(
        [#{as := clientid, who := <<"c1">>}], get_banned_list()
    ),
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

to_local(DateTimeBin) ->
    {ok, EpochSecond} = emqx_utils_calendar:to_epoch_second(DateTimeBin),
    emqx_utils_calendar:epoch_to_rfc3339(EpochSecond, second).

get_banned_list() ->
    Tabs = emqx_banned:tables(),
    Records0 = lists:foldl(
        fun(Tab, Acc) ->
            Acc ++ ets:tab2list(Tab)
        end,
        [],
        Tabs
    ),
    Records = [emqx_banned:format(Record) || Record <- Records0],
    lists:sort(Records).

%% Extract pattern from who field, handling both new and legacy formats
extract_pattern({clientid_re, Pattern}) when is_binary(Pattern) ->
    Pattern;
extract_pattern({clientid_re, {_Compiled, Pattern}}) when is_binary(Pattern) ->
    Pattern;
extract_pattern({username_re, Pattern}) when is_binary(Pattern) ->
    Pattern;
extract_pattern({username_re, {_Compiled, Pattern}}) when is_binary(Pattern) ->
    Pattern;
extract_pattern(_) ->
    error(badarg).

%%--------------------------------------------------------------------
%% Legacy format upgrade tests (pre OTP-28)
%%--------------------------------------------------------------------

t_legacy_format_upgrade(_) ->
    %% Insert legacy format records directly into ETS to simulate pre-OTP-28 data
    LegacyClientIdPattern = <<"LegacyClient.*">>,
    LegacyUsernamePattern = <<"LegacyUser.*">>,
    {ok, CompiledClientIdRE} = re:compile(LegacyClientIdPattern),
    {ok, CompiledUsernameRE} = re:compile(LegacyUsernamePattern),

    LegacyClientIdWho =
        {clientid_re, ?LEGACY_RE_PATTERN(CompiledClientIdRE, LegacyClientIdPattern)},
    LegacyUsernameWho =
        {username_re, ?LEGACY_RE_PATTERN(CompiledUsernameRE, LegacyUsernamePattern)},

    Now = erlang:system_time(second),
    LegacyClientIdBanned = #banned{
        who = LegacyClientIdWho,
        by = <<"test">>,
        reason = <<"legacy format test">>,
        at = Now,
        until = Now + 3600
    },
    LegacyUsernameBanned = #banned{
        who = LegacyUsernameWho,
        by = <<"test">>,
        reason = <<"legacy format test">>,
        at = Now,
        until = Now + 3600
    },

    %% Insert legacy format records directly into ETS
    ets:insert(emqx_banned_rules, LegacyClientIdBanned),
    ets:insert(emqx_banned_rules, LegacyUsernameBanned),

    %% Test that look_up works with legacy format
    %% Note: look_up returns records as stored in ETS (may still be legacy format)
    LegacyClientIdLookup = emqx_banned:look_up({clientid_re, LegacyClientIdPattern}),
    LegacyUsernameLookup = emqx_banned:look_up({username_re, LegacyUsernamePattern}),

    ?assertEqual(1, length(LegacyClientIdLookup)),
    ?assertEqual(1, length(LegacyUsernameLookup)),
    %% Verify look_up found the records (format may be legacy or upgraded)
    [#banned{who = ClientIdWho}] = LegacyClientIdLookup,
    [#banned{who = UsernameWho}] = LegacyUsernameLookup,
    ?assertMatch({clientid_re, _}, ClientIdWho),
    ?assertMatch({username_re, _}, UsernameWho),

    %% Test that all_rules upgrades legacy format
    AllRules = emqx_banned:all_rules(),
    LegacyClientIdRule = lists:filter(
        fun
            (#banned{who = {clientid_re, Pattern}}) when is_binary(Pattern) ->
                Pattern =:= LegacyClientIdPattern;
            (_) ->
                false
        end,
        AllRules
    ),
    LegacyUsernameRule = lists:filter(
        fun
            (#banned{who = {username_re, Pattern}}) when is_binary(Pattern) ->
                Pattern =:= LegacyUsernamePattern;
            (_) ->
                false
        end,
        AllRules
    ),

    ?assertEqual(1, length(LegacyClientIdRule)),
    ?assertEqual(1, length(LegacyUsernameRule)),
    %% Verify the rule is upgraded (not in legacy format)
    [#banned{who = {clientid_re, UpgradedClientIdPattern}}] = LegacyClientIdRule,
    [#banned{who = {username_re, UpgradedUsernamePattern}}] = LegacyUsernameRule,
    ?assertEqual(LegacyClientIdPattern, UpgradedClientIdPattern),
    ?assertEqual(LegacyUsernamePattern, UpgradedUsernamePattern),
    ?assertNot(is_tuple(UpgradedClientIdPattern)),
    ?assertNot(is_tuple(UpgradedUsernamePattern)),

    %% Test that check works with upgraded rules
    ClientInfoLegacyClientId = #{
        clientid => <<"LegacyClient123">>,
        username => <<"user">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoLegacyUsername = #{
        clientid => <<"client">>,
        username => <<"LegacyUser456">>,
        peerhost => {127, 0, 0, 1}
    },
    ClientInfoNotMatch = #{
        clientid => <<"OtherClient">>,
        username => <<"OtherUser">>,
        peerhost => {127, 0, 0, 1}
    },

    ?assert(emqx_banned:check(ClientInfoLegacyClientId)),
    ?assert(emqx_banned:check(ClientInfoLegacyUsername)),
    ?assertNot(emqx_banned:check(ClientInfoNotMatch)),

    %% Clean up: delete legacy format records using the actual who field from the records
    [#banned{who = ActualClientIdWho}] = LegacyClientIdLookup,
    [#banned{who = ActualUsernameWho}] = LegacyUsernameLookup,
    ok = emqx_banned:delete(ActualClientIdWho),
    ok = emqx_banned:delete(ActualUsernameWho),

    ?assertEqual(0, length(emqx_banned:look_up({clientid_re, LegacyClientIdPattern}))),
    ?assertEqual(0, length(emqx_banned:look_up({username_re, LegacyUsernamePattern}))),
    ?assertNot(emqx_banned:check(ClientInfoLegacyClientId)),
    ?assertNot(emqx_banned:check(ClientInfoLegacyUsername)),
    ok.

t_legacy_format_look_up(_) ->
    %% Test look_up with legacy format records
    Pattern1 = <<"TestClient.*">>,
    Pattern2 = <<"TestUser.*">>,
    {ok, CompiledRE1} = re:compile(Pattern1),
    {ok, CompiledRE2} = re:compile(Pattern2),

    Now = erlang:system_time(second),
    LegacyBanned1 = #banned{
        who = {clientid_re, ?LEGACY_RE_PATTERN(CompiledRE1, Pattern1)},
        by = <<"test">>,
        reason = <<"test">>,
        at = Now,
        until = Now + 3600
    },
    LegacyBanned2 = #banned{
        who = {username_re, ?LEGACY_RE_PATTERN(CompiledRE2, Pattern2)},
        by = <<"test">>,
        reason = <<"test">>,
        at = Now,
        until = Now + 3600
    },

    %% Insert legacy format records
    ets:insert(emqx_banned_rules, LegacyBanned1),
    ets:insert(emqx_banned_rules, LegacyBanned2),

    %% Test look_up finds legacy format records
    %% Note: look_up returns records as stored in ETS (may still be legacy format)
    Result1 = emqx_banned:look_up({clientid_re, Pattern1}),
    Result2 = emqx_banned:look_up({username_re, Pattern2}),

    ?assertEqual(1, length(Result1)),
    ?assertEqual(1, length(Result2)),
    %% Verify look_up found the records (format may be legacy or upgraded)
    [#banned{who = Who1}] = Result1,
    [#banned{who = Who2}] = Result2,
    ?assertMatch({clientid_re, _}, Who1),
    ?assertMatch({username_re, _}, Who2),
    %% Verify the pattern matches (extract pattern from either format)
    Pattern1Extracted = extract_pattern(Who1),
    Pattern2Extracted = extract_pattern(Who2),
    ?assertEqual(Pattern1, Pattern1Extracted),
    ?assertEqual(Pattern2, Pattern2Extracted),

    %% Clean up
    ok = emqx_banned:delete({clientid_re, Pattern1}),
    ok = emqx_banned:delete({username_re, Pattern2}),
    ok.
