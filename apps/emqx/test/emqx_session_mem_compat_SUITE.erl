%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_mem_compat_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_session_mem.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% Positions in the `#session{}` and `#mqueue{}` tuples of each layout.
-define(V63_INFLIGHT, 7).
-define(V63_MQUEUE, 8).
-define(V63_MQUEUE_Q, 6).
-define(PRE63_INFLIGHT, 8).
-define(PRE63_MQUEUE, 9).
-define(PRE63_MQUEUE_Q, 8).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{override_env => [{boot_modules, [broker]}]}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
Check that the `v63` record layout equals the `#session{}` record of this
version, and that the `#mqueue{}` of this version has the arity of the
6.3.0 record. A change to either record fails here until
`emqx_session_mem_compat` gets a layout for it.
""".
t_shape_guard(_Config) ->
    ?assertEqual(record_info(fields, session), emqx_session_mem_compat:fields(v63)),
    ?assertEqual(
        [clientid | emqx_session_mem_compat:fields(v63) -- [quota]],
        emqx_session_mem_compat:fields(pre63)
    ),
    MQ = emqx_session_mem:new_mqueue(clientinfo()),
    ?assertEqual(mqueue, element(1, MQ)),
    ?assertEqual(8, tuple_size(MQ)).

-doc "Check `detect/1` on every layout and on a term of no layout.".
t_detect(Config) ->
    [V63, V63Empty] = fixtures(Config, "v63.eterm"),
    [Pre63] = fixtures(Config, "pre63.eterm"),
    ?assertEqual({legacy, v63}, emqx_session_mem_compat:detect(V63)),
    ?assertEqual({legacy, v63}, emqx_session_mem_compat:detect(V63Empty)),
    ?assertEqual({legacy, pre63}, emqx_session_mem_compat:detect(Pre63)),
    Exported = emqx_session_mem_compat:to_exported(V63),
    ?assertNot(maps:is_key(vsn, Exported)),
    ?assertEqual({exported, 1}, emqx_session_mem_compat:detect(Exported)),
    ?assertEqual({exported, 2}, emqx_session_mem_compat:detect(Exported#{vsn => 2})),
    Live = emqx_session_mem:export(session()),
    ?assertEqual({exported, 1}, emqx_session_mem_compat:detect(Live)),
    ?assertEqual(unknown, emqx_session_mem_compat:detect(#{})),
    ?assertEqual(unknown, emqx_session_mem_compat:detect({session, a, b})),
    ?assertError({unknown_session_layout, _}, emqx_session_mem_compat:to_exported({session})).

-doc """
Check the `v63` fixtures: the conversion to the exported form gives the
queued messages in order without the insert timestamps, and
`to_exported`, `import/2`, `export/1` and `from_exported` give back the
fixture, with the mqueue in the shape of 6.3.0 and 6.3.1.
""".
t_v63_round_trip(Config) ->
    [V63, V63Empty] = fixtures(Config, "v63.eterm"),
    #{mqueue := Queued, inflight := [Inflight]} = emqx_session_mem_compat:to_exported(V63),
    ?assertMatch(
        [
            #message{qos = ?QOS_0, topic = <<"t/0">>, extra = #{}},
            #message{qos = ?QOS_1, topic = <<"t/2">>, extra = #{}}
        ],
        Queued
    ),
    ?assert(lists:all(fun(#message{extra = E}) -> map_size(E) =:= 0 end, Queued)),
    ?assertMatch(#{packet_id := 1, phase := wait_ack, message := #message{}}, Inflight),
    lists:foreach(fun(Fixture) -> check_round_trip(v63, Fixture) end, [V63, V63Empty]).

-doc """
Check that a `v63` session with a mqueue that stores QoS 0 messages, as a
6.3.1 node sends it, resumes on this node and takes more messages.
""".
t_v63_store_qos0_resume(Config) ->
    [V63, _] = fixtures(Config, "v63.eterm"),
    Session0 = emqx_session_mem:import(
        clientinfo(), emqx_session_mem_compat:to_exported(V63)
    ),
    ?assertEqual(2, emqx_session_mem:info(mqueue_len, Session0)),
    Msg = emqx_message:make(<<"c1">>, ?QOS_0, <<"t/3">>, <<"more">>),
    Session = emqx_session_mem:enqueue(clientinfo(), [Msg], Session0),
    ?assertEqual(3, emqx_session_mem:info(mqueue_len, Session)).

-doc """
Check the `pre63` fixture: `to_exported`, `import/2`, `export/1` and
`from_exported` give back the fixture.
""".
t_pre63_round_trip(Config) ->
    [Pre63] = fixtures(Config, "pre63.eterm"),
    #{mqueue := Queued} = emqx_session_mem_compat:to_exported(Pre63),
    ?assertMatch([#message{topic = <<"t/0">>}, #message{topic = <<"t/2">>}], Queued),
    check_round_trip(pre63, Pre63).

-doc """
Check that a live session of this version, which holds the `{empty, MaxLen}`
mqueue and the `{lazy, ListenerId}` limiter placeholders, converts to the
`v63` layout with a built mqueue and no limiter, and back.
""".
t_v63_from_live_session(_Config) ->
    Session = session(),
    ?assertMatch(#session{mqueue = {empty, _}, quota = {lazy, _}}, Session),
    Exported = emqx_session_mem:export(Session),
    ?assertEqual(1, maps:get(vsn, Exported)),
    V63 = emqx_session_mem_compat:from_exported(v63, clientinfo(), Exported),
    ?assertEqual({legacy, v63}, emqx_session_mem_compat:detect(V63)),
    MQ = element(?V63_MQUEUE, V63),
    ?assertMatch({mqueue, true, 1000, 0, 0, _, _, _}, MQ),
    ?assertEqual(false, element(#session.quota, V63)),
    ?assertEqual(
        maps:remove(vsn, Exported),
        emqx_session_mem_compat:to_exported(V63)
    ),
    %% The live record converts as it is, with the placeholders.
    ?assertEqual(maps:remove(vsn, Exported), emqx_session_mem_compat:to_exported(Session)).

-doc "Check `layout_for_peer/1` for each combination of BPAPI versions.".
t_layout_for_peer(_Config) ->
    ok = meck:new(emqx_bpapi, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_bpapi, supported_version, fun
        ('n632@h', emqx_gateway_cm) -> 3;
        ('n631@h', emqx_gateway_cm) -> 2;
        ('n631@h', emqx_cm) -> 4;
        ('n623@h', emqx_gateway_cm) -> 2;
        ('n623@h', emqx_cm) -> 3;
        ('n58@h', emqx_gateway_cm) -> 1;
        ('n58@h', emqx_cm) -> 3;
        (_, _) -> undefined
    end),
    ?assertEqual(exported, emqx_session_mem_compat:layout_for_peer(node())),
    ?assertEqual(exported, emqx_session_mem_compat:layout_for_peer('n632@h')),
    ?assertEqual(v63, emqx_session_mem_compat:layout_for_peer('n631@h')),
    ?assertEqual(pre63, emqx_session_mem_compat:layout_for_peer('n623@h')),
    ?assertEqual(pre63, emqx_session_mem_compat:layout_for_peer('n58@h')),
    ?assertEqual(pre63, emqx_session_mem_compat:layout_for_peer('unknown@h')).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

check_round_trip(Layout, Fixture) ->
    Exported = emqx_session_mem_compat:to_exported(Fixture),
    Session = emqx_session_mem:import(clientinfo(), Exported),
    Back = emqx_session_mem_compat:from_exported(
        Layout, clientinfo(), emqx_session_mem:export(Session), fixture_opts()
    ),
    ?assertEqual({legacy, Layout}, emqx_session_mem_compat:detect(Back)),
    ?assertEqual(Exported, emqx_session_mem_compat:to_exported(Back)),
    ?assertEqual(normalize(Layout, Fixture), normalize(Layout, Back)).

%% Replace the parts the conversion rebuilds: the inflight tree by its
%% entries, and the queue inside the mqueue (compared through
%% `to_exported/1`).
normalize(v63, Session) ->
    normalize(Session, ?V63_INFLIGHT, ?V63_MQUEUE, ?V63_MQUEUE_Q);
normalize(pre63, Session) ->
    normalize(Session, ?PRE63_INFLIGHT, ?PRE63_MQUEUE, ?PRE63_MQUEUE_Q).

normalize(Session, InflightPos, MQueuePos, QPos) ->
    {inflight, MaxSize, Tree} = element(InflightPos, Session),
    Session1 = setelement(InflightPos, Session, {inflight, MaxSize, gb_trees:to_list(Tree)}),
    MQ = element(MQueuePos, Session1),
    setelement(MQueuePos, Session1, setelement(QPos, MQ, q)).

%% The session configuration and inflight window of the fixtures.
fixture_opts() ->
    #{
        receive_maximum => 1,
        conf => #{
            max_subscriptions => infinity,
            upgrade_qos => false,
            retry_interval => 30000,
            max_awaiting_rel => 100,
            await_rel_timeout => 300000
        }
    }.

fixtures(Config, File) ->
    {ok, Terms} = file:consult(filename:join(?config(data_dir, Config), File)),
    [Term || {_Name, Term} <- Terms].

clientinfo() ->
    #{zone => default, listener => 'tcp:default', clientid => <<"c1">>}.

session() ->
    emqx_session_mem:create(
        clientinfo(),
        #{receive_maximum => 1, expiry_interval => 0},
        undefined,
        emqx_session:get_session_conf(clientinfo())
    ).
