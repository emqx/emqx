%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_mqtt_request_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_ID, <<"test-request">>).
-define(TOPIC_PREFIX, <<"devices/room1/">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_mqtt_request:create(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_mqtt_request:destroy(?SKILL_ID).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% create/1 registers the skill under the expected type.
t_registers_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"message.request">>, ?SKILL_ID),
    ?assertEqual(<<"message.request">>, maps:get(type, Skill)),
    ?assertEqual(?SKILL_ID, maps:get(skill_id, Skill)).

%% destroy/1 removes the skill from the registry.
t_destroy_unregisters(_Config) ->
    ok = emqx_agent_skill_mqtt_request:destroy(?SKILL_ID),
    ?assertEqual(
        {error, not_found},
        emqx_agent_skill_registry:lookup(<<"message.request">>, ?SKILL_ID)
    ),
    ok = emqx_agent_skill_mqtt_request:create(test_context()).

%% Happy path: request arrives on the device topic with a Response-Topic
%% MQTT 5 property; responder publishes back; skill reply arrives.
t_request_response(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "sensor/1">>,
    ReqId = <<"req-rr-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?SKILL_ID, #{<<"topic">> => <<"sensor/1">>, <<"payload">> => <<"ping">>}, ReqId),

    %% Act as the device: receive the request and echo back via the response topic.
    #deliver{message = ReqMsg} = await_deliver(DeviceTopic),
    ResponseTopic = response_topic(ReqMsg),
    RespMsg = emqx_message:make(<<"device-sim">>, 0, ResponseTopic, <<"pong">>),
    _ = emqx_broker:publish(RespMsg),

    %% Skill reply must carry the response payload and status=ok.
    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-rr-1">>,
            <<"frame">> := <<"unary">>,
            <<"data">> := #{<<"status">> := <<"ok">>, <<"payload">> := <<"pong">>}
        },
        Reply
    ),

    ok = emqx:unsubscribe(DeviceTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% The outbound request message must carry Response-Topic in MQTT 5 properties.
t_response_topic_in_properties(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "sensor/2">>,
    ReqId = <<"req-props-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?SKILL_ID, #{<<"topic">> => <<"sensor/2">>, <<"payload">> => <<"x">>}, ReqId),

    #deliver{message = ReqMsg} = await_deliver(DeviceTopic),
    ResponseTopic = response_topic(ReqMsg),

    %% Response-Topic must be present and rooted under the expected prefix.
    ?assertNotEqual(undefined, ResponseTopic),
    ?assert(binary:match(ResponseTopic, <<"cap/tmp/response/">>) =/= nomatch),

    %% Clean up: respond so the spawned process terminates.
    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, ResponseTopic, <<"ok">>)),
    _ = await_deliver(ReplyTopic),

    ok = emqx:unsubscribe(DeviceTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Each invocation uses a distinct response topic (no cross-talk between concurrent requests).
t_response_topics_are_unique(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "sensor/3">>,
    ReqId1 = <<"req-uniq-1">>,
    ReqId2 = <<"req-uniq-2">>,
    ReplyTopic1 = reply_topic(?SKILL_ID, ReqId1),
    ReplyTopic2 = reply_topic(?SKILL_ID, ReqId2),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic1),
    ok = emqx:subscribe(ReplyTopic2),

    invoke(?SKILL_ID, #{<<"topic">> => <<"sensor/3">>, <<"payload">> => <<"a">>}, ReqId1),
    invoke(?SKILL_ID, #{<<"topic">> => <<"sensor/3">>, <<"payload">> => <<"b">>}, ReqId2),

    #deliver{message = Msg1} = await_deliver(DeviceTopic),
    #deliver{message = Msg2} = await_deliver(DeviceTopic),
    RT1 = response_topic(Msg1),
    RT2 = response_topic(Msg2),

    ?assertNotEqual(RT1, RT2),

    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, RT1, <<"r1">>)),
    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, RT2, <<"r2">>)),
    _ = await_deliver(ReplyTopic1),
    _ = await_deliver(ReplyTopic2),

    ok = emqx:unsubscribe(DeviceTopic),
    ok = emqx:unsubscribe(ReplyTopic1),
    ok = emqx:unsubscribe(ReplyTopic2).

%% When no response arrives within timeout_ms the skill replies with status=error/timeout.
t_timeout(_Config) ->
    ReqId = <<"req-timeout-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),

    ok = emqx:subscribe(ReplyTopic),

    %% Nobody subscribes to the device topic, so no response will come.
    invoke(
        ?SKILL_ID,
        #{<<"topic">> => <<"nowhere">>, <<"payload">> => <<"x">>, <<"timeout_ms">> => 200},
        ReqId
    ),

    Reply = decode_reply(await_deliver(ReplyTopic, 3000)),
    ?assertMatch(
        #{<<"data">> := #{<<"status">> := <<"error">>, <<"reason">> := <<"timeout">>}},
        Reply
    ),

    ok = emqx:unsubscribe(ReplyTopic).

%% topic_prefix is always prepended; agent cannot target topics outside its namespace.
t_topic_prefix_applied(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "sub/topic">>,
    ReqId = <<"req-prefix-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),

    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?SKILL_ID, #{<<"topic">> => <<"sub/topic">>, <<"payload">> => <<"hi">>}, ReqId),

    %% Message must arrive on the prefixed topic, not bare "sub/topic".
    #deliver{topic = ArrivedTopic, message = ReqMsg} = await_deliver(FullTopic),
    ?assertEqual(FullTopic, ArrivedTopic),

    RT = response_topic(ReqMsg),
    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, RT, <<"bye">>)),
    _ = await_deliver(ReplyTopic),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Correlation fields (req_id, trace_id, iid, sid) are forwarded in the skill reply.
t_reply_correlation(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "corr/dev">>,
    ReqId = <<"req-corr-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?SKILL_ID,
        #{<<"topic">> => <<"corr/dev">>, <<"payload">> => <<"z">>},
        ReqId,
        #{<<"trace_id">> => <<"tr-42">>, <<"iid">> => <<"iid-7">>, <<"sid">> => <<"sid-3">>}
    ),

    #deliver{message = ReqMsg} = await_deliver(DeviceTopic),
    RT = response_topic(ReqMsg),
    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, RT, <<"ack">>)),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-corr-1">>,
            <<"trace_id">> := <<"tr-42">>,
            <<"iid">> := <<"iid-7">>,
            <<"sid">> := <<"sid-3">>
        },
        Reply
    ),

    ok = emqx:unsubscribe(DeviceTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Invocations targeting an unknown skill_id are silently dropped.
t_unknown_skill_id_ignored(_Config) ->
    ReqId = <<"req-unknown-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(<<"no-such-skill">>, #{<<"topic">> => <<"x">>, <<"payload">> => <<"y">>}, ReqId),

    receive
        #deliver{topic = ReplyTopic} -> ct:fail("unexpected reply for unknown skill_id")
    after 500 -> ok
    end,

    ok = emqx:unsubscribe(ReplyTopic).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    #{
        skill_id => ?SKILL_ID,
        desc => <<"Test request skill">>,
        topic_prefix => ?TOPIC_PREFIX
    }.

reply_topic(SkillId, ReqId) ->
    <<"cap/message.request/", SkillId/binary, "/response/", ReqId/binary>>.

invoke(SkillId, Args, ReqId) ->
    invoke(SkillId, Args, ReqId, #{}).

invoke(SkillId, Args, ReqId, Extra) ->
    Topic = <<"cap/message.request/", SkillId/binary, "/request">>,
    Payload = emqx_utils_json:encode(
        maps:merge(
            #{
                <<"req_id">> => ReqId,
                <<"trace_id">> => null,
                <<"iid">> => null,
                <<"sid">> => null,
                <<"args">> => Args
            },
            Extra
        )
    ),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)),
    ok.

%% Extract the MQTT 5 Response-Topic property from a request message.
response_topic(Msg) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    maps:get('Response-Topic', Props, undefined).

await_deliver(Topic) ->
    await_deliver(Topic, 3000).

await_deliver(Topic, Timeout) ->
    receive
        #deliver{topic = Topic} = D -> D
    after Timeout ->
        ct:fail("no message on ~s within ~w ms", [Topic, Timeout])
    end.

decode_reply(#deliver{message = #message{payload = P}}) ->
    emqx_utils_json:decode(P).
