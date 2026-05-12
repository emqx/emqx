%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_ID, <<"test-publish">>).
-define(TOPIC_PREFIX, <<"devices/room1/">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_resource, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = register_skill(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:clear_runtime_for_test().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% create/1 builds a runtime skill under the expected type.
t_create_returns_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"message__publish">>, ?SKILL_ID),
    ?assertMatch(#{type := <<"message__publish">>}, Skill),
    ?assertEqual(?SKILL_ID, maps:get(skill_id, Skill)).

%% destroy/1 accepts the full runtime skill.
t_destroy_accepts_runtime_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"message__publish">>, ?SKILL_ID),
    ?assertEqual(ok, emqx_agent_skill_publish:destroy(Skill)).

t_custom_payload_schema_is_stored(_Config) ->
    ok = emqx_agent_skill_registry:clear_runtime_for_test(),
    CustomPayloadSchema = #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"command">> => #{
                <<"type">> => <<"string">>, <<"enum">> => [<<"move">>, <<"rotate">>, <<"park">>]
            },
            <<"direction">> => #{
                <<"type">> => <<"string">>, <<"enum">> => [<<"left">>, <<"right">>, <<"down">>]
            }
        },
        <<"required">> => [<<"command">>]
    },
    ok = register_skill(maps:put(payload_schema, CustomPayloadSchema, test_context())),
    {ok, Skill} = emqx_agent_skill_registry:lookup(<<"message__publish">>, ?SKILL_ID),
    #{context := Context, input_schema := InputSchema} = Skill,
    ?assertMatch(#{payload_schema := CustomPayloadSchema}, Context),
    Props = maps:get(<<"properties">>, InputSchema),
    ?assertEqual(CustomPayloadSchema, maps:get(<<"payload">>, Props)).

%% A basic invoke publishes to prefix+topic and replies with status=ok.
t_publish_basic(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "temperature">>,
    ReqId = <<"req-pub-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?SKILL_ID, #{<<"topic">> => <<"temperature">>, <<"payload">> => <<"23.5">>}, ReqId),

    %% The published message must arrive on the full topic.
    ?assertMatch(
        #deliver{topic = FullTopic, message = #message{payload = <<"23.5">>}},
        await_deliver(FullTopic)
    ),

    %% The reply must confirm success and echo the full topic.
    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"topic">> := FullTopic}
        },
        Response
    ),
    ?assertMatch(#{<<"req_id">> := <<"req-pub-1">>}, Reply),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% The topic_prefix is always prepended; the agent cannot escape the namespace.
t_topic_prefix_is_applied(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "sub/path">>,
    ReqId = <<"req-prefix-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?SKILL_ID, #{<<"topic">> => <<"sub/path">>, <<"payload">> => <<"hello">>}, ReqId),

    ?assertMatch(
        #deliver{topic = FullTopic, message = #message{payload = <<"hello">>}},
        await_deliver(FullTopic)
    ),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertEqual(
        FullTopic,
        nested_get([<<"result">>, <<"topic">>], emqx_agent_skill_helpers:cap_response(Reply))
    ),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Optional `from` and `qos` args are accepted and forwarded correctly.
t_publish_with_from_and_qos(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "cmd">>,
    ReqId = <<"req-from-qos-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?SKILL_ID,
        #{
            <<"topic">> => <<"cmd">>,
            <<"payload">> => <<"on">>,
            <<"from">> => <<"agent-007">>,
            <<"qos">> => 1
        },
        ReqId
    ),

    ?assertMatch(
        #deliver{topic = FullTopic, message = #message{from = <<"agent-007">>, qos = 1}},
        await_deliver(FullTopic)
    ),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertMatch(#{<<"status">> := <<"ok">>}, emqx_agent_skill_helpers:cap_response(Reply)),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Correlation fields (req_id, trace_id, iid, sid) are echoed in the reply.
t_reply_correlation(_Config) ->
    ReqId = <<"req-corr-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?SKILL_ID,
        #{<<"topic">> => <<"x">>, <<"payload">> => <<"y">>},
        ReqId,
        #{<<"trace_id">> => <<"tr-99">>, <<"iid">> => <<"iid-42">>, <<"sid">> => <<"sid-7">>}
    ),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertMatch(
        #{
            <<"req_id">> := <<"req-corr-1">>,
            <<"trace_id">> := <<"tr-99">>,
            <<"iid">> := <<"iid-42">>,
            <<"sid">> := <<"sid-7">>
        },
        Reply
    ),

    ok = emqx:unsubscribe(ReplyTopic).

%% An invoke targeting an unregistered skill_id is silently dropped — no reply arrives.
t_unknown_skill_id_ignored(_Config) ->
    ReqId = <<"req-unknown-1">>,
    ReplyTopic = reply_topic(?SKILL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        <<"no-such-skill">>,
        #{<<"topic">> => <<"x">>, <<"payload">> => <<"y">>},
        ReqId
    ),

    receive
        #deliver{topic = ReplyTopic} -> ct:fail("unexpected reply for unknown skill_id")
    after 500 ->
        ok
    end,

    ok = emqx:unsubscribe(ReplyTopic).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    #{
        skill_id => ?SKILL_ID,
        desc => <<"Test publish skill">>,
        topic_prefix => ?TOPIC_PREFIX
    }.

register_skill(Context) ->
    {ok, Skill} = emqx_agent_skill_publish:create(Context),
    emqx_agent_skill_registry:put_runtime_for_test(Skill).

invoke(SkillId, Args, ReqId) ->
    invoke(SkillId, Args, ReqId, #{}).

invoke(SkillId, Args, ReqId, Extra) ->
    Topic = <<"cap/message__publish/", SkillId/binary, "/request/", ReqId/binary>>,
    Payload = emqx_utils_json:encode(
        maps:merge(
            #{
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

reply_topic(SkillId, ReqId) ->
    <<"cap/message__publish/", SkillId/binary, "/response/", ReqId/binary>>.

await_deliver(Topic) ->
    receive
        #deliver{topic = Topic} = D -> D
    after 3000 ->
        ct:fail("no message on ~s within 3 s", [Topic])
    end.

decode_reply(#deliver{message = #message{payload = P}}) ->
    emqx_utils_json:decode(P).

nested_get([Key], Map) ->
    maps:get(Key, Map);
nested_get([Key | Rest], Map) ->
    nested_get(Rest, maps:get(Key, Map)).
