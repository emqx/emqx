%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_mqtt_request_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TOOL_ID, <<"test-request">>).
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
    ok = emqx_agent_plugin_config_fixture:setup(),
    ok = register_tool(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% create/1 builds a runtime tool under the expected type.
t_create_returns_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"message__request">>, ?TOOL_ID),
    ?assertMatch(#{type := <<"message__request">>}, Tool),
    ?assertEqual(?TOOL_ID, maps:get(tool_id, Tool)).

t_create_rejects_malformed_request_payload_schema(_Config) ->
    RuntimeContext = maps:merge(test_context(), #{
        <<"tool_id">> => ?TOOL_ID,
        <<"request_payload_schema">> => <<"{">>
    }),
    ?assertMatch(
        {error, {invalid_request_payload_schema, {invalid_json, _}}},
        emqx_agent_tool_mqtt_request:create(RuntimeContext)
    ).

%% destroy/1 accepts the full runtime tool.
t_destroy_accepts_runtime_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"message__request">>, ?TOOL_ID),
    ?assertEqual(ok, emqx_agent_tool_mqtt_request:destroy(Tool)).

%% Happy path: request arrives on the device topic with a Response-Topic
%% MQTT 5 property; responder publishes back; tool reply arrives.
t_request_response(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "sensor/1">>,
    ReqId = <<"req-rr-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?TOOL_ID, #{<<"topic">> => <<"sensor/1">>, <<"payload">> => <<"ping">>}, ReqId),

    %% Act as the device: receive the request and echo back via the response topic.
    #deliver{message = ReqMsg} = await_deliver(DeviceTopic),
    ResponseTopic = response_topic(ReqMsg),
    RespMsg = emqx_message:make(<<"device-sim">>, 0, ResponseTopic, <<"pong">>),
    _ = emqx_broker:publish(RespMsg),

    %% Tool reply must carry the response payload and status=ok.
    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_tool_helpers:cap_response(Reply),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := #{<<"payload">> := <<"pong">>}
        },
        Response
    ),
    ?assertMatch(#{<<"req_id">> := <<"req-rr-1">>}, Reply),

    ok = emqx:unsubscribe(DeviceTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% The outbound request message must carry Response-Topic in MQTT 5 properties.
t_response_topic_in_properties(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "sensor/2">>,
    ReqId = <<"req-props-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?TOOL_ID, #{<<"topic">> => <<"sensor/2">>, <<"payload">> => <<"x">>}, ReqId),

    #deliver{message = ReqMsg} = await_deliver(DeviceTopic),
    ResponseTopic = response_topic(ReqMsg),

    %% Response-Topic must be present and rooted under the expected prefix.
    ?assertNotEqual(undefined, ResponseTopic),
    ?assert(binary:match(ResponseTopic, <<"$cap/tmp/response/">>) =/= nomatch),

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
    ReplyTopic1 = reply_topic(?TOOL_ID, ReqId1),
    ReplyTopic2 = reply_topic(?TOOL_ID, ReqId2),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic1),
    ok = emqx:subscribe(ReplyTopic2),

    invoke(?TOOL_ID, #{<<"topic">> => <<"sensor/3">>, <<"payload">> => <<"a">>}, ReqId1),
    invoke(?TOOL_ID, #{<<"topic">> => <<"sensor/3">>, <<"payload">> => <<"b">>}, ReqId2),

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

%% When no response arrives within timeout_ms the tool replies with status=error/timeout.
t_timeout(_Config) ->
    ReqId = <<"req-timeout-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),

    ok = emqx:subscribe(ReplyTopic),

    %% Nobody subscribes to the device topic, so no response will come.
    invoke(
        ?TOOL_ID,
        #{<<"topic">> => <<"nowhere">>, <<"payload">> => <<"x">>, <<"timeout_ms">> => 200},
        ReqId
    ),

    Reply = decode_reply(await_deliver(ReplyTopic, 3000)),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := <<"timeout">>},
        emqx_agent_tool_helpers:cap_response(Reply)
    ),

    ok = emqx:unsubscribe(ReplyTopic).

%% topic_prefix is always prepended; agent cannot target topics outside its namespace.
t_topic_prefix_applied(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "sub/topic">>,
    ReqId = <<"req-prefix-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),

    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?TOOL_ID, #{<<"topic">> => <<"sub/topic">>, <<"payload">> => <<"hi">>}, ReqId),

    %% Message must arrive on the prefixed topic, not bare "sub/topic".
    #deliver{topic = ArrivedTopic, message = ReqMsg} = await_deliver(FullTopic),
    ?assertEqual(FullTopic, ArrivedTopic),

    RT = response_topic(ReqMsg),
    _ = emqx_broker:publish(emqx_message:make(<<"sim">>, 0, RT, <<"bye">>)),
    _ = await_deliver(ReplyTopic),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Correlation fields (req_id, trace_id, iid, sid) are forwarded in the tool reply.
t_reply_correlation(_Config) ->
    DeviceTopic = <<?TOPIC_PREFIX/binary, "corr/dev">>,
    ReqId = <<"req-corr-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),

    ok = emqx:subscribe(DeviceTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?TOOL_ID,
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

%% Invocations targeting an unknown tool_id are silently dropped.
t_unknown_tool_id_ignored(_Config) ->
    ReqId = <<"req-unknown-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(<<"no-such-tool">>, #{<<"topic">> => <<"x">>, <<"payload">> => <<"y">>}, ReqId),

    receive
        #deliver{topic = ReplyTopic} -> ct:fail("unexpected reply for unknown tool_id")
    after 500 -> ok
    end,

    ok = emqx:unsubscribe(ReplyTopic).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    #{
        <<"type">> => <<"message__request">>,
        <<"id">> => ?TOOL_ID,
        <<"desc">> => <<"Test request tool">>,
        <<"topic_prefix">> => ?TOPIC_PREFIX
    }.

register_tool(Context) ->
    Body = maybe_encode_schema(<<"request_payload_schema">>, Context),
    emqx_agent_config:create_tool(Body).

maybe_encode_schema(Field, Body) ->
    case maps:get(Field, Body, undefined) of
        Schema when is_map(Schema) -> Body#{Field => emqx_utils_json:encode(Schema)};
        _ -> Body
    end.

reply_topic(ToolId, ReqId) ->
    <<"$cap/message__request/", ToolId/binary, "/response/", ReqId/binary>>.

invoke(ToolId, Args, ReqId) ->
    invoke(ToolId, Args, ReqId, #{}).

invoke(ToolId, Args, ReqId, Extra) ->
    Topic = <<"$cap/message__request/", ToolId/binary, "/request/", ReqId/binary>>,
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
    _ = emqx_broker:publish(emqx_message:make(ToolId, 0, Topic, Payload)),
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
