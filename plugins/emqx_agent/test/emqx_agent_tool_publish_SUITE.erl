%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TOOL_ID, <<"test-publish">>).
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
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"message__publish">>, ?TOOL_ID),
    ?assertMatch(#{type := <<"message__publish">>}, Tool),
    ?assertEqual(?TOOL_ID, maps:get(tool_id, Tool)).

%% destroy/1 accepts the full runtime tool.
t_destroy_accepts_runtime_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"message__publish">>, ?TOOL_ID),
    ?assertEqual(ok, emqx_agent_tool_publish:destroy(Tool)).

t_custom_payload_schema_is_stored(_Config) ->
    ok = emqx_agent_config:delete_tool(<<"message__publish">>, ?TOOL_ID),
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
        <<"required">> => [<<"command">>, <<"direction">>],
        <<"additionalProperties">> => false
    },
    ok = register_tool(maps:put(<<"payload_schema">>, CustomPayloadSchema, test_context())),
    {ok, Tool} = emqx_agent_tool_registry:lookup(<<"message__publish">>, ?TOOL_ID),
    #{context := Context, input_schema := InputSchema} = Tool,
    ?assertMatch(#{<<"payload_schema">> := CustomPayloadSchema}, Context),
    Props = maps:get(<<"properties">>, InputSchema),
    ?assertEqual(CustomPayloadSchema, maps:get(<<"payload">>, Props)).

t_create_requires_payload_schema(_Config) ->
    Context = maps:without([<<"payload_schema">>], test_context()),
    RuntimeContext = #{
        <<"tool_id">> => maps:get(<<"id">>, Context),
        <<"desc">> => maps:get(<<"desc">>, Context),
        <<"topic_prefix">> => maps:get(<<"topic_prefix">>, Context)
    },
    ?assertEqual({error, missing_payload_schema}, emqx_agent_tool_publish:create(RuntimeContext)).

t_create_rejects_invalid_payload_schema(_Config) ->
    RuntimeContext = #{
        <<"tool_id">> => ?TOOL_ID,
        <<"desc">> => <<"bad schema">>,
        <<"topic_prefix">> => ?TOPIC_PREFIX,
        <<"payload_schema">> => #{<<"type">> => <<"object">>}
    },
    ?assertMatch(
        {error, {invalid_payload_schema, _}}, emqx_agent_tool_publish:create(RuntimeContext)
    ).

t_create_rejects_malformed_payload_schema(_Config) ->
    RuntimeContext = #{
        <<"tool_id">> => ?TOOL_ID,
        <<"desc">> => <<"bad schema">>,
        <<"topic_prefix">> => ?TOPIC_PREFIX,
        <<"payload_schema">> => <<"{">>
    },
    ?assertMatch(
        {error, {invalid_payload_schema, {invalid_json, _}}},
        emqx_agent_tool_publish:create(RuntimeContext)
    ).

%% A basic invoke publishes to prefix+topic and replies with status=ok.
t_publish_basic(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "temperature">>,
    ReqId = <<"req-pub-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?TOOL_ID, #{<<"topic">> => <<"temperature">>, <<"payload">> => <<"23.5">>}, ReqId),

    %% The published message must arrive on the full topic.
    ?assertMatch(
        #deliver{topic = FullTopic, message = #message{payload = <<"23.5">>}},
        await_deliver(FullTopic)
    ),

    %% The reply must confirm success and echo the full topic.
    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_tool_helpers:cap_response(Reply),
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
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(?TOOL_ID, #{<<"topic">> => <<"sub/path">>, <<"payload">> => <<"hello">>}, ReqId),

    ?assertMatch(
        #deliver{topic = FullTopic, message = #message{payload = <<"hello">>}},
        await_deliver(FullTopic)
    ),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertEqual(
        FullTopic,
        nested_get([<<"result">>, <<"topic">>], emqx_agent_tool_helpers:cap_response(Reply))
    ),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Optional `from` and `qos` args are accepted and forwarded correctly.
t_publish_with_from_and_qos(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "cmd">>,
    ReqId = <<"req-from-qos-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?TOOL_ID,
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
    ?assertMatch(#{<<"status">> := <<"ok">>}, emqx_agent_tool_helpers:cap_response(Reply)),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

t_publish_rejects_invalid_payload(_Config) ->
    FullTopic = <<?TOPIC_PREFIX/binary, "bad">>,
    ReqId = <<"req-invalid-payload-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(FullTopic),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?TOOL_ID, #{<<"topic">> => <<"bad">>, <<"payload">> => #{<<"message">> => <<"x">>}}, ReqId
    ),

    receive
        #deliver{topic = FullTopic} -> ct:fail("invalid payload was published")
    after 500 ->
        ok
    end,

    Reply = decode_reply(await_deliver(ReplyTopic)),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := _},
        emqx_agent_tool_helpers:cap_response(Reply)
    ),

    ok = emqx:unsubscribe(FullTopic),
    ok = emqx:unsubscribe(ReplyTopic).

%% Correlation fields (req_id, trace_id, iid, sid) are echoed in the reply.
t_reply_correlation(_Config) ->
    ReqId = <<"req-corr-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        ?TOOL_ID,
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

%% An invoke targeting an unregistered tool_id is silently dropped — no reply arrives.
t_unknown_tool_id_ignored(_Config) ->
    ReqId = <<"req-unknown-1">>,
    ReplyTopic = reply_topic(?TOOL_ID, ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        <<"no-such-tool">>,
        #{<<"topic">> => <<"x">>, <<"payload">> => <<"y">>},
        ReqId
    ),

    receive
        #deliver{topic = ReplyTopic} -> ct:fail("unexpected reply for unknown tool_id")
    after 500 ->
        ok
    end,

    ok = emqx:unsubscribe(ReplyTopic).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    #{
        <<"type">> => <<"message__publish">>,
        <<"id">> => ?TOOL_ID,
        <<"desc">> => <<"Test publish tool">>,
        <<"topic_prefix">> => ?TOPIC_PREFIX,
        <<"payload_schema">> => string_payload_schema()
    }.

string_payload_schema() ->
    #{<<"type">> => <<"string">>}.

register_tool(Context) ->
    Body = maybe_encode_schema(<<"payload_schema">>, Context),
    emqx_agent_config:create_tool(Body).

maybe_encode_schema(Field, Body) ->
    case maps:get(Field, Body, undefined) of
        Schema when is_map(Schema) -> Body#{Field => emqx_utils_json:encode(Schema)};
        _ -> Body
    end.

invoke(ToolId, Args, ReqId) ->
    invoke(ToolId, Args, ReqId, #{}).

invoke(ToolId, Args, ReqId, Extra) ->
    Topic = <<"$cap/message__publish/", ToolId/binary, "/request/", ReqId/binary>>,
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

reply_topic(ToolId, ReqId) ->
    <<"$cap/message__publish/", ToolId/binary, "/response/", ReqId/binary>>.

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
