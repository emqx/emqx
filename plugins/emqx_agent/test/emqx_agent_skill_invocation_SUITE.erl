%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_invocation_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_TYPE, <<"test.invoke">>).
-define(SKILL_ID, <<"test-invoke-1">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_resource, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_registry:put_runtime_for_test(#{
        skill_id => ?SKILL_ID,
        type => ?SKILL_TYPE,
        module => emqx_agent_skill_test_invoke,
        display_name => <<"Test Invoke">>,
        description => <<"Test skill for invocation worker">>
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:delete_runtime_for_test(?SKILL_TYPE, ?SKILL_ID).

%% A basic invoke replies with status=ok and echoes the args.
t_successful_invoke(_Config) ->
    ReqId = <<"req-success-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(#{<<"topic">> => <<"hello">>}, ReqId, #{<<"timeout_ms">> => 5_000}),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"ok">>}, Response),
    ?assertMatch(#{<<"result">> := #{<<"topic">> := <<"hello">>}}, Response),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% When the skill exceeds timeout_ms the worker kills it and replies with error/timeout.
t_timeout(_Config) ->
    ReqId = <<"req-timeout-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(
        #{<<"action">> => <<"sleep">>, <<"duration">> => 5_000},
        ReqId,
        #{<<"timeout_ms">> => 100}
    ),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    ?assertMatch(#{<<"reason">> := <<"timeout">>}, Response),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% When the skill crashes the worker replies with error and a formatted reason.
t_crash(_Config) ->
    ReqId = <<"req-crash-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(#{<<"action">> => <<"crash">>}, ReqId, #{<<"timeout_ms">> => 5_000}),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    ?assert(is_binary(maps:get(<<"reason">>, Response))),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% Dispatching to an unregistered skill publishes an error reply.
t_skill_not_found(_Config) ->
    ReqId = <<"req-not-found-1">>,
    ok = emqx_agent_skill_registry:delete_runtime_for_test(?SKILL_TYPE, ?SKILL_ID),
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(#{<<"topic">> => <<"hello">>}, ReqId),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    ?assertMatch(#{<<"reason">> := <<"skill_not_found">>}, Response),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% A non-map JSON payload triggers an error reply.
t_payload_not_a_map(_Config) ->
    ReqId = <<"req-not-map-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke_raw(?SKILL_TYPE, ?SKILL_ID, ReqId, <<"\"just a string\"">>),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    ?assertMatch(#{<<"reason">> := <<"payload_not_a_map">>}, Response),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% Malformed JSON payload triggers an error reply.
t_invalid_payload(_Config) ->
    ReqId = <<"req-invalid-json-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke_raw(?SKILL_TYPE, ?SKILL_ID, ReqId, <<"{broken">>),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    Reason = maps:get(<<"reason">>, Response),
    ?assert(is_binary(Reason)),
    ?assertNotEqual(nomatch, binary:match(Reason, <<"invalid_payload">>)),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

%% When start_invocation fails, an error reply is published.
t_start_invocation_failure(_Config) ->
    ReqId = <<"req-start-fail-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    %% Negative timeout makes erlang:send_after/3 crash in init,
    %% causing start_link to return {error, badarg}.
    invoke(#{<<"topic">> => <<"hello">>}, ReqId, #{<<"timeout_ms">> => -1}),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertMatch(#{<<"status">> := <<"error">>}, Response),
    ?assert(is_binary(maps:get(<<"reason">>, Response))),
    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),

    ok = emqx:unsubscribe(ReplyTopic).

reply_topic(ReqId) ->
    reply_topic(?SKILL_TYPE, ?SKILL_ID, ReqId).

reply_topic(Type, SkillId, ReqId) ->
    <<"cap/", Type/binary, "/", SkillId/binary, "/response/", ReqId/binary>>.

invoke(Args, ReqId) ->
    invoke(Args, ReqId, #{}).

invoke(Args, ReqId, Extra) ->
    invoke_with_skill(?SKILL_TYPE, ?SKILL_ID, Args, ReqId, Extra).

invoke_with_skill(Type, SkillId, Args, ReqId) ->
    invoke_with_skill(Type, SkillId, Args, ReqId, #{}).

invoke_with_skill(Type, SkillId, Args, ReqId, Extra) ->
    Topic = <<"cap/", Type/binary, "/", SkillId/binary, "/request/", ReqId/binary>>,
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

invoke_raw(Type, SkillId, ReqId, Payload) ->
    Topic = <<"cap/", Type/binary, "/", SkillId/binary, "/request/", ReqId/binary>>,
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)),
    ok.

await_deliver(Topic) ->
    receive
        #deliver{topic = Topic} = D -> D
    after 3_000 ->
        ct:fail("no message on ~s within 3 s", [Topic])
    end.

decode_reply(#deliver{message = #message{payload = P}}) ->
    emqx_utils_json:decode(P).
