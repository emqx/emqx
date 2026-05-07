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
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_registry:register(#{
        skill_id => ?SKILL_ID,
        type => ?SKILL_TYPE,
        module => emqx_agent_skill_test_invoke,
        display_name => <<"Test Invoke">>,
        description => <<"Test skill for invocation worker">>
    }),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_registry:unregister(?SKILL_TYPE, ?SKILL_ID).

%% A basic invoke replies with status=ok and echoes the args.
t_successful_invoke(_Config) ->
    ReqId = <<"req-success-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(#{<<"topic">> => <<"hello">>}, ReqId, #{<<"timeout_ms">> => 5_000}),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Response)),
    ?assertEqual(#{<<"topic">> => <<"hello">>}, maps:get(<<"result">>, Response)),
    ?assertEqual(ReqId, maps:get(<<"req_id">>, Reply)),

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
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Response)),
    ?assertEqual(<<"timeout">>, maps:get(<<"reason">>, Response)),
    ?assertEqual(ReqId, maps:get(<<"req_id">>, Reply)),

    ok = emqx:unsubscribe(ReplyTopic).

%% When the skill crashes the worker replies with error and a formatted reason.
t_crash(_Config) ->
    ReqId = <<"req-crash-1">>,
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    invoke(#{<<"action">> => <<"crash">>}, ReqId, #{<<"timeout_ms">> => 5_000}),

    Reply = decode_reply(await_deliver(ReplyTopic)),
    Response = emqx_agent_skill_helpers:cap_response(Reply),
    ?assertEqual(<<"error">>, maps:get(<<"status">>, Response)),
    ?assert(is_binary(maps:get(<<"reason">>, Response))),
    ?assertEqual(ReqId, maps:get(<<"req_id">>, Reply)),

    ok = emqx:unsubscribe(ReplyTopic).

reply_topic(ReqId) ->
    <<"cap/", ?SKILL_TYPE/binary, "/", ?SKILL_ID/binary, "/response/", ReqId/binary>>.

invoke(Args, ReqId) ->
    invoke(Args, ReqId, #{}).

invoke(Args, ReqId, Extra) ->
    Topic = <<"cap/", ?SKILL_TYPE/binary, "/", ?SKILL_ID/binary, "/request/", ReqId/binary>>,
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
    _ = emqx_broker:publish(emqx_message:make(?SKILL_ID, 0, Topic, Payload)),
    ok.

await_deliver(Topic) ->
    receive
        #deliver{topic = Topic} = D -> D
    after 3_000 ->
        ct:fail("no message on ~s within 3 s", [Topic])
    end.

decode_reply(#deliver{message = #message{payload = P}}) ->
    emqx_utils_json:decode(P).
