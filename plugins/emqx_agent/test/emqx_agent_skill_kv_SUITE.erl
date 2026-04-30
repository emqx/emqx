%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_kv_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_ID, <<"test-kv">>).
-define(LOOKUP_TOPIC, <<"cap/kv.lookup/test-kv/request">>).
-define(PUT_TOPIC, <<"cap/kv.put/test-kv/request">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_kv:create(test_context('kv.lookup')),
    ok = emqx_agent_skill_kv:create(test_context('kv.put')),
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = emqx_agent_skill_kv:destroy_lookup(?SKILL_ID),
    _ = emqx_agent_skill_kv:destroy_put(?SKILL_ID).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_registers_lookup_and_put(_Config) ->
    {ok, L} = emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID),
    {ok, P} = emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID),
    ?assertEqual(<<"kv.lookup">>, maps:get(type, L)),
    ?assertEqual(<<"kv.put">>, maps:get(type, P)).

t_no_put_when_disallowed(_Config) ->
    %% Tear down the put skill created in init; lookup must still be accessible.
    ok = emqx_agent_skill_kv:destroy_put(?SKILL_ID),
    {ok, _} = emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID)),
    %% Re-create put so end_per_testcase cleanup does not fail silently.
    ok = emqx_agent_skill_kv:create(test_context('kv.put')).

t_put_then_lookup(_Config) ->
    ReqIdPut = <<"req-put-1">>,
    ReplyPut = reply_topic_put(ReqIdPut),
    ok = emqx:subscribe(ReplyPut),

    Asset = #{
        <<"asset_id">> => <<"ahu-17">>, <<"criticality">> => <<"high">>, <<"sla">> => <<"4h">>
    },
    put_invoke(?SKILL_ID, <<"room-42">>, Asset, ReqIdPut),

    PutReply = await_reply(ReplyPut),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"ok">>}}, PutReply),
    ok = emqx:unsubscribe(ReplyPut),

    ReqIdLookup = <<"req-lookup-1">>,
    ReplyLookup = reply_topic_lookup(ReqIdLookup),
    ok = emqx:subscribe(ReplyLookup),

    lookup_invoke(?SKILL_ID, <<"room-42">>, ReqIdLookup),

    LookupReply = await_reply(ReplyLookup),
    ?assertMatch(
        #{
            <<"data">> := #{
                <<"status">> := <<"ok">>, <<"data">> := #{<<"asset_id">> := <<"ahu-17">>}
            }
        },
        LookupReply
    ),
    ok = emqx:unsubscribe(ReplyLookup).

t_lookup_not_found(_Config) ->
    ReqId = <<"req-nf-1">>,
    ReplyTopic = reply_topic_lookup(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    lookup_invoke(?SKILL_ID, <<"no-such-key">>, ReqId),

    Reply = await_reply(ReplyTopic),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"not_found">>}}, Reply),
    ok = emqx:unsubscribe(ReplyTopic).

t_destroy_cleans_up(_Config) ->
    ok = emqx_agent_skill_kv:destroy_lookup(?SKILL_ID),
    ok = emqx_agent_skill_kv:destroy_put(?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID)),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID)),
    %% Re-create so end_per_testcase cleanup does not crash.
    ok = emqx_agent_skill_kv:create(test_context('kv.lookup')),
    ok = emqx_agent_skill_kv:create(test_context('kv.put')).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context(Type) ->
    #{
        type => Type,
        skill_id => ?SKILL_ID,
        desc => <<"Test KV store">>,
        data_schema => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"asset_id">> => #{<<"type">> => <<"string">>},
                <<"criticality">> => #{<<"type">> => <<"string">>},
                <<"sla">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"asset_id">>, <<"criticality">>, <<"sla">>]
        }
    }.

put_invoke(SkillId, Key, Data, ReqId) ->
    Topic = <<"cap/kv.put/", SkillId/binary, "/request">>,
    Payload = emqx_utils_json:encode(#{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"key">> => Key, <<"data">> => Data}
    }),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)).

lookup_invoke(SkillId, Key, ReqId) ->
    Topic = <<"cap/kv.lookup/", SkillId/binary, "/request">>,
    Payload = emqx_utils_json:encode(#{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"key">> => Key}
    }),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)).

reply_topic_put(ReqId) ->
    <<"cap/kv.put/", ?SKILL_ID/binary, "/response/", ReqId/binary>>.

reply_topic_lookup(ReqId) ->
    <<"cap/kv.lookup/", ?SKILL_ID/binary, "/response/", ReqId/binary>>.

await_reply(ReplyTopic) ->
    receive
        {deliver, ReplyTopic, #message{payload = P}} -> emqx_utils_json:decode(P)
    after 3000 ->
        ct:fail(no_reply_received)
    end.
