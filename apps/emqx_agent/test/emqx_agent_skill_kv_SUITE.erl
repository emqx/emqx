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
-define(LOOKUP_TOPIC, <<"cap/invoke/kv.lookup/test-kv">>).
-define(PUT_TOPIC, <<"cap/invoke/kv.put/test-kv">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_agent], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_skill_kv:create(test_context()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_kv:destroy(?SKILL_ID).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_registers_lookup_and_put(_Config) ->
    {ok, L} = emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID),
    {ok, P} = emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID),
    ?assertEqual(<<"kv.lookup">>, maps:get(type, L)),
    ?assertEqual(<<"kv.put">>, maps:get(type, P)).

t_no_put_when_disallowed(_Config) ->
    ok = emqx_agent_skill_kv:destroy(?SKILL_ID),
    ok = emqx_agent_skill_kv:create(test_context(#{allow_put => false})),
    {ok, _} = emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID)).

t_put_then_lookup(_Config) ->
    ReqIdPut = <<"req-put-1">>,
    ReplyPut = <<?REPLY_TOPIC_PREFIX/binary, ReqIdPut/binary>>,
    ok = emqx:subscribe(ReplyPut),

    Asset = #{<<"asset_id">> => <<"ahu-17">>, <<"criticality">> => <<"high">>, <<"sla">> => <<"4h">>},
    put_invoke(?SKILL_ID, <<"room-42">>, Asset, ReqIdPut),

    PutReply = await_reply(ReplyPut),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"ok">>}}, PutReply),
    ok = emqx:unsubscribe(ReplyPut),

    ReqIdLookup = <<"req-lookup-1">>,
    ReplyLookup = <<?REPLY_TOPIC_PREFIX/binary, ReqIdLookup/binary>>,
    ok = emqx:subscribe(ReplyLookup),

    lookup_invoke(?SKILL_ID, <<"room-42">>, ReqIdLookup),

    LookupReply = await_reply(ReplyLookup),
    ?assertMatch(
        #{<<"data">> := #{<<"status">> := <<"ok">>, <<"data">> := #{<<"asset_id">> := <<"ahu-17">>}}},
        LookupReply
    ),
    ok = emqx:unsubscribe(ReplyLookup).

t_lookup_not_found(_Config) ->
    ReqId = <<"req-nf-1">>,
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    ok = emqx:subscribe(ReplyTopic),

    lookup_invoke(?SKILL_ID, <<"no-such-key">>, ReqId),

    Reply = await_reply(ReplyTopic),
    ?assertMatch(#{<<"data">> := #{<<"status">> := <<"not_found">>}}, Reply),
    ok = emqx:unsubscribe(ReplyTopic).

t_destroy_cleans_up(_Config) ->
    ok = emqx_agent_skill_kv:destroy(?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.lookup">>, ?SKILL_ID)),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(<<"kv.put">>, ?SKILL_ID)),
    %% Re-create so end_per_testcase:destroy() doesn't crash on unregister
    ok = emqx_agent_skill_kv:create(test_context()).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_context() ->
    test_context(#{}).

test_context(Overrides) ->
    maps:merge(
        #{
            skill_id => ?SKILL_ID,
            desc => <<"Test KV store">>,
            allow_put => true,
            data_schema => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"asset_id">> => #{<<"type">> => <<"string">>},
                    <<"criticality">> => #{<<"type">> => <<"string">>},
                    <<"sla">> => #{<<"type">> => <<"string">>}
                },
                <<"required">> => [<<"asset_id">>, <<"criticality">>, <<"sla">>]
            }
        },
        Overrides
    ).

put_invoke(SkillId, Key, Data, ReqId) ->
    Topic = <<"cap/invoke/kv.put/", SkillId/binary>>,
    Payload = emqx_utils_json:encode(#{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"key">> => Key, <<"data">> => Data}
    }),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)).

lookup_invoke(SkillId, Key, ReqId) ->
    Topic = <<"cap/invoke/kv.lookup/", SkillId/binary>>,
    Payload = emqx_utils_json:encode(#{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-1">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => #{<<"key">> => Key}
    }),
    _ = emqx_broker:publish(emqx_message:make(SkillId, 0, Topic, Payload)).

await_reply(ReplyTopic) ->
    receive
        {deliver, ReplyTopic, #message{payload = P}} -> emqx_utils_json:decode(P)
    after 3000 ->
        ct:fail(no_reply_received)
    end.
