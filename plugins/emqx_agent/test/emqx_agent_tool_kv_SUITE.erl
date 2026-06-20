%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_kv_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(STREAM, <<"agent_kv">>).
-define(REGULAR_STREAM, <<"agent_regular">>).
-define(TOPIC_FILTER, <<"kv/#">>).
-define(WRITE_ID, <<"kv-writer">>).
-define(READ_ID, <<"kv-reader">>).
-define(READ_ALL_ID, <<"kv-reader-all">>).
-define(DEL_ID, <<"kv-deleter">>).
-define(CLEAR_ID, <<"kv-clearer">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx, emqx_config()},
            {emqx_mq, emqx_mq_config()},
            {emqx_streams, emqx_streams_config()},
            emqx_conf,
            emqx_resource,
            emqx_agent
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_streams_registry:delete_all(),
    ok = emqx_agent_plugin_config_fixture:setup(),
    {ok, _Stream} = emqx_streams_registry:create(stream(?STREAM, true)),
    ok = register_tools(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok = emqx_streams_registry:delete_all().

t_write_read(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),

    ok = invoke(<<"kv_read">>, ?READ_ID, #{<<"key">> => <<"k1">>}),

    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"v">> := 1}]},
        response(<<"kv_read">>, ?READ_ID)
    ).

t_write_overwrites_key(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k1">>, #{<<"v">> => 2}),

    ok = invoke(<<"kv_read">>, ?READ_ID, #{<<"key">> => <<"k1">>}),

    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"v">> := 2}]},
        response(<<"kv_read">>, ?READ_ID)
    ).

t_read_all(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"kv_read_all">>, ?READ_ALL_ID, #{}),

    #{<<"status">> := <<"ok">>, <<"result">> := Items} = response(<<"kv_read_all">>, ?READ_ALL_ID),
    ?assertEqual(
        [
            #{<<"key">> => <<"k1">>, <<"value">> => #{<<"v">> => 1}},
            #{<<"key">> => <<"k2">>, <<"value">> => #{<<"v">> => 2}}
        ],
        lists:sort(Items)
    ).

t_del(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"kv_del">>, ?DEL_ID, #{<<"key">> => <<"k1">>}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"kv_del">>, ?DEL_ID)),

    ok = invoke(<<"kv_read_all">>, ?READ_ALL_ID, #{}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"key">> := <<"k2">>}]},
        response(<<"kv_read_all">>, ?READ_ALL_ID)
    ).

t_clear(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"kv_clear">>, ?CLEAR_ID, #{}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"kv_clear">>, ?CLEAR_ID)),

    ok = invoke(<<"kv_read_all">>, ?READ_ALL_ID, #{}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := []},
        response(<<"kv_read_all">>, ?READ_ALL_ID)
    ).

t_create_rejects_regular_stream(_Config) ->
    {ok, _Stream} = emqx_streams_registry:create(stream(?REGULAR_STREAM, false)),

    ?assertEqual(
        {error, stream_is_not_lastvalue},
        emqx_agent_tool_kv:create(tool(<<"kv_read">>, <<"bad-kv">>, <<"Bad KV">>, ?REGULAR_STREAM))
    ).

write(Key, Payload) ->
    ok = invoke(<<"kv_write">>, ?WRITE_ID, #{<<"key">> => Key, <<"payload">> => Payload}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"kv_write">>, ?WRITE_ID)),
    ok.

register_tools() ->
    ok = emqx_agent_config:create_tool(tool(<<"kv_write">>, ?WRITE_ID, <<"Write KV">>, ?STREAM)),
    ok = emqx_agent_config:create_tool(tool(<<"kv_read">>, ?READ_ID, <<"Read KV">>, ?STREAM)),
    ok = emqx_agent_config:create_tool(
        tool(<<"kv_read_all">>, ?READ_ALL_ID, <<"Read All KV">>, ?STREAM)
    ),
    ok = emqx_agent_config:create_tool(tool(<<"kv_del">>, ?DEL_ID, <<"Delete KV">>, ?STREAM)),
    ok = emqx_agent_config:create_tool(tool(<<"kv_clear">>, ?CLEAR_ID, <<"Clear KV">>, ?STREAM)).

tool(Type, Id, Desc, Stream) ->
    #{<<"type">> => Type, <<"id">> => Id, <<"desc">> => Desc, <<"stream">> => Stream}.

stream(Name, IsLastValue) ->
    {ok, KeyExpression} = emqx_variform:compile(<<"message.from">>),
    #{
        name => Name,
        topic_filter => ?TOPIC_FILTER,
        key_expression => KeyExpression,
        is_lastvalue => IsLastValue,
        limits => #{max_shard_message_count => infinity, max_shard_message_bytes => infinity},
        data_retention_period => 7 * 24 * 60 * 60 * 1000,
        read_max_unacked => 1000
    }.

invoke(Type, ToolId, Args) ->
    ReqId = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    erlang:put({req_id, Type, ToolId}, ReqId),
    ok = emqx:subscribe(reply_topic(Type, ToolId, ReqId)),
    Topic = <<"$cap/", Type/binary, "/", ToolId/binary, "/request/", ReqId/binary>>,
    Payload = emqx_utils_json:encode(#{
        <<"trace_id">> => null,
        <<"iid">> => null,
        <<"sid">> => null,
        <<"args">> => Args
    }),
    _ = emqx_broker:publish(emqx_message:make(ToolId, 0, Topic, Payload)),
    ok.

response(Type, ToolId) ->
    ReqId = erlang:get({req_id, Type, ToolId}),
    ReplyTopic = reply_topic(Type, ToolId, ReqId),
    Reply = decode_reply(await_deliver(ReplyTopic)),
    ok = emqx:unsubscribe(ReplyTopic),
    emqx_agent_tool_helpers:cap_response(Reply).

reply_topic(Type, ToolId, ReqId) ->
    <<"$cap/", Type/binary, "/", ToolId/binary, "/response/", ReqId/binary>>.

await_deliver(Topic) ->
    receive
        #deliver{topic = Topic} = D -> D
    after 3000 ->
        ct:fail("no message on ~s", [Topic])
    end.

decode_reply(#deliver{message = #message{payload = P}}) ->
    emqx_utils_json:decode(P).

emqx_config() ->
    #{
        config => #{
            <<"durable_storage">> => #{
                <<"streams_messages">> => #{
                    <<"n_shards">> => 2,
                    <<"transaction">> => #{
                        <<"flush_interval">> => 100,
                        <<"idle_flush_interval">> => 20,
                        <<"conflict_window">> => 5000
                    },
                    <<"subscriptions">> => #{<<"batch_size">> => 1}
                }
            }
        }
    }.

emqx_mq_config() ->
    #{config => #{<<"mq">> => #{<<"enable">> => false}}}.

emqx_streams_config() ->
    #{
        config => #{
            <<"streams">> => #{
                <<"enable">> => true,
                <<"max_stream_count">> => 1000,
                <<"auto_create">> => #{<<"regular">> => false, <<"lastvalue">> => false}
            }
        },
        after_start => fun() -> started = emqx_streams_controller:wait_status(15_000) end
    }.
