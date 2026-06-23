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
-define(BIN_WRITE_ID, <<"kv-binary-writer">>).
-define(BIN_READ_ID, <<"kv-binary-reader">>).
-define(BIN_READ_ALL_ID, <<"kv-binary-reader-all">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx, emqx_streams_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
            {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)},
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
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_agent_plugin_config_fixture:setup(),
    _Stream = emqx_streams_test_utils:ensure_stream_created(#{
        name => ?STREAM,
        topic_filter => ?TOPIC_FILTER,
        is_lastvalue => true
    }),
    ok = register_tools(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok = emqx_streams_test_utils:cleanup_streams().

t_write_read(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),

    ok = invoke(<<"kv_read">>, ?READ_ID, #{<<"key">> => <<"k1">>}),

    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"v">> := 1}]},
        response(<<"kv_read">>, ?READ_ID)
    ).

t_binary_format_roundtrip(_Config) ->
    ok = invoke(<<"kv_write">>, ?BIN_WRITE_ID, #{
        <<"key">> => <<"raw">>,
        <<"payload">> => <<"not-json">>
    }),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"kv_write">>, ?BIN_WRITE_ID)),

    ok = invoke(<<"kv_read">>, ?BIN_READ_ID, #{<<"key">> => <<"raw">>}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [<<"not-json">>]},
        response(<<"kv_read">>, ?BIN_READ_ID)
    ),

    ok = invoke(<<"kv_read_all">>, ?BIN_READ_ALL_ID, #{}),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := [#{<<"key">> := <<"raw">>, <<"value">> := <<"not-json">>}]
        },
        response(<<"kv_read_all">>, ?BIN_READ_ALL_ID)
    ).

t_write_input_schema_matches_format(_Config) ->
    {ok, #{input_schema := JsonSchema}} = emqx_agent_tool_kv:create(
        tool(<<"kv_write">>, <<"json-schema-writer">>, <<"Write JSON KV">>, ?STREAM)
    ),
    ?assertMatch(
        #{<<"properties">> := #{<<"payload">> := #{<<"type">> := <<"object">>}}},
        JsonSchema
    ),

    {ok, #{input_schema := BinarySchema}} = emqx_agent_tool_kv:create(
        tool(
            <<"kv_write">>, <<"binary-schema-writer">>, <<"Write binary KV">>, ?STREAM, <<"binary">>
        )
    ),
    ?assertMatch(
        #{<<"properties">> := #{<<"payload">> := #{<<"type">> := <<"string">>}}},
        BinarySchema
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
    _Stream = emqx_streams_test_utils:ensure_stream_created(#{
        name => ?REGULAR_STREAM,
        topic_filter => ?TOPIC_FILTER,
        is_lastvalue => false
    }),

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
    ok = emqx_agent_config:create_tool(tool(<<"kv_clear">>, ?CLEAR_ID, <<"Clear KV">>, ?STREAM)),
    ok = emqx_agent_config:create_tool(
        tool(<<"kv_write">>, ?BIN_WRITE_ID, <<"Write Binary KV">>, ?STREAM, <<"binary">>)
    ),
    ok = emqx_agent_config:create_tool(
        tool(<<"kv_read">>, ?BIN_READ_ID, <<"Read Binary KV">>, ?STREAM, <<"binary">>)
    ),
    ok = emqx_agent_config:create_tool(
        tool(<<"kv_read_all">>, ?BIN_READ_ALL_ID, <<"Read All Binary KV">>, ?STREAM, <<"binary">>)
    ).

tool(Type, Id, Desc, Stream) ->
    #{<<"type">> => Type, <<"id">> => Id, <<"desc">> => Desc, <<"stream">> => Stream}.

tool(Type, Id, Desc, Stream, Format) ->
    maps:merge(tool(Type, Id, Desc, Stream), #{<<"format">> => Format}).

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
