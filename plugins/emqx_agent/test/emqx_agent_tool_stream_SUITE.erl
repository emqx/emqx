%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_stream_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(STREAM, <<"agent_stream">>).
-define(TOPIC_FILTER, <<"agent/#">>).
-define(WRITE_ID, <<"stream-writer">>).
-define(READ_ID, <<"stream-reader">>).
-define(DEL_ID, <<"stream-deleter">>).

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
        is_lastvalue => false
    }),
    ok = register_tools(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok = emqx_streams_test_utils:cleanup_streams().

t_write_read_roundtrip(_Config) ->
    ok = invoke(<<"stream_write">>, ?WRITE_ID, #{
        <<"key">> => <<"k1">>,
        <<"data">> => #{<<"temperature">> => 23, <<"unit">> => <<"C">>}
    }),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"stream_write">>, ?WRITE_ID)),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{}),
    ?assertMatch(
        #{
            <<"status">> := <<"ok">>,
            <<"result">> := [
                #{
                    <<"key">> := <<"k1">>,
                    <<"data">> := #{<<"temperature">> := 23, <<"unit">> := <<"C">>}
                }
            ]
        },
        response(<<"stream_read">>, ?READ_ID)
    ).

t_read_by_key(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{<<"key">> => <<"k2">>}),

    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"key">> := <<"k2">>}]},
        response(<<"stream_read">>, ?READ_ID)
    ).

t_read_from_and_from_ago(_Config) ->
    From = erlang:system_time(second) - 1,
    ok = write(<<"k1">>, #{<<"v">> => 1}),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{<<"from">> => From}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"key">> := <<"k1">>}]},
        response(<<"stream_read">>, ?READ_ID)
    ),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{<<"from_ago">> => 60}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"key">> := <<"k1">>}]},
        response(<<"stream_read">>, ?READ_ID)
    ),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{<<"from">> => erlang:system_time(second) + 60}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := []},
        response(<<"stream_read">>, ?READ_ID)
    ).

t_read_rejects_conflicting_from_args(_Config) ->
    ok = invoke(<<"stream_read">>, ?READ_ID, #{<<"from">> => 1, <<"from_ago">> => 1}),
    ?assertMatch(
        #{<<"status">> := <<"error">>, <<"reason">> := <<"from conflicts with from_ago">>},
        response(<<"stream_read">>, ?READ_ID)
    ).

t_delete_by_key(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"stream_del">>, ?DEL_ID, #{<<"key">> => <<"k1">>}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"stream_del">>, ?DEL_ID)),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := [#{<<"key">> := <<"k2">>}]},
        response(<<"stream_read">>, ?READ_ID)
    ).

t_delete_all(_Config) ->
    ok = write(<<"k1">>, #{<<"v">> => 1}),
    ok = write(<<"k2">>, #{<<"v">> => 2}),

    ok = invoke(<<"stream_del">>, ?DEL_ID, #{}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"stream_del">>, ?DEL_ID)),

    ok = invoke(<<"stream_read">>, ?READ_ID, #{}),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := []},
        response(<<"stream_read">>, ?READ_ID)
    ).

write(Key, Data) ->
    ok = invoke(<<"stream_write">>, ?WRITE_ID, #{<<"key">> => Key, <<"data">> => Data}),
    ?assertMatch(#{<<"status">> := <<"ok">>}, response(<<"stream_write">>, ?WRITE_ID)),
    ok.

register_tools() ->
    ok = emqx_agent_config:create_tool(tool(<<"stream_write">>, ?WRITE_ID, <<"Write stream">>)),
    ok = emqx_agent_config:create_tool(tool(<<"stream_read">>, ?READ_ID, <<"Read stream">>)),
    ok = emqx_agent_config:create_tool(tool(<<"stream_del">>, ?DEL_ID, <<"Delete stream">>)).

tool(Type, Id, Desc) ->
    #{<<"type">> => Type, <<"id">> => Id, <<"desc">> => Desc, <<"stream">> => ?STREAM}.

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
