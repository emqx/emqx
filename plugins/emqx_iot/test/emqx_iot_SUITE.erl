%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_iot.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setup / Teardown
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, mria],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_iot:init_tables(),
    init_test_config(),
    _ = application:ensure_all_started(prometheus),
    emqx_iot_metrics:init(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    [
        mnesia:clear_table(T)
     || T <- [
            iot_mq_msg, iot_mq_message, iot_mq_message_hash, iot_mq_message_api_id
        ]
    ],
    catch emqx_iot:init_tables(),
    catch ets:delete_all_objects(iot_mq_msg_index),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

init_test_config() ->
    Cfg = #{
        msg_ttl => 15 * 86400,
        cleanup_interval => 60,
        max_device_count => 10000,
        max_message_size_batch => 10240,
        max_message_size_broadcast => 65536,
        broadcast_topic => <<"/sys/broadcast/${productKey}">>,
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>
    },
    persistent_term:put({?APP, config}, Cfg),
    ok.

%%--------------------------------------------------------------------
%% Config tests
%%--------------------------------------------------------------------

t_config_defaults(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    ?assertEqual(10000, maps:get(max_device_count, Cfg)),
    ?assertEqual(15 * 86400, maps:get(msg_ttl, Cfg)),
    ?assertEqual(10240, maps:get(max_message_size_batch, Cfg)),
    ?assertEqual(65536, maps:get(max_message_size_broadcast, Cfg)),
    ?assert(is_binary(maps:get(broadcast_topic, Cfg))),
    ?assert(is_binary(maps:get(batch_topic, Cfg))).

%%--------------------------------------------------------------------
%% ID Mapping tests
%%--------------------------------------------------------------------

t_generate_message_id(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    ?assert(is_binary(ApiMsgId)),
    ?assert(is_binary(MsgGuid)),
    ?assertEqual(16, byte_size(MsgGuid)),
    ?assert(ApiMsgId =/= MsgGuid).

t_resolve_message_id_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_iot_id:resolve_message_id(<<"nonexistent">>)).

t_resolve_message_id_found(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"test payload">>),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"test payload">>),
    ?assertEqual({ok, MsgGuid}, emqx_iot_id:resolve_message_id(ApiMsgId)).

%%--------------------------------------------------------------------
%% Storage / Mnesia tests
%%--------------------------------------------------------------------

t_create_and_lookup_message(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"hello world">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_iot_storage:lookup_message(MsgGuid),
    ?assertEqual(Payload, Msg#iot_mq_message.payload),
    ?assertEqual(Hash, Msg#iot_mq_message.content_hash),
    ?assertEqual(ApiMsgId, Msg#iot_mq_message.api_msg_id).

t_lookup_by_hash(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"dedup test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_iot_storage:lookup_message_by_hash(Hash),
    ?assertEqual(MsgGuid, Msg#iot_mq_message.msg_id).

t_refresh_message_ttl(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"ttl test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg1} = emqx_iot_storage:lookup_message(MsgGuid),
    timer:sleep(1100),
    emqx_iot_storage:refresh_message_ttl(MsgGuid),
    {ok, Msg2} = emqx_iot_storage:lookup_message(MsgGuid),
    ?assert(Msg2#iot_mq_message.expires_at > Msg1#iot_mq_message.expires_at).

t_create_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"delivery test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_iot_utils:gen_guid(),
    DNs = [<<"D1">>, <<"D2">>, <<"D3">>],
    PK = <<"P1">>,
    D = emqx_iot_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 3, undefined),
    ?assertEqual(0, D#iot_mq_msg.counter),
    ?assertEqual(3, D#iot_mq_msg.target_ack_count),
    {ok, Ids} = emqx_iot_storage:get_device_deliveries({PK, <<"D1">>}),
    ?assertEqual([DeliveryId], Ids).

t_process_ack(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"ack test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_iot_utils:gen_guid(),
    DNs = [<<"DA">>, <<"DB">>],
    PK = <<"PA">>,
    emqx_iot_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2, undefined),
    emqx_iot_storage:process_ack(PK, <<"DA">>, DeliveryId),
    {ok, IdsA} = emqx_iot_storage:get_device_deliveries({PK, <<"DA">>}),
    ?assertEqual([], IdsA),
    {ok, IdsB} = emqx_iot_storage:get_device_deliveries({PK, <<"DB">>}),
    ?assertEqual([DeliveryId], IdsB).

t_process_ack_all_devices(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"ack all">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_iot_utils:gen_guid(),
    DNs = [<<"DX">>],
    PK = <<"PX">>,
    emqx_iot_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1, undefined),
    emqx_iot_storage:process_ack(PK, <<"DX">>, DeliveryId),
    ?assertEqual({error, not_found}, emqx_iot_storage:lookup_message(DeliveryId)).

t_process_ack_duplicate(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"dup ack">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_iot_utils:gen_guid(),
    DNs = [<<"DD">>, <<"DE">>],
    PK = <<"PD">>,
    emqx_iot_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2, undefined),
    emqx_iot_storage:process_ack(PK, <<"DD">>, DeliveryId),
    emqx_iot_storage:process_ack(PK, <<"DD">>, DeliveryId),
    {ok, Ids} = emqx_iot_storage:get_device_deliveries({PK, <<"DE">>}),
    ?assertEqual([DeliveryId], Ids).

t_cleanup_expired_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Payload = <<"expire test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_iot_utils:gen_guid(),
    DNs = [<<"DE">>],
    PK = <<"PE">>,
    D = emqx_iot_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1, undefined),
    mnesia:dirty_write(D#iot_mq_msg{expires_at = 0}),
    emqx_iot_storage:cleanup_expired(),
    ?assertEqual({error, not_found}, emqx_iot_storage:lookup_message(DeliveryId)).

%%--------------------------------------------------------------------
%% Utils tests
%%--------------------------------------------------------------------

t_topic_expansion(_Config) ->
    Result = emqx_iot_utils:expand_topic(
        <<"/${productKey}/${deviceName}/user/get">>,
        <<"P1">>,
        <<"D1">>
    ),
    ?assertEqual(<<"/P1/D1/user/get">>, Result).

t_sha256(_Config) ->
    Hash = emqx_iot_utils:sha256(<<"test">>),
    ?assertEqual(32, byte_size(Hash)).

t_base64_decode(_Config) ->
    ?assertEqual({ok, <<"hello">>}, emqx_iot_utils:decode_base64(<<"aGVsbG8=">>)),
    ?assertEqual({error, invalid_base64}, emqx_iot_utils:decode_base64(<<"!!!">>)).

%%--------------------------------------------------------------------
%% API tests
%%--------------------------------------------------------------------

t_api_missing_action(_Config) ->
    Body = #{<<"ProductKey">> => <<"P1">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(false, maps:get(<<"Success">>, Resp)),
    ?assertEqual(<<"MissingAction">>, maps:get(<<"Code">>, Resp)).

t_api_unknown_action(_Config) ->
    Body = #{<<"Action">> => <<"BadAction">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"UnknownAction">>, maps:get(<<"Code">>, Resp)).

t_api_not_found(_Config) ->
    {error, not_found} = emqx_iot_api:handle(get, [<<"pub">>], #{}).

%%--------------------------------------------------------------------
%% RegisterMessage API tests
%%--------------------------------------------------------------------

t_register_message_create(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))),
    ?assert(is_binary(maps:get(<<"RequestId">>, Resp))).

t_register_message_dedup(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, _, _, Resp1} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    {ok, _, _, Resp2} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(maps:get(<<"MessageId">>, Resp1), maps:get(<<"MessageId">>, Resp2)).

t_register_message_refresh_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => <<"nonexistent-uuid">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

t_register_message_mutual_exclusion(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

t_register_message_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

t_register_message_empty(_Config) ->
    Body = #{<<"Action">> => <<"RegisterMessage">>},
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% BatchPub API tests
%%--------------------------------------------------------------------

t_batch_pub_qos0_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))).

t_batch_pub_qos1_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_batch_pub_messageid_reuse(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_iot_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"reuse">>),
    emqx_iot_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"reuse">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => ApiMsgId,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, Resp)).

t_batch_pub_messageid_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => <<"no-such-id">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

t_batch_pub_topic_template_name(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PX">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0,
        <<"TopicTemplateName">> => <<"/custom/${deviceName}/topic">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_batch_pub_topic_short_name(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PX">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0,
        <<"TopicShortName">> => <<"custom">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_batch_pub_default_topic(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_batch_pub_duplicate_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"DuplicateDeviceName">>, maps:get(<<"Code">>, Resp)).

t_batch_pub_missing_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp)).

t_batch_pub_content_id_conflict(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

t_batch_pub_neither_content_nor_id(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% PubBroadcast API tests
%%--------------------------------------------------------------------

t_broadcast_with_topic_full_name(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"TopicFullName">> => <<"/custom/broadcast/topic">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_broadcast_missing_product_key(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

t_broadcast_missing_content(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

t_broadcast_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_iot_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% Metric verification tests
%%--------------------------------------------------------------------

metric(Name) ->
    try
        prometheus_counter:value(emqx_iot_metrics:name(Name))
    catch
        _:_ ->
            try ets:lookup_element(iot_mq_counters, Name, 2) catch _:_ -> 0 end
    end.

t_metrics_qos0_targeted(_Config) ->
    Before = metric('batch_pub_qos0_targeted'),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>, <<"D3">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    {ok, 200, _, _} = emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric('batch_pub_qos0_targeted'),
    ?assertEqual(3, After - Before).

t_metrics_broadcast_in(_Config) ->
    Before = metric('broadcast_pub_in'),
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    {ok, 200, _, _} = emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric('broadcast_pub_in'),
    ?assertEqual(1, After - Before).

t_metrics_broadcast_error(_Config) ->
    Before = metric('broadcast_pub_error'),
    Body = #{<<"Action">> => <<"PubBroadcast">>, <<"MessageContent">> => <<"!!!">>},
    {ok, 400, _, _} = emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric('broadcast_pub_error'),
    ?assertEqual(1, After - Before).

t_metrics_qos1_wanted(_Config) ->
    Before = metric('batch_pub_qos1_msg_wanted'),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DA">>, <<"DB">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric('batch_pub_qos1_msg_wanted'),
    ?assertEqual(2, After - Before).

t_metrics_register_message_in(_Config) ->
    Before = metric('register_message_in'),
    Body = #{<<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => <<"dGVzdA==">>},
    {ok, 200, _, _} = emqx_iot_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric('register_message_in'),
    ?assertEqual(1, After - Before).
