%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_bcast.hrl").

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
    ok = emqx_bcast:init_tables(),
    init_test_config(),
    application:load(prometheus),
    {ok, _} = application:ensure_all_started(prometheus),
    emqx_bcast_metrics:init(),
    %% Delivery goes through the async pool; start the plugin supervisor so
    %% pool workers exist, without starting the full application (no hooks).
    %% NOTE: start_link makes the caller the supervisor's parent, and a
    %% supervisor dies when its parent exits. The init_per_suite process
    %% exits right after setup, so a dedicated keeper process owns the
    %% supervisor for the whole suite lifetime.
    SupKeeper = spawn(fun() -> sup_keeper() end),
    [{apps, Apps}, {sup_keeper, SupKeeper} | Config].

end_per_suite(Config) ->
    ?config(sup_keeper, Config) ! stop,
    emqx_cth_suite:stop(?config(apps, Config)).

sup_keeper() ->
    {ok, _Pid} = emqx_bcast_sup:start_link(),
    receive
        stop -> ok
    end.

init_per_testcase(_Case, Config) ->
    [
        mnesia:clear_table(T)
     || T <- [
            bcast_msg, bcast_message, bcast_message_hash, bcast_message_api_id, bcast_msg_index
        ]
    ],
    catch emqx_bcast:init_tables(),
    [
        catch ets:delete_all_objects(T)
     || T <- [bcast_buffer_a, bcast_buffer_b, bcast_buffer3, bcast_device_sub, bcast_subscription]
    ],
    emqx_bcast_metrics:init(),
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
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>,
        delivery_pool_size => 2
    },
    persistent_term:put({?APP, config}, Cfg),
    ok.

%%--------------------------------------------------------------------
%% Config tests
%%--------------------------------------------------------------------

-doc "Plugin config defaults are populated with the documented values.".
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

-doc "generate_message_id returns distinct API UUID and internal GUID.".
t_generate_message_id(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ?assert(is_binary(ApiMsgId)),
    ?assert(is_binary(MsgGuid)),
    ?assertEqual(16, byte_size(MsgGuid)),
    ?assert(ApiMsgId =/= MsgGuid).

-doc "resolve_message_id returns not_found for an unknown API id.".
t_resolve_message_id_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_bcast_id:resolve_message_id(<<"nonexistent">>)).

-doc "resolve_message_id maps a stored API id back to the GUID.".
t_resolve_message_id_found(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"test payload">>),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"test payload">>),
    ?assertEqual({ok, MsgGuid}, emqx_bcast_id:resolve_message_id(ApiMsgId)).

%%--------------------------------------------------------------------
%% Storage / Mnesia tests
%%--------------------------------------------------------------------

-doc "create_message stores payload, hash and api id; lookup returns them.".
t_create_and_lookup_message(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"hello world">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assertEqual(Payload, Msg#bcast_message.payload),
    ?assertEqual(Hash, Msg#bcast_message.content_hash),
    ?assertEqual(ApiMsgId, Msg#bcast_message.api_msg_id).

-doc "lookup_message_by_hash finds a message by its content hash.".
t_lookup_by_hash(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"dedup test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message_by_hash(Hash),
    ?assertEqual(MsgGuid, Msg#bcast_message.msg_id).

-doc "refresh_message_ttl extends expires_at past the backdated expiry.".
t_refresh_message_ttl(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ttl test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    Now = emqx_bcast_utils:now_sec(),
    %% backdate expiry so a refresh is observable
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    {ok, Msg1} = emqx_bcast_storage:lookup_message(MsgGuid),
    emqx_bcast_storage:refresh_message_ttl(MsgGuid),
    {ok, Msg2} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assert(Msg2#bcast_message.expires_at > Msg1#bcast_message.expires_at).

-doc "create_delivery indexes the delivery for every target device.".
t_create_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"delivery test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"D1">>, <<"D2">>, <<"D3">>],
    PK = <<"P1">>,
    D = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 3),
    ?assertEqual(0, D#bcast_msg.counter),
    ?assertEqual(3, D#bcast_msg.target_ack_count),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}),
    ?assertEqual([DeliveryId], Ids).

-doc "process_ack removes the delivery index entry for the acking device.".
t_process_ack(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ack test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DA">>, <<"DB">>],
    PK = <<"PA">>,
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    emqx_bcast_storage:process_ack(PK, <<"DA">>, DeliveryId),
    {ok, IdsA} = emqx_bcast_storage:get_device_deliveries({PK, <<"DA">>}),
    ?assertEqual([], IdsA),
    {ok, IdsB} = emqx_bcast_storage:get_device_deliveries({PK, <<"DB">>}),
    ?assertEqual([DeliveryId], IdsB).

-doc "the delivery record is deleted once all devices have acked.".
t_process_ack_all_devices(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ack all">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DX">>],
    PK = <<"PX">>,
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1),
    emqx_bcast_storage:process_ack(PK, <<"DX">>, DeliveryId),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

-doc "duplicate acks are idempotent and do not corrupt the index.".
t_process_ack_duplicate(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"dup ack">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DD">>, <<"DE">>],
    PK = <<"PD">>,
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    emqx_bcast_storage:process_ack(PK, <<"DD">>, DeliveryId),
    emqx_bcast_storage:process_ack(PK, <<"DD">>, DeliveryId),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"DE">>}),
    ?assertEqual([DeliveryId], Ids).

-doc "cleanup_expired removes deliveries past their expiry.".
t_cleanup_expired_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"expire test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DE">>],
    PK = <<"PE">>,
    D = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1),
    mnesia:dirty_write(D#bcast_msg{expires_at = 0}),
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

%%--------------------------------------------------------------------
%% Utils tests
%%--------------------------------------------------------------------

-doc "expand_topic substitutes productKey and deviceName placeholders.".
t_topic_expansion(_Config) ->
    Result = emqx_bcast_utils:expand_topic(
        <<"/${productKey}/${deviceName}/user/get">>,
        <<"P1">>,
        <<"D1">>
    ),
    ?assertEqual(<<"/P1/D1/user/get">>, Result).

-doc "sha256 returns a 32-byte digest.".
t_sha256(_Config) ->
    Hash = emqx_bcast_utils:sha256(<<"test">>),
    ?assertEqual(32, byte_size(Hash)).

-doc "decode_base64 handles valid input and rejects invalid encoding.".
t_base64_decode(_Config) ->
    ?assertEqual({ok, <<"hello">>}, emqx_bcast_utils:decode_base64(<<"aGVsbG8=">>)),
    ?assertEqual({error, invalid_base64}, emqx_bcast_utils:decode_base64(<<"!!!">>)).

%%--------------------------------------------------------------------
%% Topic matching tests
%%--------------------------------------------------------------------

-doc "exact topic filters match the concrete topic.".
t_topic_match_exact(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/D1/user/get">>)).

-doc "plus wildcards match a single topic level.".
t_topic_match_plus(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/+/user/get">>)).

-doc "hash wildcards match any number of trailing levels.".
t_topic_match_hash(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/#">>)).

-doc "unrelated filters do not match the topic.".
t_topic_match_no_match(_Config) ->
    ?assertNot(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P2/+/user/get">>)).

%%--------------------------------------------------------------------
%% Subscription QoS match tests
%%--------------------------------------------------------------------

-doc "subscription match returns the stored subscription QoS.".
t_sub_match_returns_qos(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D1/user/get">>, 0}),
    ?assertEqual({ok, 0}, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

-doc "overlapping filters resolve to the highest QoS.".
t_sub_match_max_qos_overlapping(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/+/user/get">>, 0}),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D1/user/get">>, 1}),
    ?assertEqual({ok, 1}, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

-doc "subscription match is false without a matching filter.".
t_sub_match_no_match(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D2/user/get">>, 1}),
    ?assertEqual(false, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

-doc "stale unsubscribe/disconnect from an old connection is ignored.".
t_sub_takeover_pid_guard(_Config) ->
    emqx_bcast_subscription:init(),
    Old = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    New = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    Topic = <<"/P1/DT/user/get">>,
    emqx_bcast_subscription:add(<<"devT">>, Old, {Topic, 1}),
    %% takeover: new connection re-registers under the same ClientId
    emqx_bcast_subscription:add(<<"devT">>, New, {Topic, 1}),
    %% stale unsubscribe from the old connection must not remove the entry
    emqx_bcast_subscription:remove(<<"devT">>, Old, {Topic, 1}),
    ?assertEqual({ok, 1}, emqx_bcast_subscription:match(<<"devT">>, Topic)),
    %% stale disconnect from the old connection must not clear the entry
    emqx_bcast_subscription:clear(<<"devT">>, Old),
    ?assertEqual({ok, 1}, emqx_bcast_subscription:match(<<"devT">>, Topic)),
    %% the current owner's disconnect clears the entry
    emqx_bcast_subscription:clear(<<"devT">>, New),
    ?assertEqual(false, emqx_bcast_subscription:match(<<"devT">>, Topic)),
    Old ! stop,
    New ! stop.

-doc "message.acked removes the delivery; duplicate acks are idempotent.".
t_message_acked_hook(_Config) ->
    PK = <<"PC">>,
    DN = <<"DC1">>,
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"message acked test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        0,
        <<"/PC/DC1/user/get">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    %% delivery record removed after the target ack count is reached
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    %% duplicate ack is idempotent and does not crash: the ack path is a cast
    %% into emqx_bcast_ack_pool, so sys:get_state guarantees it was processed
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(emqx_bcast_ack_pool),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% messages without plugin headers pass through untouched
    Plain = emqx_message:make(DN, 0, <<"/t">>, <<"p">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Plain).

-doc "concurrent identical RegisterMessage calls yield one MessageId.".
t_register_message_concurrent_dedup(_Config) ->
    Content = base64:encode(crypto:strong_rand_bytes(16)),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => Content
    },
    Parent = self(),
    N = 20,
    Pids = [
        spawn(fun() ->
            Res = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
            Parent ! {reg_result, self(), Res}
        end)
     || _ <- lists:seq(1, N)
    ],
    Results = [
        receive
            {reg_result, P, R} -> R
        end
     || P <- Pids
    ],
    ?assertEqual(N, length(Results)),
    lists:foreach(fun(R) -> ?assertMatch({ok, 200, _, _}, R) end, Results),
    Ids = lists:usort([maps:get(<<"MessageId">>, Resp) || {ok, 200, _, Resp} <- Results]),
    ?assertEqual(1, length(Ids)).

-doc "re-registering content refreshes the message TTL.".
t_register_message_ttl_refresh(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = crypto:strong_rand_bytes(16),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    TTL = emqx_bcast_utils:ttl(),
    Now = emqx_bcast_utils:now_sec(),
    %% backdate expiry so a refresh is observable
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    [#bcast_message{expires_at = OldExpiry}] = mnesia:dirty_read(bcast_message, MsgGuid),
    ?assertEqual(Now - 100, OldExpiry),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => base64:encode(Payload)
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    [#bcast_message{expires_at = NewExpiry}] = mnesia:dirty_read(bcast_message, MsgGuid),
    ?assert(NewExpiry >= Now + TTL - 5).

%%--------------------------------------------------------------------
%% Subscription QoS tests
%%--------------------------------------------------------------------

-doc "QoS=1 BatchPub to a QoS=0 subscriber delivers and self-acks as QoS=0.".
t_qos1_to_qos0_subscriber(_Config) ->
    emqx_bcast:register_device(<<"P1">>, <<"DA">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DA">>, self(), {<<"/P1/DA/user/get">>, 0}),
    BeforeAcked = metric(<<"batch_pub_qos1_acked">>),
    BeforeInline = metric(<<"batch_pub_qos1_delivered">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DA">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, BeforeAcked + 1)),
    ?assertEqual(0, metric(<<"batch_pub_qos1_delivered">>) - BeforeInline),
    flush_mailbox().

-doc "QoS=1 BatchPub to a QoS=1 subscriber delivers at QoS=1 and waits for ack.".
t_qos1_to_qos1_subscriber(_Config) ->
    emqx_bcast:register_device(<<"P1">>, <<"DB">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DB">>, self(), {<<"/P1/DB/user/get">>, 1}),
    BeforeInline = metric(<<"batch_pub_qos1_delivered">>),
    BeforeAcked = metric(<<"batch_pub_qos1_acked">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DB">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(wait_metric(<<"batch_pub_qos1_delivered">>, BeforeInline + 1)),
    %% The ack path is a cast into emqx_bcast_ack_pool; sys:get_state makes
    %% the "nothing was acked" assertion deterministic instead of a bare sleep.
    _ = sys:get_state(emqx_bcast_ack_pool),
    ?assertEqual(0, metric(<<"batch_pub_qos1_acked">>) - BeforeAcked),
    flush_mailbox().

%% Poll until a prometheus counter reaches the expected value (async delivery
%% happens on pool workers, so metrics lag the API response).
wait_metric(Name, Expected) ->
    wait_until(fun() -> metric(Name) =:= Expected end, 100).

wait_until(_F, 0) ->
    false;
wait_until(F, N) ->
    case F() of
        true ->
            true;
        false ->
            timer:sleep(50),
            wait_until(F, N - 1)
    end.

flush_mailbox() ->
    receive
        #deliver{} -> flush_mailbox()
    after 0 -> ok
    end.

%%--------------------------------------------------------------------
%% Async delivery pool tests
%%--------------------------------------------------------------------

-doc "QoS=1 BatchPub stores the delivery and the pull pool delivers it.".
t_qos1_batchpub_stores_then_delivers_via_pull(_Config) ->
    emqx_bcast:register_device(<<"P1">>, <<"DQ">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DQ">>, self(), {<<"/P1/DQ/user/get">>, 0}),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DQ">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(maps:get(<<"Success">>, Resp)),
    %% Delivery record is created asynchronously by the API worker pool.
    ?assert(
        wait_until(
            fun() -> length(mnesia:dirty_match_object(#bcast_msg{_ = '_'})) =:= 1 end,
            100
        )
    ),
    %% Pull pools turn the trigger into a delivery.
    ?assert(wait_until(fun() -> count_deliver_messages() >= 1 end, 100)),
    flush_mailbox(),
    ok.

-doc "BatchPub by MessageId refreshes the message TTL asynchronously.".
t_async_ttl_refresh(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = crypto:strong_rand_bytes(16),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    TTL = emqx_bcast_utils:ttl(),
    Now = emqx_bcast_utils:now_sec(),
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"OFF1">>],
        <<"MessageId">> => ApiMsgId,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    %% TTL refresh is a fire-and-forget pool task; wait for it to land
    ?assert(
        wait_until(
            fun() ->
                [#bcast_message{expires_at = E}] = mnesia:dirty_read(bcast_message, MsgGuid),
                E >= Now + TTL - 5
            end,
            100
        )
    ).

-doc "QoS=0 broadcast delivers to all locally subscribed devices.".
t_qos0_broadcast_delivers_locally(_Config) ->
    PK = <<"PR">>,
    N = 50,
    DNs = [<<"R", (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)],
    lists:foreach(
        fun(DN) ->
            emqx_bcast:register_device(PK, DN, self()),
            emqx_bcast_subscription:add(DN, self(), {<<"/PR/#">>, 0})
        end,
        DNs
    ),
    Template = <<"/PR/${deviceName}/user/get">>,
    ok = emqx_bcast_pull_server_pool:qos0_broadcast(PK, undefined, Template, <<"p">>),
    ?assert(
        wait_until(
            fun() ->
                {message_queue_len, Len} = process_info(self(), message_queue_len),
                Len >= N
            end,
            100
        )
    ),
    ?assertEqual(N, count_deliver_messages()).

count_deliver_messages() ->
    receive
        #deliver{} -> 1 + count_deliver_messages()
    after 0 -> 0
    end.

-doc "index add/remove are idempotent for repeated calls.".
t_index_add_remove_idempotent(_Config) ->
    PK = <<"PI">>,
    DNs = [<<"D1">>, <<"D2">>],
    Did = emqx_bcast_utils:gen_guid(),
    ok = emqx_bcast_storage:add_index_entries(PK, DNs, Did),
    ok = emqx_bcast_storage:add_index_entries(PK, DNs, Did),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}),
    ?assertEqual([Did], Ids),
    ok = emqx_bcast_storage:remove_index_entries(PK, DNs, Did),
    ok = emqx_bcast_storage:remove_index_entries(PK, DNs, Did),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}).

-doc "pull pool buffer tables exist after pool start.".
t_pull_pool_buffers_initialized(_Config) ->
    ?assertNotEqual(undefined, ets:info(bcast_buffer_a)),
    ?assertNotEqual(undefined, ets:info(bcast_buffer_b)),
    ?assertNotEqual(undefined, ets:info(bcast_buffer3)).

-doc "missing Action returns 400 MissingAction.".
t_api_missing_action(_Config) ->
    Body = #{<<"ProductKey">> => <<"P1">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(false, maps:get(<<"Success">>, Resp)),
    ?assertEqual(<<"MissingAction">>, maps:get(<<"Code">>, Resp)).

-doc "unknown Action returns 400 UnknownAction.".
t_api_unknown_action(_Config) ->
    Body = #{<<"Action">> => <<"BadAction">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"UnknownAction">>, maps:get(<<"Code">>, Resp)).

-doc "unknown API path returns not_found.".
t_api_not_found(_Config) ->
    {error, not_found} = emqx_bcast_api:handle(get, [<<"pub">>], #{}).

%%--------------------------------------------------------------------
%% RegisterMessage API tests
%%--------------------------------------------------------------------

-doc "RegisterMessage creates a message and returns its MessageId.".
t_register_message_create(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))),
    ?assert(is_binary(maps:get(<<"RequestId">>, Resp))).

-doc "identical content returns the same MessageId.".
t_register_message_dedup(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, _, _, Resp1} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    {ok, _, _, Resp2} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(maps:get(<<"MessageId">>, Resp1), maps:get(<<"MessageId">>, Resp2)).

-doc "refreshing an unknown MessageId returns 400 MessageNotFound.".
t_register_message_refresh_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => <<"nonexistent-uuid">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "MessageContent and MessageId together return 400.".
t_register_message_mutual_exclusion(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "invalid Base64 returns 400 InvalidBase64.".
t_register_message_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

-doc "payloads over max_message_size_batch return 400 MessageTooLarge.".
t_register_message_too_large(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    MaxSize = maps:get(max_message_size_batch, Cfg, 10240),
    Payload = crypto:strong_rand_bytes(MaxSize + 1),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => base64:encode(Payload)
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageTooLarge">>, maps:get(<<"Code">>, Resp)).

-doc "RegisterMessage with no content or id returns 400.".
t_register_message_empty(_Config) ->
    Body = #{<<"Action">> => <<"RegisterMessage">>},
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% BatchPub API tests
%%--------------------------------------------------------------------

-doc "QoS=0 inline BatchPub is accepted.".
t_batch_pub_qos0_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))).

-doc "QoS=1 inline BatchPub is accepted.".
t_batch_pub_qos1_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "BatchPub by MessageId reuses the stored payload and returns the id.".
t_batch_pub_messageid_reuse(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"reuse">>),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"reuse">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => ApiMsgId,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, Resp)).

-doc "BatchPub with an unknown MessageId returns 400 MessageNotFound.".
t_batch_pub_messageid_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => <<"no-such-id">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "TopicTemplateName overrides the delivery topic.".
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
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "TopicShortName builds the delivery topic suffix.".
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
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "BatchPub without topic params uses the configured default topic.".
t_batch_pub_default_topic(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "duplicate DeviceName entries return 400 DuplicateDeviceName.".
t_batch_pub_duplicate_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"DuplicateDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "missing DeviceName returns 400 InvalidDeviceName.".
t_batch_pub_missing_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "MessageContent and MessageId together return 400.".
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
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub with neither content nor id returns 400.".
t_batch_pub_neither_content_nor_id(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub with an empty DeviceName list returns 400.".
t_batch_pub_empty_device_names(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub rejects DeviceName entries with wildcard or separator characters.".
t_batch_pub_device_name_special_chars(_Config) ->
    lists:foreach(
        fun(DN) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [DN],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"D+1">>, <<"D#1">>, <<"D/1">>, <<"D$1">>]
    ).

-doc "BatchPub rejects a ProductKey with wildcard or separator characters.".
t_batch_pub_product_key_special_chars(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P+1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub rejects an invalid TopicShortName with 400 InvalidTopicTemplate.".
t_batch_pub_invalid_short_name(_Config) ->
    lists:foreach(
        fun(ShortName) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [<<"D1">>],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0,
                <<"TopicShortName">> => ShortName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"a/b">>, <<"a+b">>, <<"a#b">>, <<"a$b">>, <<"a${b}">>, 123]
    ).

-doc "BatchPub rejects a TopicTemplateName with wildcards or unknown placeholders.".
t_batch_pub_invalid_template_name(_Config) ->
    lists:foreach(
        fun(TemplateName) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [<<"D1">>],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0,
                <<"TopicTemplateName">> => TemplateName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"/a/+/b">>, <<"/a/#/b">>, <<"/a/${productKey}/b">>, <<"/a/${unknown}/b">>, 123]
    ).

-doc "BatchPub with a non-binary MessageId returns 400 MessageNotFound.".
t_batch_pub_message_id_wrong_type(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => 123,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "RegisterMessage with a non-binary MessageId returns 400 MessageNotFound.".
t_register_message_id_wrong_type(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => 123
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% PubBroadcast API tests
%%--------------------------------------------------------------------

-doc "PubBroadcast accepts a custom TopicFullName.".
t_broadcast_with_topic_full_name(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"TopicFullName">> => <<"/custom/broadcast/topic">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "PubBroadcast rejects an invalid TopicFullName with 400 InvalidTopicTemplate.".
t_broadcast_invalid_topic_full_name(_Config) ->
    lists:foreach(
        fun(TopicFullName) ->
            Body = #{
                <<"Action">> => <<"PubBroadcast">>,
                <<"ProductKey">> => <<"P1">>,
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"TopicFullName">> => TopicFullName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"/a/+/b">>, <<"/a/#/b">>, <<"/a/${b}">>, 123]
    ).

-doc "PubBroadcast rejects a ProductKey with wildcard or separator characters.".
t_broadcast_product_key_special_chars(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P/1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast without ProductKey returns 400 InvalidProductKey.".
t_broadcast_missing_product_key(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast without content returns 400.".
t_broadcast_missing_content(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast with invalid Base64 returns 400 InvalidBase64.".
t_broadcast_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% Metric verification tests
%%--------------------------------------------------------------------

metric(Name) ->
    try
        prometheus_counter:value(?BCAST_REGISTRY, mname(Name), [])
    catch
        _:_ -> 0
    end.

mname(Suffix) -> <<"bcast_", Suffix/binary>>.

-doc "QoS=0 BatchPub increments the targeted counter by device count.".
t_metrics_qos0_targeted(_Config) ->
    Before = metric(<<"batch_pub_qos0_targeted">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>, <<"D3">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"batch_pub_qos0_targeted">>),
    ?assertEqual(3, After - Before).

-doc "PubBroadcast increments the broadcast_pub_in counter.".
t_metrics_broadcast_in(_Config) ->
    Before = metric(<<"broadcast_pub_in">>),
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"broadcast_pub_in">>),
    ?assertEqual(1, After - Before).

-doc "failed PubBroadcast increments the broadcast error counter.".
t_metrics_broadcast_error(_Config) ->
    Before = metric(<<"broadcast_pub_error">>),
    Body = #{<<"Action">> => <<"PubBroadcast">>, <<"MessageContent">> => <<"!!!">>},
    {ok, 400, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"broadcast_pub_error">>),
    ?assertEqual(1, After - Before).

-doc "QoS=1 BatchPub increments the wanted counter by device count.".
t_metrics_qos1_wanted(_Config) ->
    Before = metric(<<"batch_pub_qos1_wanted">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DA">>, <<"DB">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"batch_pub_qos1_wanted">>),
    ?assertEqual(2, After - Before).

-doc "RegisterMessage increments the register_message_in counter.".
t_metrics_register_message_in(_Config) ->
    Before = metric(<<"register_message_in">>),
    Body = #{<<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => <<"dGVzdA==">>},
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"register_message_in">>),
    ?assertEqual(1, After - Before).

%%--------------------------------------------------------------------
%% Management API tests
%%--------------------------------------------------------------------

-doc "list messages paginates with limit/offset and no payload leak.".
t_mgmt_list_messages_pagination(_Config) ->
    [create_test_msg(<<"mgmt-list-", (integer_to_binary(N))/binary>>) || N <- [1, 2, 3]],
    {ok, 200, _, All} = emqx_bcast_api:handle(get, [<<"messages">>], #{}),
    Total = maps:get(<<"TotalCount">>, All),
    ?assert(Total >= 3),
    {ok, 200, _, Page1} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"2">>, <<"offset">> => <<"0">>}
    }),
    ?assertEqual(Total, maps:get(<<"TotalCount">>, Page1)),
    Items1 = maps:get(<<"Messages">>, Page1),
    ?assertEqual(2, length(Items1)),
    [
        begin
            ?assert(maps:is_key(<<"MessageId">>, Item)),
            ?assert(maps:is_key(<<"CreatedAt">>, Item)),
            ?assert(maps:is_key(<<"ExpiresAt">>, Item)),
            ?assert(maps:is_key(<<"PayloadSize">>, Item)),
            ?assertNot(maps:is_key(<<"Payload">>, Item))
        end
     || Item <- Items1
    ],
    {ok, 200, _, Page2} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"2">>, <<"offset">> => <<"2">>}
    }),
    Items2 = maps:get(<<"Messages">>, Page2),
    ?assert(length(Items2) >= 1),
    Ids1 = [maps:get(<<"MessageId">>, I) || I <- Items1],
    Ids2 = [maps:get(<<"MessageId">>, I) || I <- Items2],
    ?assertEqual([], [I || I <- Ids1, lists:member(I, Ids2)]).

-doc "an offset past the end returns an empty page, not a crash.".
t_mgmt_list_messages_offset_overflow(_Config) ->
    [create_test_msg(<<"mgmt-off-", (integer_to_binary(N))/binary>>) || N <- [1, 2, 3]],
    %% An offset past the last record must return an empty page, not crash.
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"10">>, <<"offset">> => <<"1000000">>}
    }),
    ?assertEqual([], maps:get(<<"Messages">>, Resp)).

-doc "get message returns metadata and delivery count; unknown id 404s.".
t_mgmt_get_message(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-get">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DM1">>], 1
    ),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, Resp)),
    ?assertEqual(1, maps:get(<<"DeliveryCount">>, Resp)),
    ?assertEqual(8, maps:get(<<"PayloadSize">>, Resp)),
    ?assertNot(maps:is_key(<<"Payload">>, Resp)),
    {error, 404, _, NotFound} = emqx_bcast_api:handle(
        get, [<<"messages">>, <<"no-such-id">>], #{}
    ),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, NotFound)).

-doc "deleting a message cascades to its deliveries and index entries.".
t_mgmt_delete_message_cascade(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-del">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DD1">>, <<"DD2">>],
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, DNs, 2),
    {ok, [_]} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD1">>}),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(delete, [<<"messages">>, ApiMsgId], #{}),
    ?assert(maps:get(<<"Success">>, Resp)),
    {error, 404, _, _} = emqx_bcast_api:handle(get, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual({error, not_found}, emqx_bcast_storage:lookup_message(MsgGuid)),
    {error, 404, _, _} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, emqx_bcast_utils:guid_to_uuid(DeliveryId)], #{}
    ),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD1">>}),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD2">>}),
    {error, 404, _, Again} = emqx_bcast_api:handle(delete, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Again)).

-doc "deliveries for a device list UUIDs and metadata; missing params 400.".
t_mgmt_deliveries_for_device(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-dev">>),
    D1 = emqx_bcast_utils:gen_guid(),
    D2 = emqx_bcast_utils:gen_guid(),
    emqx_bcast_storage:create_delivery(D1, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DEV1">>], 1),
    emqx_bcast_storage:create_delivery(D2, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DEV1">>], 1),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"deliveries">>], #{
        query_string => #{<<"product_key">> => <<"PMGMT">>, <<"device_name">> => <<"DEV1">>}
    }),
    Deliveries = maps:get(<<"Deliveries">>, Resp),
    ?assertEqual(2, length(Deliveries)),
    Ids = lists:sort([maps:get(<<"DeliveryId">>, D) || D <- Deliveries]),
    ?assertEqual(
        lists:sort([emqx_bcast_utils:guid_to_uuid(D1), emqx_bcast_utils:guid_to_uuid(D2)]),
        Ids
    ),
    [
        ?assertMatch(
            <<_:8/binary, $-, _:4/binary, $-, _:4/binary, $-, _:4/binary, $-, _:12/binary>>, Id
        )
     || Id <- Ids
    ],
    [
        begin
            ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, D)),
            ?assertEqual(1, maps:get(<<"TargetCount">>, D)),
            ?assertEqual(1, maps:get(<<"PendingCount">>, D)),
            ?assertEqual(<<"PMGMT">>, maps:get(<<"ProductKey">>, D))
        end
     || D <- Deliveries
    ],
    {error, 400, _, BadReq} = emqx_bcast_api:handle(get, [<<"deliveries">>], #{
        query_string => #{<<"product_key">> => <<"PMGMT">>}
    }),
    ?assertEqual(<<"InvalidParams">>, maps:get(<<"Code">>, BadReq)).

-doc "deleting a delivery removes it; unknown or malformed ids 404.".
t_mgmt_delete_delivery(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-ddel">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DE1">>], 1
    ),
    {ok, [_]} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DE1">>}),
    IdStr = emqx_bcast_utils:guid_to_uuid(DeliveryId),
    {ok, 200, _, _} = emqx_bcast_api:handle(delete, [<<"deliveries">>, IdStr], #{}),
    {error, 404, _, NotFound} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, IdStr], #{}
    ),
    ?assertEqual(<<"DeliveryNotFound">>, maps:get(<<"Code">>, NotFound)),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DE1">>}),
    {error, 404, _, _} = emqx_bcast_api:handle(delete, [<<"deliveries">>, IdStr], #{}),
    {error, 404, _, BadId} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, <<"not-a-uuid">>], #{}
    ),
    ?assertEqual(<<"DeliveryNotFound">>, maps:get(<<"Code">>, BadId)).

create_test_msg(Payload) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ApiMsgId, MsgGuid}.
