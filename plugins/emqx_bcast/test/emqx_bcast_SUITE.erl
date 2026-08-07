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
            bcast_msg, bcast_message, bcast_message_hash, bcast_message_api_id
        ]
    ],
    catch emqx_bcast:init_tables(),
    catch ets:delete_all_objects(bcast_msg_index),
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
        force_upgrade_qos => true,
        delivery_pool_size => 2,
        delivery_queue_max => 10000
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
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ?assert(is_binary(ApiMsgId)),
    ?assert(is_binary(MsgGuid)),
    ?assertEqual(16, byte_size(MsgGuid)),
    ?assert(ApiMsgId =/= MsgGuid).

t_resolve_message_id_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_bcast_id:resolve_message_id(<<"nonexistent">>)).

t_resolve_message_id_found(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"test payload">>),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"test payload">>),
    ?assertEqual({ok, MsgGuid}, emqx_bcast_id:resolve_message_id(ApiMsgId)).

%%--------------------------------------------------------------------
%% Storage / Mnesia tests
%%--------------------------------------------------------------------

t_create_and_lookup_message(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"hello world">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assertEqual(Payload, Msg#bcast_message.payload),
    ?assertEqual(Hash, Msg#bcast_message.content_hash),
    ?assertEqual(ApiMsgId, Msg#bcast_message.api_msg_id).

t_lookup_by_hash(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"dedup test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message_by_hash(Hash),
    ?assertEqual(MsgGuid, Msg#bcast_message.msg_id).

t_refresh_message_ttl(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ttl test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg1} = emqx_bcast_storage:lookup_message(MsgGuid),
    timer:sleep(1100),
    emqx_bcast_storage:refresh_message_ttl(MsgGuid),
    {ok, Msg2} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assert(Msg2#bcast_message.expires_at > Msg1#bcast_message.expires_at).

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

t_topic_expansion(_Config) ->
    Result = emqx_bcast_utils:expand_topic(
        <<"/${productKey}/${deviceName}/user/get">>,
        <<"P1">>,
        <<"D1">>
    ),
    ?assertEqual(<<"/P1/D1/user/get">>, Result).

t_sha256(_Config) ->
    Hash = emqx_bcast_utils:sha256(<<"test">>),
    ?assertEqual(32, byte_size(Hash)).

t_base64_decode(_Config) ->
    ?assertEqual({ok, <<"hello">>}, emqx_bcast_utils:decode_base64(<<"aGVsbG8=">>)),
    ?assertEqual({error, invalid_base64}, emqx_bcast_utils:decode_base64(<<"!!!">>)).

%%--------------------------------------------------------------------
%% Topic matching tests
%%--------------------------------------------------------------------

t_topic_match_exact(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/D1/user/get">>)).

t_topic_match_plus(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/+/user/get">>)).

t_topic_match_hash(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/#">>)).

t_topic_match_no_match(_Config) ->
    ?assertNot(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P2/+/user/get">>)).

%%--------------------------------------------------------------------
%% Subscription QoS match tests
%%--------------------------------------------------------------------

t_sub_match_returns_qos(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D1/user/get">>, 0}),
    ?assertEqual({ok, 0}, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

t_sub_match_max_qos_overlapping(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/+/user/get">>, 0}),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D1/user/get">>, 1}),
    ?assertEqual({ok, 1}, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

t_sub_match_no_match(_Config) ->
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"dev1">>, self(), {<<"/P1/D2/user/get">>, 1}),
    ?assertEqual(false, emqx_bcast_subscription:match(<<"dev1">>, <<"/P1/D1/user/get">>)).

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

t_delivery_completed_hook(_Config) ->
    PK = <<"PC">>,
    DN = <<"DC1">>,
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"delivery completed test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Before = metric(<<"batch_pub_qos1_acked">>),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        0,
        <<"/PC/DC1/user/get">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    ok = emqx_bcast:on_delivery_completed(Msg, #{clientid => DN}),
    ?assertEqual(1, metric(<<"batch_pub_qos1_acked">>) - Before),
    %% duplicate completion does not double count
    ok = emqx_bcast:on_delivery_completed(Msg, #{clientid => DN}),
    ?assertEqual(1, metric(<<"batch_pub_qos1_acked">>) - Before),
    %% delivery record removed after all acks
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% messages without plugin headers pass through untouched
    Plain = emqx_message:make(DN, 0, <<"/t">>, <<"p">>),
    ok = emqx_bcast:on_delivery_completed(Plain, #{clientid => DN}),
    ?assertEqual(1, metric(<<"batch_pub_qos1_acked">>) - Before).

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
%% Force upgrade QoS tests
%%--------------------------------------------------------------------

t_force_upgrade_false_qos0_sub(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{force_upgrade_qos => false}),
    emqx_bcast:register_device(<<"P1">>, <<"DA">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DA">>, self(), {<<"/P1/DA/user/get">>, 0}),
    BeforeAcked = metric(<<"batch_pub_qos1_acked">>),
    BeforeInline = metric(<<"batch_pub_qos1_delivered_inline">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DA">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, BeforeAcked + 1)),
    ?assertEqual(0, metric(<<"batch_pub_qos1_delivered_inline">>) - BeforeInline),
    flush_mailbox(),
    persistent_term:put({?APP, config}, Cfg).

t_force_upgrade_false_qos1_sub(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{force_upgrade_qos => false}),
    emqx_bcast:register_device(<<"P1">>, <<"DB">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DB">>, self(), {<<"/P1/DB/user/get">>, 1}),
    BeforeInline = metric(<<"batch_pub_qos1_delivered_inline">>),
    BeforeAcked = metric(<<"batch_pub_qos1_acked">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DB">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(wait_metric(<<"batch_pub_qos1_delivered_inline">>, BeforeInline + 1)),
    timer:sleep(200),
    ?assertEqual(0, metric(<<"batch_pub_qos1_acked">>) - BeforeAcked),
    flush_mailbox(),
    persistent_term:put({?APP, config}, Cfg).

t_force_upgrade_true_qos0_sub(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{force_upgrade_qos => true}),
    emqx_bcast:register_device(<<"P1">>, <<"DC">>, self()),
    emqx_bcast_subscription:init(),
    emqx_bcast_subscription:add(<<"DC">>, self(), {<<"/P1/DC/user/get">>, 0}),
    BeforeInline = metric(<<"batch_pub_qos1_delivered_inline">>),
    BeforeAcked = metric(<<"batch_pub_qos1_acked">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DC">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(wait_metric(<<"batch_pub_qos1_delivered_inline">>, BeforeInline + 1)),
    timer:sleep(200),
    ?assertEqual(0, metric(<<"batch_pub_qos1_acked">>) - BeforeAcked),
    flush_mailbox(),
    persistent_term:put({?APP, config}, Cfg).

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

t_delivery_queue_overflow(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{delivery_queue_max => 0}),
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
    {ok, 429, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assertEqual(<<"DeliveryQueueFull">>, maps:get(<<"Code">>, Resp)),
    %% 429 leaves no QoS=1 delivery record behind
    ?assertEqual([], mnesia:dirty_select(bcast_msg, [{'_', [], ['$_']}])),
    %% direct submit over the limit is rejected and counted
    Before = metric(<<"delivery_submit_rejected">>),
    {error, overloaded} = emqx_bcast_deliver:submit_task(fun() -> ok end),
    ?assertEqual(1, metric(<<"delivery_submit_rejected">>) - Before),
    flush_mailbox(),
    persistent_term:put({?APP, config}, Cfg).

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

t_resolve_local_and_chunked_submit(_Config) ->
    PK = <<"PR">>,
    N = 205,
    DNs = [<<"R", (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)],
    lists:foreach(
        fun(DN) ->
            emqx_bcast:register_device(PK, DN, self()),
            emqx_bcast_subscription:add(DN, self(), {<<"/PR/#">>, 0})
        end,
        DNs
    ),
    Template = <<"/PR/${deviceName}/user/get">>,
    Targets = emqx_bcast_batch_pub:resolve_local(DNs, PK, Template),
    ?assertEqual(N, length(Targets)),
    ?assertMatch(
        [{_, _, <<"/PR/", _/binary>>, 0} | _],
        lists:sort(Targets)
    ),
    %% 205 targets -> 2 chunks; all delivered asynchronously to self()
    ok = emqx_bcast_deliver:submit_targets(Targets, #{qos => 0, payload => <<"p">>}),
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

t_pool_queue_depth_metric(_Config) ->
    ok = emqx_bcast_deliver:submit_task(fun() -> ok end),
    ?assert(wait_until(fun() -> emqx_bcast_deliver:queue_depth() =:= 0 end, 100)),
    ?assertEqual(
        0,
        prometheus_gauge:value(?BCAST_REGISTRY, <<"bcast_delivery_queue_depth">>, [])
    ).

t_api_missing_action(_Config) ->
    Body = #{<<"ProductKey">> => <<"P1">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(false, maps:get(<<"Success">>, Resp)),
    ?assertEqual(<<"MissingAction">>, maps:get(<<"Code">>, Resp)).

t_api_unknown_action(_Config) ->
    Body = #{<<"Action">> => <<"BadAction">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"UnknownAction">>, maps:get(<<"Code">>, Resp)).

t_api_not_found(_Config) ->
    {error, not_found} = emqx_bcast_api:handle(get, [<<"pub">>], #{}).

%%--------------------------------------------------------------------
%% RegisterMessage API tests
%%--------------------------------------------------------------------

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

t_register_message_dedup(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, _, _, Resp1} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    {ok, _, _, Resp2} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(maps:get(<<"MessageId">>, Resp1), maps:get(<<"MessageId">>, Resp2)).

t_register_message_refresh_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => <<"nonexistent-uuid">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

t_register_message_mutual_exclusion(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

t_register_message_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

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

t_register_message_empty(_Config) ->
    Body = #{<<"Action">> => <<"RegisterMessage">>},
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
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
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
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
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

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
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

t_broadcast_missing_product_key(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

t_broadcast_missing_content(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

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

t_metrics_broadcast_error(_Config) ->
    Before = metric(<<"broadcast_pub_error">>),
    Body = #{<<"Action">> => <<"PubBroadcast">>, <<"MessageContent">> => <<"!!!">>},
    {ok, 400, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"broadcast_pub_error">>),
    ?assertEqual(1, After - Before).

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

t_metrics_register_message_in(_Config) ->
    Before = metric(<<"register_message_in">>),
    Body = #{<<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => <<"dGVzdA==">>},
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"register_message_in">>),
    ?assertEqual(1, After - Before).

%%--------------------------------------------------------------------
%% Management API tests
%%--------------------------------------------------------------------

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

t_mgmt_list_messages_offset_overflow(_Config) ->
    [create_test_msg(<<"mgmt-off-", (integer_to_binary(N))/binary>>) || N <- [1, 2, 3]],
    %% An offset past the last record must return an empty page, not crash.
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"10">>, <<"offset">> => <<"1000000">>}
    }),
    ?assertEqual([], maps:get(<<"Messages">>, Resp)).

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
