%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_metrics).

-export([
    init/0,
    inc_batch_pub_qos0_in/0,
    inc_batch_pub_qos0_error/0,
    inc_qos0_targeted/1,
    inc_qos0_delivered/0,
    inc_qos0_skipped/0,
    inc_batch_pub_qos1_in/0,
    inc_batch_pub_qos1_error/0,
    inc_batch_pub_qos1_incomplete/0,
    inc_qos1_delivered_inline/0,
    inc_qos1_stored_offline/0,
    inc_broadcast_pub_in/0,
    inc_broadcast_pub_error/0,
    inc_broadcast_devices_online/1,
    inc_broadcast_delivery_count/0,
    inc_register_message_in/0,
    inc_register_message_refresh/0,
    inc_register_message_error/0,
    inc_msg_acked/0,
    inc_msg_replayed/0,
    inc_msg_error/0,
    inc_msg_incomplete/0,
    inc_msg_succeed/0,
    inc_msg_wanted/1,
    set_msg_pending/1,
    prometheus_export/0
]).

-define(TAB, iot_mq_counters).

init() ->
    catch ets:new(?TAB, [named_table, public, set, {write_concurrency, true}]),
    ok.

inc(Name) -> inc(Name, 1).
inc(Name, N) ->
    ensure_table(),
    ets:update_counter(?TAB, Name, {2, N}, {Name, 0, 0}).

set(Name, Val) ->
    ensure_table(),
    ets:insert(?TAB, {Name, Val, 0}).

ensure_table() ->
    case ets:info(?TAB) of
        undefined -> ets:new(?TAB, [named_table, public, set, {write_concurrency, true}]);
        _ -> ok
    end.

prometheus_export() ->
    Lines = [
        [
            io_lib:format("# TYPE iot_mq_~s counter\n", [safe(Name)]),
            io_lib:format("iot_mq_~s ~w\n", [safe(Name), get_val(Name)])
        ]
     || {Name, _Val, _} <- ets:tab2list(?TAB), Name =/= total
    ],
    iolist_to_binary(lists:append(Lines)).

get_val(Name) ->
    try
        ets:lookup_element(?TAB, Name, 2)
    catch
        _:_ -> 0
    end.

safe(Name) when is_atom(Name) ->
    binary:replace(atom_to_binary(Name, utf8), <<".">>, <<"_">>, [global]);
safe(Name) ->
    Name.

%% ── API ──

inc_batch_pub_qos0_in() -> inc('batch_pub_qos0_in').
inc_batch_pub_qos0_error() -> inc('batch_pub_qos0_error').
inc_qos0_targeted(N) -> inc('batch_pub_qos0_targeted', N).
inc_qos0_delivered() -> inc('batch_pub_qos0_delivered').
inc_qos0_skipped() -> inc('batch_pub_qos0_skipped').
inc_batch_pub_qos1_in() -> inc('batch_pub_qos1_in').
inc_batch_pub_qos1_error() -> inc('batch_pub_qos1_error').
inc_batch_pub_qos1_incomplete() -> inc('batch_pub_qos1_incomplete').
inc_qos1_delivered_inline() -> inc('batch_pub_qos1_delivered_inline').
inc_qos1_stored_offline() -> inc('batch_pub_qos1_stored_offline').
inc_broadcast_pub_in() -> inc('broadcast_pub_in').
inc_broadcast_pub_error() -> inc('broadcast_pub_error').
inc_broadcast_devices_online(N) -> inc('broadcast_pub_devices_online', N).
inc_broadcast_delivery_count() -> inc('broadcast_pub_delivery_count').
inc_register_message_in() -> inc('register_message_in').
inc_register_message_refresh() -> inc('register_message.refresh').
inc_register_message_error() -> inc('register_message.error').
inc_msg_acked() -> inc('batch_pub_qos1_msg_acked').
inc_msg_replayed() -> inc('batch_pub_qos1_msg_replayed').
inc_msg_error() -> inc('batch_pub_qos1_msg_error').
inc_msg_incomplete() -> inc('batch_pub_qos1_msg_incomplete').
inc_msg_succeed() -> inc('batch_pub_qos1_msg_succeed').
inc_msg_wanted(N) -> inc('batch_pub_qos1_msg_wanted', N).
set_msg_pending(N) -> set('batch_pub_qos1_msg_pending', N).
