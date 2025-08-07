%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DURABLE_TIMER_HRL).
-define(EMQX_DURABLE_TIMER_HRL, true).

-include_lib("snabbkaffe/include/trace.hrl").

-define(DB_GLOB, timers).
-define(regs_tab, emqx_durable_timer_registry).

%% Topics:
-define(top_heartbeat, <<"h">>).
-define(top_started, <<"s">>).
-define(top_deadhand, <<"d">>).
%%   Shard-local epochs:
-define(top_epoch, <<"e">>).
%%   Heartbeats:
%%      Topic:
-define(hb_topic(NODE, EPOCH), [?top_heartbeat, NODE, EPOCH]).
%%      Value:
-define(hb(NODE, EPOCH, TIME, ISUP), {?hb_topic(NODE, EPOCH), 0, <<TIME:64, ISUP:8>>}).

-define(type_bits, 32).
-define(max_type, (1 bsl ?type_bits - 1)).

%% Tracepoints:
-define(tp_init, emqx_durable_timer_init).
-define(tp_register, emqx_durable_timer_register).
-define(tp_update_epoch, emqx_durable_timer_update_epoch).
-define(tp_heartbeat, emqx_durable_timer_heartbeat).
-define(tp_missed_heartbeat, emqx_durable_timer_missed_heartbeat).
-define(tp_remote_missed_heartbeat, emqx_durable_timer_remote_missed_heartbeat).
-define(tp_new_apply_after, emqx_durable_timer_apply_after).
-define(tp_new_dead_hand, emqx_durable_timer_dead_hand).
-define(tp_delete, emqx_durable_timer_delete).
-define(tp_fire, emqx_durable_timer_fire).
-define(tp_unknown_event, emqx_durable_timer_unknown_event).
-define(tp_app_activation, emqx_durable_timer_app_activated).
-define(tp_state_change, emqx_durable_timer_state_change).
-define(tp_replay, emqx_durable_timer_replay).
-define(tp_replay_failed, emqx_durable_timer_replay_failed).
-define(tp_worker_started, emqx_durable_timer_worker_started).
-define(tp_terminate, emqx_durable_timer_process_terminate).

-define(tp_apply_after_write_begin, emqx_durable_timer_apply_write_begin).
-define(tp_apply_after_write_ok, emqx_durable_timer_apply_write_ok).
-define(tp_apply_after_write_fail, emqx_durable_timer_apply_write_fail).

-define(tp_test_fire, emqx_durable_timer_test_fire).

-define(is_valid_timer(TYPE, KEY, VALUE, TIME),
    (TYPE >= 0 andalso TYPE =< ?max_type andalso is_binary(KEY) andalso
        is_binary(VALUE) andalso
        is_integer(TIME))
).

-define(workers_pg, emqx_durable_timer_worker_pg).

-endif.
