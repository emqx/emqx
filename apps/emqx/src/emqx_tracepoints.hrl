%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This file lists snabbkaffe trace point kinds
-ifndef(EMQX_TRACEPOINTS_HRL).
-define(EMQX_TRACEPOINTS_HRL, true).

%%%%% Trace point kinds:
-define(sessds_drain_inflight, sessds_drain_inflight).

-define(sessds_unknown_timeout, sessds_unknown_timeout).
-define(sessds_unknown_message, sessds_unknown_message).
-define(sessds_replay_inconsistency, sessds_replay_inconsistency).
-define(sessds_out_of_order_commit, 'sessds_out-of-order_commit').
-define(sessds_unexpected_reply, sessds_unexpected_ds_reply).
-define(sessds_replay_unrecoverable_error, sessds_replay_unrecoverable_error).

-define(sessds_terminate, sessds_terminate).
-define(sessds_drop, sessds_drop).
-define(sessds_open_session, sessds_open_session).
-define(sessds_ensure_new, sessds_ensure_new).
-define(sessds_commit, sessds_commit).
-define(sessds_commit_failure, sessds_commit_failure).

-define(sessds_update_srs_ssid, sessds_update_srs_ssid).
-define(sessds_do_enqueue, sessds_do_enqueue).
-define(sessds_poll_reply, sessds_poll_reply).

-define(sessds_stream_state_trans, sessds_stream_state_trans).
-define(sessds_unblock_stream, sessds_unblock_stream).
-define(sessds_unexpected_stream_notification, sessds_unexpected_stream_notification).
-define(sessds_sched_new_stream_event, sessds_sched_new_stream_event).
-define(sessds_retry_renew_streams, sessds_retry_renew_streams).
-define(sessds_sched_watch_streams, sessds_sched_watch_streams).
-define(sessds_sched_unwatch_streams, sessds_sched_unwatch_streams).
-define(sessds_sched_renew_streams, sessds_sched_renew_streams).
-define(sessds_sched_renew_streams_result, sessds_sched_renew_streams_result).
-define(sessds_advance_generation, sessds_advance_generation).

-define(sessds_put_seqno, sessds_put_seqno).

-define(sessds_sched_subscribe, sessds_sched_subscribe).
-define(sessds_sched_subscribe_fail, sessds_sched_subscribe_fail).
-define(sessds_sched_unsubscribe, sessds_sched_unsubscribe).
-define(sessds_unexpected_ds_batch, sessds_unexpected_ds_batch).
-define(sessds_sub_down, sessds_sub_down).

-define(sessds_takeover_conflict, sessds_takeover_conflict).

-endif.
