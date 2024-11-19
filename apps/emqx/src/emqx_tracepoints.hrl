%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc This file lists snabbkaffe trace point kinds
-ifndef(EMQX_TRACEPOINTS_HRL).
-define(EMQX_TRACEPOINTS_HRL, true).

%%%%% Trace point kinds:
-define(sessds_pull, sessds_pull).
-define(sessds_push, sessds_push).
-define(sessds_unknown_timeout, sessds_unknown_timeout).
-define(sessds_unknown_message, sessds_unknown_message).
-define(sessds_replay_inconsistency, sessds_replay_inconsistency).
-define(sessds_terminate, sessds_terminate).
-define(sessds_drop, sessds_drop).
-define(sessds_open_session, sessds_open_session).
-define(sessds_ensure_new, sessds_ensure_new).
-define(sessds_out_of_order_commit, 'sessds_out-of-order_commit').
-define(sessds_update_srs_ssid, sessds_update_srs_ssid).
-define(sessds_commit, sessds_commit).
-define(sessds_do_enqueue, sessds_do_enqueue).
-define(sessds_poll_reply, sessds_poll_reply).

-define(sessds_stream_state_trans, sessds_stream_state_trans).
-define(sessds_sched_new_stream_event, sessds_sched_new_stream_event).
-define(sessds_unexpected_stream_notification, sessds_unexpected_stream_notification).
-define(sessds_sched_renew_streams, sessds_sched_renew_streams).
-define(sessds_sched_renew_streams_result, sessds_sched_renew_streams_result).
-define(sessds_sched_watch_streams, sessds_sched_watch_streams).
-define(sessds_sched_unwatch_streams, sessds_sched_unwatch_streams).

-endif.
