%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_events).

-include("emqx.hrl").
-include("logger.hrl").

-export([handle_event/2]).

-type message() :: emqx_types:message().

-type event_expired() :: {expired, message()}.
-type event_dropped() :: {dropped, message(), _Reason :: atom() | #{reason := atom(), _ => _}}.
-type event_expire_rel() :: {expired_rel, non_neg_integer()}.

-type event() ::
    event_expired()
    | event_dropped()
    | event_expire_rel().

%%

-spec handle_event(emqx_session:clientinfo(), event()) ->
    ok.
handle_event(ClientInfo, {expired, Msg}) ->
    ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, expired]),
    ok = inc_delivery_expired_cnt(1);
handle_event(ClientInfo, {dropped, Msg, no_local}) ->
    ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, no_local]),
    ok = emqx_metrics:inc('delivery.dropped'),
    ok = emqx_metrics:inc('delivery.dropped.no_local');
handle_event(ClientInfo, {dropped, Msg, #{reason := qos0_msg, logctx := Ctx}}) ->
    ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, qos0_msg]),
    ok = emqx_metrics:inc('delivery.dropped'),
    ok = emqx_metrics:inc('delivery.dropped.qos0_msg'),
    ok = inc_pd('send_msg.dropped', 1),
    ?SLOG(
        warning,
        Ctx#{
            msg => "dropped_qos0_msg",
            payload => Msg#message.payload
        },
        #{topic => Msg#message.topic}
    );
handle_event(ClientInfo, {dropped, Msg, #{reason := queue_full, logctx := Ctx}}) ->
    ok = emqx_hooks:run('delivery.dropped', [ClientInfo, Msg, queue_full]),
    ok = emqx_metrics:inc('delivery.dropped'),
    ok = emqx_metrics:inc('delivery.dropped.queue_full'),
    ok = inc_pd('send_msg.dropped', 1),
    ok = inc_pd('send_msg.dropped.queue_full', 1),
    ?SLOG_THROTTLE(
        warning,
        Ctx#{
            msg => dropped_msg_due_to_mqueue_is_full,
            payload => Msg#message.payload
        },
        #{topic => Msg#message.topic}
    );
handle_event(_ClientInfo, {expired_rel, 0}) ->
    ok;
handle_event(_ClientInfo, {expired_rel, ExpiredCnt}) ->
    inc_await_pubrel_timeout(ExpiredCnt).

inc_delivery_expired_cnt(N) ->
    ok = inc_pd('send_msg.dropped', N),
    ok = inc_pd('send_msg.dropped.expired', N),
    ok = emqx_metrics:inc('delivery.dropped', N),
    emqx_metrics:inc('delivery.dropped.expired', N).

inc_await_pubrel_timeout(N) ->
    ok = inc_pd('recv_msg.dropped', N),
    ok = inc_pd('recv_msg.dropped.await_pubrel_timeout', N),
    ok = emqx_metrics:inc('messages.dropped', N),
    emqx_metrics:inc('messages.dropped.await_pubrel_timeout', N).

inc_pd(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.
