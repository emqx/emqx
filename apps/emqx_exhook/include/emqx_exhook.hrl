%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_EXHOOK_HRL).
-define(EMQX_EXHOOK_HRL, true).

-define(APP, emqx_exhook).
-define(HOOKS_REF_COUNTER, emqx_exhook_ref_counter).
-define(HOOKS_METRICS, emqx_exhook_metrics).

-define(ENABLED_HOOKS, [
    {'client.connect', {emqx_exhook_handler, on_client_connect, []}},
    {'client.connack', {emqx_exhook_handler, on_client_connack, []}},
    {'client.connected', {emqx_exhook_handler, on_client_connected, []}},
    {'client.disconnected', {emqx_exhook_handler, on_client_disconnected, []}},
    {'client.authenticate', {emqx_exhook_handler, on_client_authenticate, []}},
    {'client.authorize', {emqx_exhook_handler, on_client_authorize, []}},
    {'client.subscribe', {emqx_exhook_handler, on_client_subscribe, []}},
    {'client.unsubscribe', {emqx_exhook_handler, on_client_unsubscribe, []}},
    {'session.created', {emqx_exhook_handler, on_session_created, []}},
    {'session.subscribed', {emqx_exhook_handler, on_session_subscribed, []}},
    {'session.unsubscribed', {emqx_exhook_handler, on_session_unsubscribed, []}},
    {'session.resumed', {emqx_exhook_handler, on_session_resumed, []}},
    {'session.discarded', {emqx_exhook_handler, on_session_discarded, []}},
    {'session.takenover', {emqx_exhook_handler, on_session_takenover, []}},
    {'session.terminated', {emqx_exhook_handler, on_session_terminated, []}},
    {'message.publish', {emqx_exhook_handler, on_message_publish, []}},
    {'message.delivered', {emqx_exhook_handler, on_message_delivered, []}},
    {'message.acked', {emqx_exhook_handler, on_message_acked, []}},
    {'message.dropped', {emqx_exhook_handler, on_message_dropped, []}}
]).

-endif.

-define(CMD_MOVE_FRONT, front).
-define(CMD_MOVE_REAR, rear).
-define(CMD_MOVE_BEFORE(Before), {before, Before}).
-define(CMD_MOVE_AFTER(After), {'after', After}).
