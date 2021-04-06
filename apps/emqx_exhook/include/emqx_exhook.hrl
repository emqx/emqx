%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(ENABLED_HOOKS,
      [ {'client.connect',      {?MODULE, on_client_connect,       []}}
      , {'client.connack',      {?MODULE, on_client_connack,       []}}
      , {'client.connected',    {?MODULE, on_client_connected,     []}}
      , {'client.disconnected', {?MODULE, on_client_disconnected,  []}}
      , {'client.authenticate', {?MODULE, on_client_authenticate,  []}}
      , {'client.check_acl',    {?MODULE, on_client_check_acl,     []}}
      , {'client.subscribe',    {?MODULE, on_client_subscribe,     []}}
      , {'client.unsubscribe',  {?MODULE, on_client_unsubscribe,   []}}
      , {'session.created',     {?MODULE, on_session_created,      []}}
      , {'session.subscribed',  {?MODULE, on_session_subscribed,   []}}
      , {'session.unsubscribed',{?MODULE, on_session_unsubscribed, []}}
      , {'session.resumed',     {?MODULE, on_session_resumed,      []}}
      , {'session.discarded',   {?MODULE, on_session_discarded,    []}}
      , {'session.takeovered',  {?MODULE, on_session_takeovered,   []}}
      , {'session.terminated',  {?MODULE, on_session_terminated,   []}}
      , {'message.publish',     {?MODULE, on_message_publish,      []}}
      , {'message.delivered',   {?MODULE, on_message_delivered,    []}}
      , {'message.acked',       {?MODULE, on_message_acked,        []}}
      , {'message.dropped',     {?MODULE, on_message_dropped,      []}}
      ]).

-endif.
