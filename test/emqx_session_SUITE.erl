%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_session_init(_) ->
    error('TODO').

%%--------------------------------------------------------------------
%% Test cases for info/stats
%%--------------------------------------------------------------------

t_session_info(_) ->
    error('TODO').

t_session_attrs(_) ->
    error('TODO').

t_session_stats(_) ->
    error('TODO').

%%--------------------------------------------------------------------
%% Test cases for pub/sub
%%--------------------------------------------------------------------

t_subscribe(_) ->
    error('TODO').

t_unsubscribe(_) ->
    error('TODO').

t_publish_qos0(_) ->
    error('TODO').

t_publish_qos1(_) ->
    error('TODO').

t_publish_qos2(_) ->
    error('TODO').

t_puback(_) ->
    error('TODO').

t_pubrec(_) ->
    error('TODO').

t_pubrel(_) ->
    error('TODO').

t_pubcomp(_) ->
    error('TODO').

%%--------------------------------------------------------------------
%% Test cases for deliver/retry
%%--------------------------------------------------------------------

t_deliver(_) ->
    error('TODO').

t_enqueue(_) ->
    error('TODO').

t_retry(_) ->
    error('TODO').

%%--------------------------------------------------------------------
%% Test cases for takeover/resume
%%--------------------------------------------------------------------

t_takeover(_) ->
    error('TODO').

t_resume(_) ->
    error('TODO').

t_redeliver(_) ->
    error('TODO').

t_expire(_) ->
    error('TODO').

