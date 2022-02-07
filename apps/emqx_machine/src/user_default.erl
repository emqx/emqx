%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(user_default).

%% INCLUDE BEGIN
%% Import all the record definitions from the header file into the erlang shell.
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_conf/include/emqx_conf.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
%% INCLUDE END

%% API
-export([lock/0, unlock/0]).

lock() -> emqx_restricted_shell:lock().
unlock() -> emqx_restricted_shell:unlock().
