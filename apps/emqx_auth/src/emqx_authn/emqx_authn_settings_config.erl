%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Authentication settings configuration management module.
-module(emqx_authn_settings_config).

-behaviour(emqx_config_handler).

-export([
    post_config_update/5
]).

-spec post_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:raw_config(),
    emqx_config:app_envs()
) ->
    ok | {ok, map()} | {error, term()}.
post_config_update(
    _ConfPath,
    _UpdateReq,
    #{total_latency_metric_buckets := Buckets} = _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    ok = emqx_access_control:update_latency_buckets('client.authenticate', Buckets).
