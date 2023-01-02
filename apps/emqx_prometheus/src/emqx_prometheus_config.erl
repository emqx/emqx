%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_prometheus_config).

-behaviour(emqx_config_handler).

-include("emqx_prometheus.hrl").

-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).
-export([update/1]).

update(Config) ->
    case
        emqx_conf:update(
            [prometheus],
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

add_handler() ->
    ok = emqx_config_handler:add_handler(?PROMETHEUS, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?PROMETHEUS),
    ok.

post_config_update(?PROMETHEUS, _Req, New, _Old, AppEnvs) ->
    application:set_env(AppEnvs),
    update_prometheus(New);
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

update_prometheus(#{enable := true}) ->
    emqx_prometheus_sup:start_child(?APP);
update_prometheus(#{enable := false}) ->
    emqx_prometheus_sup:stop_child(?APP).
