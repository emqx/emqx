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
-module(emqx_statsd_config).

-behaviour(emqx_config_handler).

-include("emqx_statsd.hrl").

-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).
-export([update/1]).

update(Config) ->
    case
        emqx_conf:update(
            ?STATSD,
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
    ok = emqx_config_handler:add_handler(?STATSD, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?STATSD),
    ok.

post_config_update(?STATSD, _Req, #{enable := true}, _Old, _AppEnvs) ->
    emqx_statsd_sup:ensure_child_stopped(?APP),
    emqx_statsd_sup:ensure_child_started(?APP);
post_config_update(?STATSD, _Req, #{enable := false}, _Old, _AppEnvs) ->
    emqx_statsd_sup:ensure_child_stopped(?APP);
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.
