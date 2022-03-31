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
-module(emqx_config_logger).

-behaviour(emqx_config_handler).

%% API
-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).

-define(LOG, [log]).

add_handler() ->
    ok = emqx_config_handler:add_handler(?LOG, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?LOG),
    ok.

post_config_update(?LOG, _Req, _NewConf, _OldConf, AppEnvs) ->
    Kernel = proplists:get_value(kernel, AppEnvs),
    NewHandlers = proplists:get_value(logger, Kernel, []),
    Level = proplists:get_value(logger_level, Kernel, warning),
    ok = update_log_handlers(NewHandlers),
    ok = emqx_logger:set_primary_log_level(Level),
    application:set_env(kernel, logger_level, Level),
    ok;
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

update_log_handlers(NewHandlers) ->
    OldHandlers = application:get_env(kernel, logger, []),
    lists:foreach(
        fun({handler, HandlerId, _Mod, _Conf}) ->
            logger:remove_handler(HandlerId)
        end,
        OldHandlers -- NewHandlers
    ),
    lists:foreach(
        fun({handler, HandlerId, Mod, Conf}) ->
            logger:add_handler(HandlerId, Mod, Conf)
        end,
        NewHandlers -- OldHandlers
    ),
    application:set_env(kernel, logger, NewHandlers).
