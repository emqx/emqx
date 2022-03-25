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
-module(emqx_dashboard_config).

-behaviour(emqx_config_handler).

%% API
-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).

add_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:add_handler(Roots, ?MODULE),
    ok.

remove_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:remove_handler(Roots),
    ok.

post_config_update(_, _Req, NewConf, OldConf, _AppEnvs) ->
    #{listeners := NewListeners} = NewConf,
    #{listeners := OldListeners} = OldConf,
    case NewListeners =:= OldListeners of
        true -> ok;
        false ->
            ok = emqx_dashboard:stop_listeners(OldListeners),
            ok = emqx_dashboard:start_listeners(NewListeners)
    end,
    ok.
