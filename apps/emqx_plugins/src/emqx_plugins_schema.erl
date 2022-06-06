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

-module(emqx_plugins_schema).

-behaviour(hocon_schema).

-export([
    roots/0,
    fields/1,
    namespace/0
]).

-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_plugins.hrl").

namespace() -> "plugin".

roots() -> [?CONF_ROOT].

fields(?CONF_ROOT) ->
    #{
        fields => root_fields(),
        desc => ?DESC(?CONF_ROOT)
    };
fields(state) ->
    #{
        fields => state_fields(),
        desc => ?DESC(state)
    }.

state_fields() ->
    [
        {name_vsn,
            ?HOCON(
                string(),
                #{
                    desc => ?DESC(name_vsn),
                    required => true
                }
            )},
        {enable,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(enable),
                    required => true
                }
            )}
    ].

root_fields() ->
    [
        {states, fun states/1},
        {install_dir, fun install_dir/1},
        {check_interval, fun check_interval/1}
    ].

states(type) -> ?ARRAY(?R_REF(state));
states(required) -> false;
states(default) -> [];
states(desc) -> ?DESC(states);
states(_) -> undefined.

install_dir(type) -> string();
install_dir(required) -> false;
%% runner's root dir
install_dir(default) -> "plugins";
install_dir(T) when T =/= desc -> undefined;
install_dir(desc) -> ?DESC(install_dir).

check_interval(type) -> emqx_schema:duration();
check_interval(default) -> "5s";
check_interval(T) when T =/= desc -> undefined;
check_interval(desc) -> ?DESC(check_interval).
