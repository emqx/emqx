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

-module(emqx_boot_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_is_enabled(_) ->
    ok = application:set_env(emqx, boot_modules, all),
    ?assert(emqx_boot:is_enabled(router)),
    ?assert(emqx_boot:is_enabled(broker)),
    ?assert(emqx_boot:is_enabled(listeners)),
    ok = application:set_env(emqx, boot_modules, [router]),
    ?assert(emqx_boot:is_enabled(router)),
    ?assertNot(emqx_boot:is_enabled(broker)),
    ?assertNot(emqx_boot:is_enabled(listeners)),
    ok = application:set_env(emqx, boot_modules, [router, broker]),
    ?assert(emqx_boot:is_enabled(router)),
    ?assert(emqx_boot:is_enabled(broker)),
    ?assertNot(emqx_boot:is_enabled(listeners)),
    ok = application:set_env(emqx, boot_modules, [router, broker, listeners]),
    ?assert(emqx_boot:is_enabled(router)),
    ?assert(emqx_boot:is_enabled(broker)),
    ?assert(emqx_boot:is_enabled(listeners)).

