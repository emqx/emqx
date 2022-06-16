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

-module(emqx_gateway_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(CONF_DEFAULT, <<
    ""
    "\n"
    "gateway: {\n"
    "    stomp {}\n"
    "}\n"
    ""
>>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Cfg) ->
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([emqx_authn, emqx_gateway]),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_common_test_helpers:stop_apps([emqx_gateway, emqx_authn]),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_unload(_) ->
    OldCnt = length(emqx_gateway_registry:list()),
    RgOpts = [{cbkmod, ?MODULE}],
    ok = emqx_gateway_registry:reg(test, RgOpts),
    ?assertEqual(OldCnt + 1, length(emqx_gateway_registry:list())),

    #{
        cbkmod := ?MODULE,
        rgopts := RgOpts
    } = emqx_gateway_registry:lookup(test),

    {error, already_existed} = emqx_gateway_registry:reg(test, [{cbkmod, ?MODULE}]),

    ok = emqx_gateway_registry:unreg(test),
    ok = emqx_gateway_registry:unreg(test),
    undefined = emqx_gateway_registry:lookup(test),
    OldCnt = length(emqx_gateway_registry:list()),
    ok.

t_handle_unexpected_msg(_) ->
    _ = emqx_gateway_registry ! unexpected_info,
    ok = gen_server:cast(emqx_gateway_registry, unexpected_cast),
    ok = gen_server:call(emqx_gateway_registry, unexpected_call).
