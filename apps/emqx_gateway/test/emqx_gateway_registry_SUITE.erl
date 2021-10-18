%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(CONF_DEFAULT, <<"""
gateway: {
    stomp {}
}
""">>).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Cfg) ->
    ok = emqx_config:init_load(emqx_gateway_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([emqx_gateway]),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_common_test_helpers:stop_apps([emqx_gateway]),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_unload(_) ->
    OldCnt = length(emqx_gateway_registry:list()),
    RgOpts = [{cbkmod, ?MODULE}],
    ok = emqx_gateway_registry:reg(test, RgOpts),
    ?assertEqual(OldCnt+1, length(emqx_gateway_registry:list())),

    #{cbkmod := ?MODULE,
      rgopts := RgOpts} = emqx_gateway_registry:lookup(test),

    {error, already_existed} = emqx_gateway_registry:reg(test, [{cbkmod, ?MODULE}]),

    ok = emqx_gateway_registry:unreg(test),
    undefined = emqx_gateway_registry:lookup(test),
    OldCnt = length(emqx_gateway_registry:list()),
    ok.
