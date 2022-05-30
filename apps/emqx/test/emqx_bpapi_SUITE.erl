%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bpapi_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/src/bpapi/emqx_bpapi.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx]),
    [mnesia:dirty_write(Rec) || Rec <- fake_records()],
    Config.

end_per_suite(_Config) ->
    meck:unload(),
    [mnesia:dirty_delete({?TAB, Key}) || #?TAB{key = Key} <- fake_records()],
    emqx_bpapi:announce(emqx),
    emqx_common_test_helpers:stop_apps([emqx]),
    ok.

t_max_supported_version(_Config) ->
    ?assertMatch(3, emqx_bpapi:supported_version('fake-node2@localhost', api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(api2)),
    ?assertError(_, emqx_bpapi:supported_version('fake-node2@localhost', nonexistent_api)),
    ?assertError(_, emqx_bpapi:supported_version(nonexistent_api)).

t_announce(Config) ->
    meck:new(emqx_bpapi, [passthrough, no_history]),
    Filename = filename:join(?config(data_dir, Config), "test.versions"),
    meck:expect(emqx_bpapi, versions_file, fun(_) -> Filename end),
    ?assertMatch(ok, emqx_bpapi:announce(emqx)),
    timer:sleep(100),
    ?assertMatch(4, emqx_bpapi:supported_version(node(), api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(node(), api1)),
    ?assertMatch(2, emqx_bpapi:supported_version(api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(api1)).

fake_records() ->
    [
        #?TAB{key = {'fake-node@localhost', api1}, version = 2},
        #?TAB{key = {'fake-node2@localhost', api1}, version = 2},
        #?TAB{key = {?multicall, api1}, version = 2},

        #?TAB{key = {'fake-node@localhost', api2}, version = 2},
        #?TAB{key = {'fake-node2@localhost', api2}, version = 3},
        #?TAB{key = {?multicall, api2}, version = 2}
    ].
