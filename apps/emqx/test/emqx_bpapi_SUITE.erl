%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("../src/bpapi/emqx_bpapi.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [mnesia:dirty_write(Rec) || Rec <- fake_records()],
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    meck:unload(),
    emqx_cth_suite:stop(?config(apps, Config)).

t_max_supported_version(_Config) ->
    ?assertMatch(3, emqx_bpapi:supported_version('fake-node2@localhost', api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(api2)),
    ?assertMatch(undefined, emqx_bpapi:supported_version('fake-node2@localhost', nonexistent_api)),
    ?assertError(_, emqx_bpapi:supported_version(nonexistent_api)).

t_announce(Config) ->
    meck:new(emqx_bpapi, [passthrough, no_history]),
    Filename = filename:join(?config(data_dir, Config), "test.versions"),
    meck:expect(emqx_bpapi, versions_file, fun(_) -> Filename end),
    FakeNode = 'fake-node@127.0.0.1',
    ?assertMatch(ok, emqx_bpapi:announce(FakeNode, emqx)),
    timer:sleep(100),
    ?assertMatch(4, emqx_bpapi:supported_version(FakeNode, api2)),
    ?assertMatch(2, emqx_bpapi:supported_version(FakeNode, api1)),
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
