%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ctl_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_command(_) ->
    emqx_ctl:start_link(),
    emqx_ctl:register_command(test, {?MODULE, test}),
    ct:sleep(50),
    ?assertEqual([{emqx_ctl_SUITE,test}], emqx_ctl:lookup_command(test)),
    ?assertEqual(ok, emqx_ctl:run_command(["test", "ok"])),
    ?assertEqual({error, test_failed}, emqx_ctl:run_command(["test", "error"])),
    ?assertEqual({error, cmd_not_found}, emqx_ctl:run_command(["test2", "ok"])),
    emqx_ctl:unregister_command(test),
    ct:sleep(50),
    ?assertEqual([], emqx_ctl:lookup_command(test)).

test(["ok"]) ->
    ok;
test(["error"]) ->
    error(test_failed);
test(_) ->
    io:format("Hello world").



