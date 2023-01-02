%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_retainer_ct_helper:ensure_start(),
    Config.

end_per_suite(_Config) ->
    emqx_retainer_ct_helper:ensure_stop(),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_retainer:clean(<<"#">>).

t_cmd(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(C1, <<"/retained">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"/retained/2">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),
    timer:sleep(1000),
    ?assertMatch(ok, emqx_retainer_cli:cmd(["topics"])),
    ?assertMatch(ok, emqx_retainer_cli:cmd(["info"])),
    ?assertMatch(ok, emqx_retainer_cli:cmd(["clean", "retained"])),
    ?assertMatch(ok, emqx_retainer_cli:cmd(["clean"])).

% t_unload(_) ->
%     error('TODO').

% t_load(_) ->
%     error('TODO').
