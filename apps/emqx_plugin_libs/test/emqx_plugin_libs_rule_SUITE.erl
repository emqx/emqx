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

-module(emqx_plugin_libs_rule_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(PORT, 9876).

all() -> emqx_common_test_helpers:all(?MODULE).

t_http_connectivity(_) ->
    {ok, Socket} = gen_tcp:listen(?PORT, []),
    ok = emqx_plugin_libs_rule:http_connectivity(
        "http://127.0.0.1:" ++ emqx_plugin_libs_rule:str(?PORT), 1000
    ),
    gen_tcp:close(Socket),
    {error, _} = emqx_plugin_libs_rule:http_connectivity(
        "http://127.0.0.1:" ++ emqx_plugin_libs_rule:str(?PORT), 1000
    ).

t_tcp_connectivity(_) ->
    {ok, Socket} = gen_tcp:listen(?PORT, []),
    ok = emqx_plugin_libs_rule:tcp_connectivity("127.0.0.1", ?PORT, 1000),
    gen_tcp:close(Socket),
    {error, _} = emqx_plugin_libs_rule:tcp_connectivity("127.0.0.1", ?PORT, 1000).

t_str(_) ->
    ?assertEqual("abc", emqx_plugin_libs_rule:str("abc")),
    ?assertEqual("abc", emqx_plugin_libs_rule:str(abc)),
    ?assertEqual("{\"a\":1}", emqx_plugin_libs_rule:str(#{a => 1})),
    ?assertEqual("1", emqx_plugin_libs_rule:str(1)),
    ?assertEqual("2.0", emqx_plugin_libs_rule:str(2.0)),
    ?assertEqual("true", emqx_plugin_libs_rule:str(true)),
    ?assertError(_, emqx_plugin_libs_rule:str({a, v})).

t_bin(_) ->
    ?assertEqual(<<"abc">>, emqx_plugin_libs_rule:bin("abc")),
    ?assertEqual(<<"abc">>, emqx_plugin_libs_rule:bin(abc)),
    ?assertEqual(<<"{\"a\":1}">>, emqx_plugin_libs_rule:bin(#{a => 1})),
    ?assertEqual(<<"[{\"a\":1}]">>, emqx_plugin_libs_rule:bin([#{a => 1}])),
    ?assertEqual(<<"1">>, emqx_plugin_libs_rule:bin(1)),
    ?assertEqual(<<"2.0">>, emqx_plugin_libs_rule:bin(2.0)),
    ?assertEqual(<<"true">>, emqx_plugin_libs_rule:bin(true)),
    ?assertError(_, emqx_plugin_libs_rule:bin({a, v})).

t_atom_key(_) ->
    _ = erlang,
    _ = port,
    ?assertEqual([erlang], emqx_plugin_libs_rule:atom_key([<<"erlang">>])),
    ?assertEqual([erlang, port], emqx_plugin_libs_rule:atom_key([<<"erlang">>, port])),
    ?assertEqual([erlang, port], emqx_plugin_libs_rule:atom_key([<<"erlang">>, <<"port">>])),
    ?assertEqual(erlang, emqx_plugin_libs_rule:atom_key(<<"erlang">>)),
    ?assertError({invalid_key, {a, v}}, emqx_plugin_libs_rule:atom_key({a, v})),
    _ = xyz876gv123,
    ?assertEqual([xyz876gv123, port], emqx_plugin_libs_rule:atom_key([<<"xyz876gv123">>, port])).

t_unsafe_atom_key(_) ->
    ?assertEqual([xyz876gv], emqx_plugin_libs_rule:unsafe_atom_key([<<"xyz876gv">>])),
    ?assertEqual(
        [xyz876gv33, port],
        emqx_plugin_libs_rule:unsafe_atom_key([<<"xyz876gv33">>, port])
    ),
    ?assertEqual(
        [xyz876gv331, port1221],
        emqx_plugin_libs_rule:unsafe_atom_key([<<"xyz876gv331">>, <<"port1221">>])
    ),
    ?assertEqual(xyz876gv3312, emqx_plugin_libs_rule:unsafe_atom_key(<<"xyz876gv3312">>)).
