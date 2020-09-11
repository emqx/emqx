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

-module(emqx_rule_utils_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(PORT, 9876).

all() -> emqx_ct:all(?MODULE).

t_preproc_sql(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, GetPrepareParams} = emqx_rule_utils:preproc_sql(<<"a:${a},b:${b},c:${c},d:${d}">>, '?'),
    ?assertEqual(<<"a:?,b:?,c:?,d:?">>, PrepareStatement),
    ?assertEqual([<<"1">>,1,1.0,<<"{\"d1\":\"hi\"}">>],
                 GetPrepareParams(Selected)).

t_http_connectivity(_) ->
    {ok, Socket} = gen_tcp:listen(?PORT, []),
    ok = emqx_rule_utils:http_connectivity("http://127.0.0.1:"++emqx_rule_utils:str(?PORT), 1000),
    gen_tcp:close(Socket),
    {error, _} = emqx_rule_utils:http_connectivity("http://127.0.0.1:"++emqx_rule_utils:str(?PORT), 1000).

t_tcp_connectivity(_) ->
    {ok, Socket} = gen_tcp:listen(?PORT, []),
    ok = emqx_rule_utils:tcp_connectivity("127.0.0.1", ?PORT, 1000),
    gen_tcp:close(Socket),
    {error, _} = emqx_rule_utils:tcp_connectivity("127.0.0.1", ?PORT, 1000).

t_str(_) ->
    ?assertEqual("abc", emqx_rule_utils:str("abc")),
    ?assertEqual("abc", emqx_rule_utils:str(abc)),
    ?assertEqual("{\"a\":1}", emqx_rule_utils:str(#{a => 1})),
    ?assertEqual("1", emqx_rule_utils:str(1)),
    ?assertEqual("2.0", emqx_rule_utils:str(2.0)),
    ?assertEqual("true", emqx_rule_utils:str(true)),
    ?assertError(_, emqx_rule_utils:str({a, v})).

t_bin(_) ->
    ?assertEqual(<<"abc">>, emqx_rule_utils:bin("abc")),
    ?assertEqual(<<"abc">>, emqx_rule_utils:bin(abc)),
    ?assertEqual(<<"{\"a\":1}">>, emqx_rule_utils:bin(#{a => 1})),
    ?assertEqual(<<"[{\"a\":1}]">>, emqx_rule_utils:bin([#{a => 1}])),
    ?assertEqual(<<"1">>, emqx_rule_utils:bin(1)),
    ?assertEqual(<<"2.0">>, emqx_rule_utils:bin(2.0)),
    ?assertEqual(<<"true">>, emqx_rule_utils:bin(true)),
    ?assertError(_, emqx_rule_utils:bin({a, v})).

t_atom_key(_) ->
    _ = erlang, _ = port,
    ?assertEqual([erlang], emqx_rule_utils:atom_key([<<"erlang">>])),
    ?assertEqual([erlang, port], emqx_rule_utils:atom_key([<<"erlang">>, port])),
    ?assertEqual([erlang, port], emqx_rule_utils:atom_key([<<"erlang">>, <<"port">>])),
    ?assertEqual(erlang, emqx_rule_utils:atom_key(<<"erlang">>)),
    ?assertError({invalid_key, {a, v}}, emqx_rule_utils:atom_key({a, v})),
    _ = xyz876gv123,
    ?assertEqual([xyz876gv123, port], emqx_rule_utils:atom_key([<<"xyz876gv123">>, port])).

t_unsafe_atom_key(_) ->
    ?assertEqual([xyz876gv], emqx_rule_utils:unsafe_atom_key([<<"xyz876gv">>])),
    ?assertEqual([xyz876gv33, port], emqx_rule_utils:unsafe_atom_key([<<"xyz876gv33">>, port])),
    ?assertEqual([xyz876gv331, port1221], emqx_rule_utils:unsafe_atom_key([<<"xyz876gv331">>, <<"port1221">>])),
    ?assertEqual(xyz876gv3312, emqx_rule_utils:unsafe_atom_key(<<"xyz876gv3312">>)).

t_proc_tmpl(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    Tks = emqx_rule_utils:preproc_tmpl(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(<<"a:1,b:1,c:1.0,d:{\"d1\":\"hi\"}">>,
                 emqx_rule_utils:proc_tmpl(Tks, Selected)).
