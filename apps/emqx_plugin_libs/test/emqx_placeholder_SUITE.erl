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

-module(emqx_placeholder_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_proc_tmpl(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    Tks = emqx_placeholder:preproc_tmpl(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(
        <<"a:1,b:1,c:1.0,d:{\"d1\":\"hi\"}">>,
        emqx_placeholder:proc_tmpl(Tks, Selected)
    ).

t_proc_tmpl_path(_) ->
    Selected = #{d => #{d1 => <<"hi">>}},
    Tks = emqx_placeholder:preproc_tmpl(<<"d.d1:${d.d1}">>),
    ?assertEqual(
        <<"d.d1:hi">>,
        emqx_placeholder:proc_tmpl(Tks, Selected)
    ).

t_proc_tmpl_custom_ph(_) ->
    Selected = #{a => <<"a">>, b => <<"b">>},
    Tks = emqx_placeholder:preproc_tmpl(<<"a:${a},b:${b}">>, #{placeholders => [<<"${a}">>]}),
    ?assertEqual(
        <<"a:a,b:${b}">>,
        emqx_placeholder:proc_tmpl(Tks, Selected)
    ).

t_proc_tmpl1(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    Tks = emqx_placeholder:preproc_tmpl(<<"a:$a,b:b},c:{c},d:${d">>),
    ?assertEqual(
        <<"a:$a,b:b},c:{c},d:${d">>,
        emqx_placeholder:proc_tmpl(Tks, Selected)
    ).

t_proc_cmd(_) ->
    Selected = #{v0 => <<"x">>, v1 => <<"1">>, v2 => #{d1 => <<"hi">>}},
    Tks = emqx_placeholder:preproc_cmd(<<"hset name a:${v0} ${v1} b ${v2} ">>),
    ?assertEqual(
        [
            <<"hset">>,
            <<"name">>,
            <<"a:x">>,
            <<"1">>,
            <<"b">>,
            <<"{\"d1\":\"hi\"}">>
        ],
        emqx_placeholder:proc_cmd(Tks, Selected)
    ).

t_preproc_sql(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, ParamsTokens} =
        emqx_placeholder:preproc_sql(<<"a:${a},b:${b},c:${c},d:${d}">>, '?'),
    ?assertEqual(<<"a:?,b:?,c:?,d:?">>, PrepareStatement),
    ?assertEqual(
        [<<"1">>, 1, 1.0, <<"{\"d1\":\"hi\"}">>],
        emqx_placeholder:proc_sql(ParamsTokens, Selected)
    ).

t_preproc_sql1(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, ParamsTokens} =
        emqx_placeholder:preproc_sql(<<"a:${a},b:${b},c:${c},d:${d}">>, '$n'),
    ?assertEqual(<<"a:$1,b:$2,c:$3,d:$4">>, PrepareStatement),
    ?assertEqual(
        [<<"1">>, 1, 1.0, <<"{\"d1\":\"hi\"}">>],
        emqx_placeholder:proc_sql(ParamsTokens, Selected)
    ).

t_preproc_sql2(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, ParamsTokens} =
        emqx_placeholder:preproc_sql(<<"a:$a,b:b},c:{c},d:${d">>, '?'),
    ?assertEqual(<<"a:$a,b:b},c:{c},d:${d">>, PrepareStatement),
    ?assertEqual([], emqx_placeholder:proc_sql(ParamsTokens, Selected)).

t_preproc_sql3(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    ParamsTokens = emqx_placeholder:preproc_tmpl(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(
        <<"a:'1',b:1,c:1.0,d:'{\"d1\":\"hi\"}'">>,
        emqx_placeholder:proc_sql_param_str(ParamsTokens, Selected)
    ).

t_preproc_sql4(_) ->
    %% with apostrophes
    %% https://github.com/emqx/emqx/issues/4135
    Selected = #{
        a => <<"1''2">>,
        b => 1,
        c => 1.0,
        d => #{d1 => <<"someone's phone">>}
    },
    ParamsTokens = emqx_placeholder:preproc_tmpl(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(
        <<"a:'1\\'\\'2',b:1,c:1.0,d:'{\"d1\":\"someone\\'s phone\"}'">>,
        emqx_placeholder:proc_sql_param_str(ParamsTokens, Selected)
    ).

t_preproc_sql5(_) ->
    %% with apostrophes for cassandra
    %% https://github.com/emqx/emqx/issues/4148
    Selected = #{
        a => <<"1''2">>,
        b => 1,
        c => 1.0,
        d => #{d1 => <<"someone's phone">>}
    },
    ParamsTokens = emqx_placeholder:preproc_tmpl(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(
        <<"a:'1''''2',b:1,c:1.0,d:'{\"d1\":\"someone''s phone\"}'">>,
        emqx_placeholder:proc_cql_param_str(ParamsTokens, Selected)
    ).

t_preproc_sql6(_) ->
    Selected = #{a => <<"a">>, b => <<"b">>},
    {PrepareStatement, ParamsTokens} = emqx_placeholder:preproc_sql(
        <<"a:${a},b:${b}">>,
        #{
            replace_with => '$n',
            placeholders => [<<"${a}">>]
        }
    ),
    ?assertEqual(<<"a:$1,b:${b}">>, PrepareStatement),
    ?assertEqual(
        [<<"a">>],
        emqx_placeholder:proc_sql(ParamsTokens, Selected)
    ).

t_preproc_tmpl_deep(_) ->
    Selected = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},

    Tmpl0 = emqx_placeholder:preproc_tmpl_deep(
        #{<<"${a}">> => [<<"${b}">>, "c", 2, 3.0, '${d}', {[<<"${c}">>], 0}]}
    ),
    ?assertEqual(
        #{<<"1">> => [<<"1">>, "c", 2, 3.0, '${d}', {[<<"1.0">>], 0}]},
        emqx_placeholder:proc_tmpl_deep(Tmpl0, Selected)
    ),

    Tmpl1 = emqx_placeholder:preproc_tmpl_deep(
        #{<<"${a}">> => [<<"${b}">>, "c", 2, 3.0, '${d}', {[<<"${c}">>], 0}]},
        #{process_keys => false}
    ),
    ?assertEqual(
        #{<<"${a}">> => [<<"1">>, "c", 2, 3.0, '${d}', {[<<"1.0">>], 0}]},
        emqx_placeholder:proc_tmpl_deep(Tmpl1, Selected)
    ).
