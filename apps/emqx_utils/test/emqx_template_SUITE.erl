%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_template_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx_placeholder.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_render(_) ->
    Context = #{
        a => <<"1">>,
        b => 1,
        c => 1.0,
        d => #{<<"d1">> => <<"hi">>},
        l => [0, 1, 1000],
        u => "utf-8 is ǝɹǝɥ"
    },
    Template = emqx_template:parse(
        <<"a:${a},b:${b},c:${c},d:${d},d1:${d.d1},l:${l},u:${u}">>
    ),
    ?assertEqual(
        {<<"a:1,b:1,c:1.0,d:{\"d1\":\"hi\"},d1:hi,l:[0,1,1000],u:utf-8 is ǝɹǝɥ"/utf8>>, []},
        render_string(Template, Context)
    ).

t_render_var_trans(_) ->
    Context = #{a => <<"1">>, b => 1, c => #{prop => 1.0}},
    Template = emqx_template:parse(<<"a:${a},b:${b},c:${c.prop}">>),
    {String, Errors} = emqx_template:render(
        Template,
        Context,
        #{var_trans => fun(Name, _) -> "<" ++ Name ++ ">" end}
    ),
    ?assertEqual(
        {<<"a:<a>,b:<b>,c:<c.prop>">>, []},
        {bin(String), Errors}
    ).

t_render_path(_) ->
    Context = #{d => #{d1 => <<"hi">>}},
    Template = emqx_template:parse(<<"d.d1:${d.d1}">>),
    ?assertEqual(
        ok,
        emqx_template:validate(["d.d1"], Template)
    ),
    ?assertEqual(
        {<<"d.d1:hi">>, []},
        render_string(Template, Context)
    ).

t_render_custom_ph(_) ->
    Context = #{a => <<"a">>, b => <<"b">>},
    Template = emqx_template:parse(<<"a:${a},b:${b}">>),
    ?assertEqual(
        {error, [{"b", disallowed}]},
        emqx_template:validate(["a"], Template)
    ),
    ?assertEqual(
        <<"a:a,b:b">>,
        render_strict_string(Template, Context)
    ).

t_render_this(_) ->
    Context = #{a => <<"a">>, b => [1, 2, 3]},
    Template = emqx_template:parse(<<"this:${} / also:${.}">>),
    ?assertEqual(ok, emqx_template:validate(["."], Template)),
    ?assertEqual(
        % NOTE: order of the keys in the JSON object depends on the JSON encoder
        <<"this:{\"b\":[1,2,3],\"a\":\"a\"} / also:{\"b\":[1,2,3],\"a\":\"a\"}">>,
        render_strict_string(Template, Context)
    ).

t_render_missing_bindings(_) ->
    Context = #{no => #{}, c => #{<<"c1">> => 42}},
    Template = emqx_template:parse(
        <<"a:${a},b:${b},c:${c.c1.c2},d:${d.d1},e:${no.such_atom_i_swear}">>
    ),
    ?assertEqual(
        {<<"a:undefined,b:undefined,c:undefined,d:undefined,e:undefined">>, [
            {"no.such_atom_i_swear", undefined},
            {"d.d1", undefined},
            {"c.c1.c2", {2, number}},
            {"b", undefined},
            {"a", undefined}
        ]},
        render_string(Template, Context)
    ),
    ?assertError(
        [
            {"no.such_atom_i_swear", undefined},
            {"d.d1", undefined},
            {"c.c1.c2", {2, number}},
            {"b", undefined},
            {"a", undefined}
        ],
        render_strict_string(Template, Context)
    ).

t_render_custom_bindings(_) ->
    _ = erlang:put(a, <<"foo">>),
    _ = erlang:put(b, #{<<"bar">> => #{atom => 42}}),
    Template = emqx_template:parse(
        <<"a:${a},b:${b.bar.atom},c:${c},oops:${b.bar.atom.oops}">>
    ),
    ?assertEqual(
        {<<"a:foo,b:42,c:undefined,oops:undefined">>, [
            {"b.bar.atom.oops", {2, number}},
            {"c", undefined}
        ]},
        render_string(Template, {?MODULE, []})
    ).

t_placeholders(_) ->
    TString = <<"a:${a},b:${b},c:$${c},d:{${d.d1}},e:${$}{e},lit:${$}{$}">>,
    Template = emqx_template:parse(TString),
    ?assertEqual(
        ["a", "b", "c", "d.d1"],
        emqx_template:placeholders(Template)
    ),
    ?assertEqual(
        {["a", "b", "d.d1"], ["c"]},
        emqx_template:placeholders(["a", "b", "d.d1", "e"], Template)
    ).

t_unparse(_) ->
    TString = <<"a:${a},b:${b},c:$${c},d:{${d.d1}},e:${$}{e},lit:${$}{$}">>,
    Template = emqx_template:parse(TString),
    ?assertEqual(
        TString,
        unicode:characters_to_binary(emqx_template:unparse(Template))
    ).

t_const(_) ->
    ?assertEqual(
        true,
        emqx_template:is_const(emqx_template:parse(<<"">>))
    ),
    ?assertEqual(
        false,
        emqx_template:is_const(
            emqx_template:parse(<<"a:${a},b:${b},c:${$}{c}">>)
        )
    ),
    ?assertEqual(
        true,
        emqx_template:is_const(
            emqx_template:parse(<<"a:${$}{a},b:${$}{b}">>)
        )
    ).

t_render_partial_ph(_) ->
    Context = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    Template = emqx_template:parse(<<"a:$a,b:b},c:{c},d:${d">>),
    ?assertEqual(
        <<"a:$a,b:b},c:{c},d:${d">>,
        render_strict_string(Template, Context)
    ).

t_parse_escaped(_) ->
    Context = #{a => <<"1">>, b => 1, c => "VAR"},
    Template = emqx_template:parse(<<"a:${a},b:${$}{b},c:${$}{${c}},lit:${$}{$}">>),
    ?assertEqual(
        <<"a:1,b:${b},c:${VAR},lit:${$}">>,
        render_strict_string(Template, Context)
    ).

t_parse_escaped_dquote(_) ->
    Context = #{a => <<"1">>, b => 1},
    Template = emqx_template:parse(<<"a:\"${a}\",b:\"${$}{b}\"">>, #{
        strip_double_quote => true
    }),
    ?assertEqual(
        <<"a:1,b:\"${b}\"">>,
        render_strict_string(Template, Context)
    ).

t_parse_sql_prepstmt(_) ->
    Context = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, RowTemplate} =
        emqx_template_sql:parse_prepstmt(<<"a:${a},b:${b},c:${c},d:${d}">>, #{
            parameters => '?'
        }),
    ?assertEqual(<<"a:?,b:?,c:?,d:?">>, bin(PrepareStatement)),
    ?assertEqual(
        {[<<"1">>, 1, 1.0, <<"{\"d1\":\"hi\"}">>], _Errors = []},
        emqx_template_sql:render_prepstmt(RowTemplate, Context)
    ).

t_parse_sql_prepstmt_n(_) ->
    Context = #{a => undefined, b => true, c => atom, d => #{d1 => 42.1337}},
    {PrepareStatement, RowTemplate} =
        emqx_template_sql:parse_prepstmt(<<"a:${a},b:${b},c:${c},d:${d}">>, #{
            parameters => '$n'
        }),
    ?assertEqual(<<"a:$1,b:$2,c:$3,d:$4">>, bin(PrepareStatement)),
    ?assertEqual(
        [null, true, <<"atom">>, <<"{\"d1\":42.1337}">>],
        emqx_template_sql:render_prepstmt_strict(RowTemplate, Context)
    ).

t_parse_sql_prepstmt_colon(_) ->
    {PrepareStatement, _RowTemplate} =
        emqx_template_sql:parse_prepstmt(<<"a=${a},b=${b},c=${c},d=${d}">>, #{
            parameters => ':n'
        }),
    ?assertEqual(<<"a=:1,b=:2,c=:3,d=:4">>, bin(PrepareStatement)).

t_parse_sql_prepstmt_partial_ph(_) ->
    Context = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},
    {PrepareStatement, RowTemplate} =
        emqx_template_sql:parse_prepstmt(<<"a:$a,b:b},c:{c},d:${d">>, #{parameters => '?'}),
    ?assertEqual(<<"a:$a,b:b},c:{c},d:${d">>, bin(PrepareStatement)),
    ?assertEqual([], emqx_template_sql:render_prepstmt_strict(RowTemplate, Context)).

t_render_sql(_) ->
    Context = #{
        a => <<"1">>,
        b => 1,
        c => 1.0,
        d => #{d1 => <<"hi">>},
        n => undefined,
        u => "utf8's cool 🐸"
    },
    Template = emqx_template:parse(<<"a:${a},b:${b},c:${c},d:${d},n:${n},u:${u}">>),
    ?assertMatch(
        {_String, _Errors = []},
        emqx_template_sql:render(Template, Context, #{})
    ),
    ?assertEqual(
        <<"a:'1',b:1,c:1.0,d:'{\"d1\":\"hi\"}',n:NULL,u:'utf8\\'s cool 🐸'"/utf8>>,
        bin(emqx_template_sql:render_strict(Template, Context, #{}))
    ),
    ?assertEqual(
        <<"a:'1',b:1,c:1.0,d:'{\"d1\":\"hi\"}',n:'undefined',u:'utf8\\'s cool 🐸'"/utf8>>,
        bin(emqx_template_sql:render_strict(Template, Context, #{undefined => "undefined"}))
    ).

t_render_mysql(_) ->
    %% with apostrophes
    %% https://github.com/emqx/emqx/issues/4135
    Context = #{
        a => <<"1''2">>,
        b => 1,
        c => 1.0,
        d => #{d1 => <<"someone's phone">>},
        e => <<$\\, 0, "💩"/utf8>>,
        f => <<"non-utf8", 16#DCC900:24>>,
        g => "utf8's cool 🐸",
        h => imgood
    },
    Template = emqx_template_sql:parse(
        <<"a:${a},b:${b},c:${c},d:${d},e:${e},f:${f},g:${g},h:${h}">>
    ),
    ?assertEqual(
        <<
            "a:'1\\'\\'2',b:1,c:1.0,d:'{\"d1\":\"someone\\'s phone\"}',"
            "e:'\\\\\\0💩',f:0x6E6F6E2D75746638DCC900,g:'utf8\\'s cool 🐸',"/utf8,
            "h:'imgood'"
        >>,
        bin(emqx_template_sql:render_strict(Template, Context, #{escaping => mysql}))
    ).

t_render_cql(_) ->
    %% with apostrophes for cassandra
    %% https://github.com/emqx/emqx/issues/4148
    Context = #{
        a => <<"1''2">>,
        b => 1,
        c => 1.0,
        d => #{d1 => <<"someone's phone">>}
    },
    Template = emqx_template:parse(<<"a:${a},b:${b},c:${c},d:${d}">>),
    ?assertEqual(
        <<"a:'1''''2',b:1,c:1.0,d:'{\"d1\":\"someone''s phone\"}'">>,
        bin(emqx_template_sql:render_strict(Template, Context, #{escaping => cql}))
    ).

t_render_sql_custom_ph(_) ->
    {PrepareStatement, RowTemplate} =
        emqx_template_sql:parse_prepstmt(<<"a:${a},b:${b.c}">>, #{parameters => '$n'}),
    ?assertEqual(
        {error, [{"b.c", disallowed}]},
        emqx_template:validate(["a"], RowTemplate)
    ),
    ?assertEqual(<<"a:$1,b:$2">>, bin(PrepareStatement)).

t_render_sql_strip_double_quote(_) ->
    Context = #{a => <<"a">>, b => <<"b">>},

    %% no strip_double_quote option: "${key}" -> "value"
    {PrepareStatement1, RowTemplate1} = emqx_template_sql:parse_prepstmt(
        <<"a:\"${a}\",b:\"${b}\"">>,
        #{parameters => '$n'}
    ),
    ?assertEqual(<<"a:\"$1\",b:\"$2\"">>, bin(PrepareStatement1)),
    ?assertEqual(
        [<<"a">>, <<"b">>],
        emqx_template_sql:render_prepstmt_strict(RowTemplate1, Context)
    ),

    %% strip_double_quote = true:  "${key}" -> value
    {PrepareStatement2, RowTemplate2} = emqx_template_sql:parse_prepstmt(
        <<"a:\"${a}\",b:\"${b}\"">>,
        #{parameters => '$n', strip_double_quote => true}
    ),
    ?assertEqual(<<"a:$1,b:$2">>, bin(PrepareStatement2)),
    ?assertEqual(
        [<<"a">>, <<"b">>],
        emqx_template_sql:render_prepstmt_strict(RowTemplate2, Context)
    ).

t_render_tmpl_deep(_) ->
    Context = #{a => <<"1">>, b => 1, c => 1.0, d => #{d1 => <<"hi">>}},

    Template = emqx_template:parse_deep(
        #{<<"${a}">> => [<<"$${b}">>, "c", 2, 3.0, '${d}', {[<<"${c}">>, <<"${$}{d}">>], 0}]}
    ),

    ?assertEqual(
        {error, [{V, disallowed} || V <- ["b", "c"]]},
        emqx_template:validate(["a"], Template)
    ),

    ?assertEqual(
        #{<<"1">> => [<<"$1">>, "c", 2, 3.0, '${d}', {[<<"1.0">>, <<"${d}">>], 0}]},
        emqx_template:render_strict(Template, Context)
    ).

t_unparse_tmpl_deep(_) ->
    Term = #{<<"${a}">> => [<<"$${b}">>, "c", 2, 3.0, '${d}', {[<<"${c}">>], <<"${$}{d}">>, 0}]},
    Template = emqx_template:parse_deep(Term),
    ?assertEqual(Term, emqx_template:unparse(Template)).

t_allow_this(_) ->
    ?assertEqual(
        {error, [{"", disallowed}]},
        emqx_template:validate(["d"], emqx_template:parse(<<"this:${}">>))
    ),
    ?assertEqual(
        {error, [{"", disallowed}]},
        emqx_template:validate(["d"], emqx_template:parse(<<"this:${.}">>))
    ).

t_allow_var_by_namespace(_) ->
    Context = #{d => #{d1 => <<"hi">>}},
    Template = emqx_template:parse(<<"d.d1:${d.d1}">>),
    ?assertEqual(
        ok,
        emqx_template:validate([{var_namespace, "d"}], Template)
    ),
    ?assertEqual(
        {<<"d.d1:hi">>, []},
        render_string(Template, Context)
    ).

%%

render_string(Template, Context) ->
    {String, Errors} = emqx_template:render(Template, Context),
    {bin(String), Errors}.

render_strict_string(Template, Context) ->
    bin(emqx_template:render_strict(Template, Context)).

bin(String) ->
    unicode:characters_to_binary(String).

%% Access module API

lookup([], _) ->
    {error, undefined};
lookup([Prop | Rest], _) ->
    case erlang:get(binary_to_atom(Prop)) of
        undefined -> {error, undefined};
        Value -> emqx_template:lookup_var(Rest, Value)
    end.
