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

-module(emqx_placeholder).

%% preprocess and process template string with place holders
-export([ preproc_tmpl/1
        , proc_tmpl/2
        , proc_tmpl/3
        , preproc_cmd/1
        , proc_cmd/2
        , proc_cmd/3
        , preproc_sql/1
        , preproc_sql/2
        , proc_sql/2
        , proc_sql_param_str/2
        , proc_cql_param_str/2
        ]).

-import(emqx_plugin_libs_rule, [bin/1]).

-define(EX_PLACE_HOLDER, "(\\$\\{[a-zA-Z0-9\\._]+\\})").
-define(EX_WITHE_CHARS, "\\s"). %% Space and CRLF

-type(tmpl_token() :: list({var, binary()} | {str, binary()})).

-type(tmpl_cmd() :: list(tmpl_token())).

-type(prepare_statement_key() :: binary()).

%% preprocess template string with place holders
-spec(preproc_tmpl(binary()) -> tmpl_token()).
preproc_tmpl(Str) ->
    Tokens = re:split(Str, ?EX_PLACE_HOLDER, [{return,binary},group,trim]),
    preproc_tmpl(Tokens, []).

preproc_tmpl([], Acc) ->
    lists:reverse(Acc);
preproc_tmpl([[Str, Phld] | Tokens], Acc) ->
    preproc_tmpl(Tokens,
        put_head(var, parse_nested(unwrap(Phld)),
            put_head(str, Str, Acc)));
preproc_tmpl([[Str] | Tokens], Acc) ->
    preproc_tmpl(Tokens, put_head(str, Str, Acc)).

put_head(_Type, <<>>, List) -> List;
put_head(Type, Term, List) ->
    [{Type, Term} | List].

-spec(proc_tmpl(tmpl_token(), map()) -> binary()).
proc_tmpl(Tokens, Data) ->
    proc_tmpl(Tokens, Data, #{return => full_binary}).

-spec(proc_tmpl(tmpl_token(), map(), map()) -> binary() | list()).
proc_tmpl(Tokens, Data, Opts = #{return := full_binary}) ->
    Trans = maps:get(var_trans, Opts, fun emqx_plugin_libs_rule:bin/1),
    list_to_binary(
        proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => Trans}));

proc_tmpl(Tokens, Data, Opts = #{return := rawlist}) ->
    Trans = maps:get(var_trans, Opts, undefined),
    lists:map(
        fun ({str, Str}) -> Str;
            ({var, Phld}) when is_function(Trans) ->
                Trans(get_phld_var(Phld, Data));
            ({var, Phld}) ->
                get_phld_var(Phld, Data)
        end, Tokens).


-spec(preproc_cmd(binary()) -> tmpl_cmd()).
preproc_cmd(Str) ->
    SubStrList = re:split(Str, ?EX_WITHE_CHARS, [{return,binary},trim]),
    [preproc_tmpl(SubStr) || SubStr <- SubStrList].

-spec(proc_cmd([tmpl_token()], map()) -> binary() | list()).
proc_cmd(Tokens, Data) ->
    proc_cmd(Tokens, Data, #{return => full_binary}).
-spec(proc_cmd([tmpl_token()], map(), map()) -> list()).
proc_cmd(Tokens, Data, Opts) ->
    [proc_tmpl(Tks, Data, Opts) || Tks <- Tokens].

%% preprocess SQL with place holders
-spec(preproc_sql(Sql::binary()) -> {prepare_statement_key(), tmpl_token()}).
preproc_sql(Sql) ->
    preproc_sql(Sql, '?').

-spec(preproc_sql(Sql::binary(), ReplaceWith :: '?' | '$n')
        -> {prepare_statement_key(), tmpl_token()}).

preproc_sql(Sql, ReplaceWith) ->
    case re:run(Sql, ?EX_PLACE_HOLDER, [{capture, all_but_first, binary}, global]) of
        {match, PlaceHolders} ->
            PhKs = [parse_nested(unwrap(Phld)) || [Phld | _] <- PlaceHolders],
            {replace_with(Sql, ReplaceWith), [{var, Phld} || Phld <- PhKs]};
        nomatch ->
            {Sql, []}
    end.

-spec(proc_sql(tmpl_token(), map()) -> list()).
proc_sql(Tokens, Data) ->
    proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => fun sql_data/1}).

-spec(proc_sql_param_str(tmpl_token(), map()) -> binary()).
proc_sql_param_str(Tokens, Data) ->
    proc_param_str(Tokens, Data, fun quote_sql/1).

-spec(proc_cql_param_str(tmpl_token(), map()) -> binary()).
proc_cql_param_str(Tokens, Data) ->
    proc_param_str(Tokens, Data, fun quote_cql/1).

proc_param_str(Tokens, Data, Quote) ->
    iolist_to_binary(
        proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => Quote})).

%% backward compatibility for hot upgrading from =< e4.2.1
get_phld_var(Fun, Data) when is_function(Fun) ->
    Fun(Data);
get_phld_var(Phld, Data) ->
    emqx_rule_maps:nested_get(Phld, Data).

replace_with(Tmpl, '?') ->
    re:replace(Tmpl, ?EX_PLACE_HOLDER, "?", [{return, binary}, global]);
replace_with(Tmpl, '$n') ->
    Parts = re:split(Tmpl, ?EX_PLACE_HOLDER, [{return, binary}, trim, group]),
    {Res, _} =
        lists:foldl(
            fun([Tkn, _Phld], {Acc, Seq}) ->
                Seq1 = erlang:integer_to_binary(Seq),
                {<<Acc/binary, Tkn/binary, "$", Seq1/binary>>, Seq + 1};
                ([Tkn], {Acc, Seq}) ->
                    {<<Acc/binary, Tkn/binary>>, Seq}
            end, {<<>>, 1}, Parts),
    Res.

parse_nested(Attr) ->
    case string:split(Attr, <<".">>, all) of
        [Attr] -> {var, Attr};
        Nested -> {path, [{key, P} || P <- Nested]}
    end.

unwrap(<<"${", Val/binary>>) ->
    binary:part(Val, {0, byte_size(Val)-1}).

sql_data(undefined) -> null;
sql_data(List) when is_list(List) -> List;
sql_data(Bin) when is_binary(Bin) -> Bin;
sql_data(Num) when is_number(Num) -> Num;
sql_data(Bool) when is_boolean(Bool) -> Bool;
sql_data(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
sql_data(Map) when is_map(Map) -> emqx_json:encode(Map).

quote_sql(Str) ->
    quote(Str, <<"\\\\'">>).

quote_cql(Str) ->
    quote(Str, <<"''">>).

quote(Str, ReplaceWith) when
    is_list(Str);
    is_binary(Str);
    is_atom(Str);
    is_map(Str) ->
    [$', escape_apo(bin(Str), ReplaceWith), $'];
quote(Val, _) ->
    bin(Val).

escape_apo(Str, ReplaceWith) ->
    re:replace(Str, <<"'">>, ReplaceWith, [{return, binary}, global]).
