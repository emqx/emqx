%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([
    preproc_tmpl/1,
    preproc_tmpl/2,
    proc_tmpl/2,
    proc_tmpl/3,
    preproc_cmd/1,
    proc_cmd/2,
    proc_cmd/3,
    preproc_sql/1,
    preproc_sql/2,
    proc_sql/2,
    proc_sql_param_str/2,
    proc_cql_param_str/2,
    preproc_tmpl_deep/1,
    preproc_tmpl_deep/2,
    proc_tmpl_deep/2,
    proc_tmpl_deep/3,

    bin/1,
    sql_data/1
]).

-define(EX_PLACE_HOLDER, "(\\$\\{[a-zA-Z0-9\\._]+\\})").
%% Space and CRLF
-define(EX_WITHE_CHARS, "\\s").

-type tmpl_token() :: list({var, binary()} | {str, binary()}).

-type tmpl_cmd() :: list(tmpl_token()).

-type prepare_statement_key() :: binary().

-type var_trans() ::
    fun((FoundValue :: term()) -> binary())
    | fun((Placeholder :: term(), FoundValue :: term()) -> binary()).

-type preproc_tmpl_opts() :: #{placeholders => list(binary())}.

-type preproc_sql_opts() :: #{
    placeholders => list(binary()),
    replace_with => '?' | '$n'
}.

-type preproc_deep_opts() :: #{
    placeholders => list(binary()),
    process_keys => boolean()
}.

-type proc_tmpl_opts() :: #{
    return => rawlist | full_binary,
    var_trans => var_trans()
}.

-type deep_template() ::
    #{deep_template() => deep_template()}
    | {tuple, [deep_template()]}
    | [deep_template()]
    | {tmpl, tmpl_token()}
    | {value, term()}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec preproc_tmpl(binary()) -> tmpl_token().
preproc_tmpl(Str) ->
    preproc_tmpl(Str, #{}).

-spec preproc_tmpl(binary(), preproc_tmpl_opts()) -> tmpl_token().
preproc_tmpl(Str, Opts) ->
    RE = preproc_var_re(Opts),
    Tokens = re:split(Str, RE, [{return, binary}, group, trim]),
    do_preproc_tmpl(Tokens, []).

-spec proc_tmpl(tmpl_token(), map()) -> binary().
proc_tmpl(Tokens, Data) ->
    proc_tmpl(Tokens, Data, #{return => full_binary}).

-spec proc_tmpl(tmpl_token(), map(), proc_tmpl_opts()) -> binary() | list().
proc_tmpl(Tokens, Data, Opts = #{return := full_binary}) ->
    Trans = maps:get(var_trans, Opts, fun emqx_plugin_libs_rule:bin/1),
    list_to_binary(
        proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => Trans})
    );
proc_tmpl(Tokens, Data, Opts = #{return := rawlist}) ->
    Trans = maps:get(var_trans, Opts, undefined),
    lists:map(
        fun
            ({str, Str}) ->
                Str;
            ({var, Phld}) when is_function(Trans, 1) ->
                Trans(get_phld_var(Phld, Data));
            ({var, Phld}) when is_function(Trans, 2) ->
                Trans(Phld, get_phld_var(Phld, Data));
            ({var, Phld}) ->
                get_phld_var(Phld, Data)
        end,
        Tokens
    ).

-spec preproc_cmd(binary()) -> tmpl_cmd().
preproc_cmd(Str) ->
    SubStrList = re:split(Str, ?EX_WITHE_CHARS, [{return, binary}, trim]),
    [preproc_tmpl(SubStr) || SubStr <- SubStrList].

-spec proc_cmd([tmpl_token()], map()) -> binary() | list().
proc_cmd(Tokens, Data) ->
    proc_cmd(Tokens, Data, #{return => full_binary}).
-spec proc_cmd([tmpl_token()], map(), map()) -> list().
proc_cmd(Tokens, Data, Opts) ->
    [proc_tmpl(Tks, Data, Opts) || Tks <- Tokens].

%% preprocess SQL with place holders
-spec preproc_sql(Sql :: binary()) -> {prepare_statement_key(), tmpl_token()}.
preproc_sql(Sql) ->
    preproc_sql(Sql, '?').

-spec preproc_sql(binary(), '?' | '$n' | preproc_sql_opts()) ->
    {prepare_statement_key(), tmpl_token()}.
preproc_sql(Sql, ReplaceWith) when is_atom(ReplaceWith) ->
    preproc_sql(Sql, #{replace_with => ReplaceWith});
preproc_sql(Sql, Opts) ->
    RE = preproc_var_re(Opts),
    ReplaceWith = maps:get(replace_with, Opts, '?'),
    case re:run(Sql, RE, [{capture, all_but_first, binary}, global]) of
        {match, PlaceHolders} ->
            PhKs = [parse_nested(unwrap(Phld)) || [Phld | _] <- PlaceHolders],
            {replace_with(Sql, RE, ReplaceWith), [{var, Phld} || Phld <- PhKs]};
        nomatch ->
            {Sql, []}
    end.

-spec proc_sql(tmpl_token(), map()) -> list().
proc_sql(Tokens, Data) ->
    proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => fun sql_data/1}).

-spec proc_sql_param_str(tmpl_token(), map()) -> binary().
proc_sql_param_str(Tokens, Data) ->
    proc_param_str(Tokens, Data, fun quote_sql/1).

-spec proc_cql_param_str(tmpl_token(), map()) -> binary().
proc_cql_param_str(Tokens, Data) ->
    proc_param_str(Tokens, Data, fun quote_cql/1).

-spec preproc_tmpl_deep(term()) -> deep_template().
preproc_tmpl_deep(Data) ->
    preproc_tmpl_deep(Data, #{process_keys => true}).

-spec preproc_tmpl_deep(term(), preproc_deep_opts()) -> deep_template().
preproc_tmpl_deep(List, Opts) when is_list(List) ->
    [preproc_tmpl_deep(El, Opts) || El <- List];
preproc_tmpl_deep(Map, Opts) when is_map(Map) ->
    maps:from_list(
        lists:map(
            fun({K, V}) ->
                {preproc_tmpl_deep_map_key(K, Opts), preproc_tmpl_deep(V, Opts)}
            end,
            maps:to_list(Map)
        )
    );
preproc_tmpl_deep(Binary, Opts) when is_binary(Binary) ->
    {tmpl, preproc_tmpl(Binary, Opts)};
preproc_tmpl_deep(Tuple, Opts) when is_tuple(Tuple) ->
    {tuple, preproc_tmpl_deep(tuple_to_list(Tuple), Opts)};
preproc_tmpl_deep(Other, _Opts) ->
    {value, Other}.

-spec proc_tmpl_deep(deep_template(), map()) -> term().
proc_tmpl_deep(DeepTmpl, Data) ->
    proc_tmpl_deep(DeepTmpl, Data, #{return => full_binary}).

-spec proc_tmpl_deep(deep_template(), map(), proc_tmpl_opts()) -> term().
proc_tmpl_deep(List, Data, Opts) when is_list(List) ->
    [proc_tmpl_deep(El, Data, Opts) || El <- List];
proc_tmpl_deep(Map, Data, Opts) when is_map(Map) ->
    maps:from_list(
        lists:map(
            fun({K, V}) ->
                {proc_tmpl_deep(K, Data, Opts), proc_tmpl_deep(V, Data, Opts)}
            end,
            maps:to_list(Map)
        )
    );
proc_tmpl_deep({value, Value}, _Data, _Opts) ->
    Value;
proc_tmpl_deep({tmpl, Tokens}, Data, Opts) ->
    proc_tmpl(Tokens, Data, Opts);
proc_tmpl_deep({tuple, Elements}, Data, Opts) ->
    list_to_tuple([proc_tmpl_deep(El, Data, Opts) || El <- Elements]).

-spec sql_data(term()) -> term().
sql_data(undefined) -> null;
sql_data(List) when is_list(List) -> List;
sql_data(Bin) when is_binary(Bin) -> Bin;
sql_data(Num) when is_number(Num) -> Num;
sql_data(Bool) when is_boolean(Bool) -> Bool;
sql_data(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
sql_data(Map) when is_map(Map) -> emqx_json:encode(Map).

-spec bin(term()) -> binary().
bin(Val) -> emqx_plugin_libs_rule:bin(Val).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

proc_param_str(Tokens, Data, Quote) ->
    iolist_to_binary(
        proc_tmpl(Tokens, Data, #{return => rawlist, var_trans => Quote})
    ).

%% backward compatibility for hot upgrading from =< e4.2.1
get_phld_var(Fun, Data) when is_function(Fun) ->
    Fun(Data);
get_phld_var(Phld, Data) ->
    emqx_rule_maps:nested_get(Phld, Data).

preproc_var_re(#{placeholders := PHs}) ->
    "(" ++ string:join([ph_to_re(PH) || PH <- PHs], "|") ++ ")";
preproc_var_re(#{}) ->
    ?EX_PLACE_HOLDER.

ph_to_re(VarPH) ->
    re:replace(VarPH, "[\\$\\{\\}]", "\\\\&", [global, {return, list}]).

do_preproc_tmpl([], Acc) ->
    lists:reverse(Acc);
do_preproc_tmpl([[Str, Phld] | Tokens], Acc) ->
    do_preproc_tmpl(
        Tokens,
        put_head(
            var,
            parse_nested(unwrap(Phld)),
            put_head(str, Str, Acc)
        )
    );
do_preproc_tmpl([[Str] | Tokens], Acc) ->
    do_preproc_tmpl(
        Tokens,
        put_head(str, Str, Acc)
    ).

put_head(_Type, <<>>, List) -> List;
put_head(Type, Term, List) -> [{Type, Term} | List].

preproc_tmpl_deep_map_key(Key, #{process_keys := true} = Opts) ->
    preproc_tmpl_deep(Key, Opts);
preproc_tmpl_deep_map_key(Key, _) ->
    {value, Key}.

replace_with(Tmpl, RE, '?') ->
    re:replace(Tmpl, RE, "?", [{return, binary}, global]);
replace_with(Tmpl, RE, '$n') ->
    Parts = re:split(Tmpl, RE, [{return, binary}, trim, group]),
    {Res, _} =
        lists:foldl(
            fun
                ([Tkn, _Phld], {Acc, Seq}) ->
                    Seq1 = erlang:integer_to_binary(Seq),
                    {<<Acc/binary, Tkn/binary, "$", Seq1/binary>>, Seq + 1};
                ([Tkn], {Acc, Seq}) ->
                    {<<Acc/binary, Tkn/binary>>, Seq}
            end,
            {<<>>, 1},
            Parts
        ),
    Res.

parse_nested(Attr) ->
    case string:split(Attr, <<".">>, all) of
        [Attr] -> {var, Attr};
        Nested -> {path, [{key, P} || P <- Nested]}
    end.

unwrap(<<"${", Val/binary>>) ->
    binary:part(Val, {0, byte_size(Val) - 1}).

quote_sql(Str) ->
    quote(Str, <<"\\\\'">>).

quote_cql(Str) ->
    quote(Str, <<"''">>).

quote(Str, ReplaceWith) when
    is_list(Str);
    is_binary(Str);
    is_atom(Str);
    is_map(Str)
->
    [$', escape_apo(bin(Str), ReplaceWith), $'];
quote(Val, _) ->
    bin(Val).

escape_apo(Str, ReplaceWith) ->
    re:replace(Str, <<"'">>, ReplaceWith, [{return, binary}, global]).
