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

-module(emqx_rule_utils).

-include("rule_engine.hrl").

-export([ replace_var/2
        ]).

%% preprocess and process tempalte string with place holders
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
        , if_contains_placeholder/1
        ]).

%% type converting
-export([ str/1
        , float2str/2
        , bin/1
        , bool/1
        , int/1
        , float/1
        , map/1
        , utf8_bin/1
        , utf8_str/1
        , number_to_binary/1
        , atom_key/1
        , unsafe_atom_key/1
        ]).

%% connectivity check
-export([ http_connectivity/1
        , http_connectivity/2
        , tcp_connectivity/2
        , tcp_connectivity/3
        ]).

-export([ now_ms/0
        , can_topic_match_oneof/2
        ]).

-export([ add_metadata/2
        , log_action/4
        ]).

-compile({no_auto_import,
          [ float/1
          ]}).

%% To match any pattern starts with '$' and followed by '{', and closed by a '}' char:
%% e.g. for string "a${abc}bb", "${abc}" will be matched.
%% Note that if "${{abc}}" is given, the "${{abc}" should be matched, NOT "${{abc}}".
-define(EX_PLACE_HOLDER, "(\\$\\{.*?\\})").
-define(EX_WITHE_CHARS, "\\s"). %% Space and CRLF
-define(FLOAT_PRECISION, 17).

-type(uri_string() :: iodata()).

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
preproc_tmpl([[Str, Phld]| Tokens], Acc) ->
    preproc_tmpl(Tokens,
        put_head(var, parse_nested(unwrap(Phld)),
            put_head(str, Str, Acc)));
preproc_tmpl([[Str]| Tokens], Acc) ->
    preproc_tmpl(Tokens, put_head(str, Str, Acc)).

%% Replace a simple var to its value. For example, given "${var}", if the var=1, then the result
%% value will be an integer 1.
replace_var(Tokens, Data) when is_list(Tokens) ->
    [Val] = proc_tmpl(Tokens, Data, #{return => rawlist}),
    Val;
replace_var(Val, _Data) ->
    Val.

put_head(_Type, <<>>, List) -> List;
put_head(Type, Term, List) ->
    [{Type, Term} | List].

-spec(proc_tmpl(tmpl_token(), map()) -> binary()).
proc_tmpl(Tokens, Data) ->
    proc_tmpl(Tokens, Data, #{return => full_binary}).

-spec(proc_tmpl(tmpl_token(), map(), map()) -> binary() | list()).
proc_tmpl(Tokens, Data, Opts = #{return := full_binary}) ->
    Trans = maps:get(var_trans, Opts, fun bin/1),
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

%% return true if the Str contains any placeholder in "${var}" format.
-spec(if_contains_placeholder(string() | binary()) -> boolean()).
if_contains_placeholder(Str) ->
    case re:split(Str, ?EX_PLACE_HOLDER, [{return, list}, group, trim]) of
        [[_]] -> false;
        _ -> true
    end.

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

unsafe_atom_key(Key) when is_atom(Key) ->
    Key;
unsafe_atom_key(Key) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
unsafe_atom_key(Keys = [_Key | _]) ->
    [unsafe_atom_key(SubKey) || SubKey <- Keys];
unsafe_atom_key(Key) ->
    error({invalid_key, Key}).

atom_key(Key) when is_atom(Key) ->
    Key;
atom_key(Key) when is_binary(Key) ->
    try binary_to_existing_atom(Key, utf8)
    catch error:badarg -> error({invalid_key, Key})
    end;
atom_key(Keys = [_Key | _]) -> %% nested keys
    [atom_key(SubKey) || SubKey <- Keys];
atom_key(Key) ->
    error({invalid_key, Key}).

-spec(http_connectivity(uri_string()) -> ok | {error, Reason :: term()}).
http_connectivity(Url) ->
    http_connectivity(Url, 3000).

-spec(http_connectivity(uri_string(), integer()) -> ok | {error, Reason :: term()}).
http_connectivity(Url, Timeout) ->
    case emqx_http_lib:uri_parse(Url) of
        {ok, #{host := Host, port := Port}} ->
            tcp_connectivity(Host, Port, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

-spec tcp_connectivity(Host :: inet:socket_address() | inet:hostname(),
                       Port :: inet:port_number())
      -> ok | {error, Reason :: term()}.
tcp_connectivity(Host, Port) ->
    tcp_connectivity(Host, Port, 3000).

-spec(tcp_connectivity(Host :: inet:socket_address() | inet:hostname(),
                       Port :: inet:port_number(),
                       Timeout :: integer())
        -> ok | {error, Reason :: term()}).
tcp_connectivity(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, emqx_misc:ipv6_probe([]), Timeout) of
        {ok, Sock} -> gen_tcp:close(Sock), ok;
        {error, Reason} -> {error, Reason}
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

str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(Num) when is_number(Num) -> number_to_list(Num);
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Map) when is_map(Map) -> binary_to_list(emqx_json:encode(Map));
str(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> List;
        false ->  binary_to_list(emqx_json:encode(List))
    end;
str(Data) -> error({invalid_str, Data}).

float2str(Float, Precision) when is_float(Float) and is_integer(Precision)->
    float_to_binary(Float, [{decimals, Precision}, compact]).

utf8_bin(Str) when is_binary(Str); is_list(Str) ->
    unicode:characters_to_binary(Str);
utf8_bin(Str) ->
    unicode:characters_to_binary(bin(Str)).

utf8_str(Str) when is_binary(Str); is_list(Str) ->
    unicode:characters_to_list(Str);
utf8_str(Str) ->
    unicode:characters_to_list(str(Str)).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Num) when is_number(Num) -> number_to_binary(Num);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
bin(Map) when is_map(Map) -> emqx_json:encode(Map);
bin(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> list_to_binary(List);
        false -> emqx_json:encode(List)
    end;
bin(Data) -> error({invalid_bin, Data}).

int(List) when is_list(List) ->
    try list_to_integer(List)
    catch error:badarg ->
        int(list_to_float(List))
    end;
int(Bin) when is_binary(Bin) ->
    try binary_to_integer(Bin)
    catch error:badarg ->
        int(binary_to_float(Bin))
    end;
int(Int) when is_integer(Int) -> Int;
int(Float) when is_float(Float) -> erlang:floor(Float);
int(true) -> 1;
int(false) -> 0;
int(Data) -> error({invalid_number, Data}).

float(List) when is_list(List) ->
    try list_to_float(List)
    catch error:badarg ->
        float(list_to_integer(List))
    end;
float(Bin) when is_binary(Bin) ->
    try binary_to_float(Bin)
    catch error:badarg ->
        float(binary_to_integer(Bin))
    end;
float(Num) when is_number(Num) -> erlang:float(Num);
float(Data) -> error({invalid_number, Data}).

map(Bin) when is_binary(Bin) ->
    case emqx_json:decode(Bin, [return_maps]) of
        Map = #{} -> Map;
        _ -> error({invalid_map, Bin})
    end;
map(List) when is_list(List) -> maps:from_list(List);
map(Map) when is_map(Map) -> Map;
map(Data) -> error({invalid_map, Data}).


bool(Bool) when Bool == true;
                Bool == <<"true">>;
                Bool == 1 -> true;
bool(Bool) when Bool == false;
                Bool == <<"false">>;
                Bool == 0 -> false;
bool(Bool) -> error({invalid_boolean, Bool}).

number_to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, ?FLOAT_PRECISION}, compact]).

number_to_list(Int) when is_integer(Int) ->
    integer_to_list(Int);
number_to_list(Float) when is_float(Float) ->
    float_to_list(Float, [{decimals, ?FLOAT_PRECISION}, compact]).

parse_nested(Attr) ->
    case string:split(Attr, <<".">>, all) of
        [Attr] -> {var, Attr};
        Nested -> {path, [{key, P} || P <- Nested]}
    end.

now_ms() ->
    erlang:system_time(millisecond).

can_topic_match_oneof(Topic, Filters) ->
    lists:any(fun(Fltr) ->
        emqx_topic:match(Topic, Fltr)
    end, Filters).

add_metadata(Envs, Metadata) when is_map(Envs), is_map(Metadata) ->
    NMetadata = maps:merge(maps:get(metadata, Envs, #{}), Metadata),
    Envs#{metadata => NMetadata};
add_metadata(Envs, Action) when is_map(Envs), is_record(Action, action_instance)->
    Metadata = gen_metadata_from_action(Action),
    NMetadata = maps:merge(maps:get(metadata, Envs, #{}), Metadata),
    Envs#{metadata => NMetadata}.

gen_metadata_from_action(#action_instance{name = Name, args = undefined}) ->
    #{action_name => Name, resource_id => undefined};
gen_metadata_from_action(#action_instance{name = Name, args = Args})
  when is_map(Args) ->
    #{action_name => Name, resource_id => maps:get(<<"$resource">>, Args, undefined)};
gen_metadata_from_action(#action_instance{name = Name}) ->
    #{action_name => Name, resource_id => undefined}.

log_action(Level, Metadata, Fmt, Args) ->
    ?LOG(Level,
         "Rule: ~p; Action: ~p; Resource: ~p. " ++ Fmt,
         metadata_values(Metadata) ++ Args).

metadata_values(Metadata) ->
    RuleId = maps:get(rule_id, Metadata, undefined),
    ActionName = maps:get(action_name, Metadata, undefined),
    ResourceName = maps:get(resource_id, Metadata, undefined),
    [RuleId, ActionName, ResourceName].
