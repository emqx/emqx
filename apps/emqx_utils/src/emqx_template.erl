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

-module(emqx_template).

-export([parse/1]).
-export([parse/2]).
-export([parse_deep/1]).
-export([parse_deep/2]).
-export([placeholders/1]).
-export([placeholders/2]).
-export([validate/2]).
-export([is_const/1]).
-export([unparse/1]).
-export([render/2]).
-export([render/3]).
-export([render_strict/2]).
-export([render_strict/3]).

-export([lookup_var/2]).
-export([lookup/2]).

-export([to_string/1]).
-export([escape_disallowed/2]).

-export_type([t/0]).
-export_type([str/0]).
-export_type([deep/0]).
-export_type([placeholder/0]).
-export_type([varname/0]).
-export_type([bindings/0]).
-export_type([accessor/0]).

-export_type([context/0]).
-export_type([render_opts/0]).

-type t() :: str() | {'$tpl', deeptpl()}.

-type str() :: [iodata() | byte() | placeholder()].
-type deep() :: {'$tpl', deeptpl()}.

-type deeptpl() ::
    t()
    | #{deeptpl() => deeptpl()}
    | {list, [deeptpl()]}
    | {tuple, [deeptpl()]}
    | scalar()
    | function()
    | pid()
    | port()
    | reference().

-type placeholder() :: {var, varname(), accessor()}.
-type accessor() :: [binary()].
-type varname() :: string().

-type scalar() :: atom() | unicode:chardata() | binary() | number().
-type binding() :: scalar() | list(scalar()) | bindings().
-type bindings() :: #{atom() | binary() => binding()}.

-type reason() :: undefined | {location(), _InvalidType :: atom()}.
-type location() :: non_neg_integer().

-type var_trans() ::
    fun((Value :: term()) -> unicode:chardata())
    | fun((varname(), Value :: term()) -> unicode:chardata()).

-type parse_opts() :: #{
    strip_double_quote => boolean()
}.

-type render_opts() :: #{
    var_trans => var_trans()
}.

-type context() ::
    %% Map with (potentially nested) bindings.
    bindings()
    %% Arbitrary term accessible via an access module with `lookup/2` function.
    | {_AccessModule :: module(), _Bindings}.

%% Access module API
-callback lookup(accessor(), _Bindings) -> {ok, _Value} | {error, reason()}.

-define(RE_PLACEHOLDER, "\\$\\{[.]?([a-zA-Z0-9._]*)\\}").
-define(RE_ESCAPE, "\\$\\{(\\$)\\}").

%% @doc Parse a unicode string into a template.
%% String might contain zero or more of placeholders in the form of `${var}`,
%% where `var` is a _location_ (possibly deeply nested) of some value in the
%% bindings map.
%% String might contain special escaped form `$${...}` which interpreted as a
%% literal `${...}`.
-spec parse(String :: unicode:chardata()) ->
    t().
parse(String) ->
    parse(String, #{}).

-spec parse(String :: unicode:chardata(), parse_opts()) ->
    t().
parse(String, Opts) ->
    RE =
        case Opts of
            #{strip_double_quote := true} ->
                <<"((?|" ?RE_PLACEHOLDER "|\"" ?RE_PLACEHOLDER "\")|" ?RE_ESCAPE ")">>;
            #{} ->
                <<"(" ?RE_PLACEHOLDER "|" ?RE_ESCAPE ")">>
        end,
    Splits = re:split(String, RE, [{return, binary}, group, trim, unicode]),
    lists:flatmap(fun parse_split/1, Splits).

parse_split([Part, _PH, Var, <<>>]) ->
    % Regular placeholder
    prepend(Part, [{var, unicode:characters_to_list(Var), parse_accessor(Var)}]);
parse_split([Part, _Escape, <<>>, <<"$">>]) ->
    % Escaped literal `$`.
    % Use single char as token so the `unparse/1` function can distinguish escaped `$`.
    prepend(Part, [$$]);
parse_split([Tail]) ->
    [Tail].

prepend(<<>>, To) ->
    To;
prepend(Head, To) ->
    [Head | To].

parse_accessor(Var) ->
    case string:split(Var, <<".">>, all) of
        [<<>>] ->
            [];
        Name ->
            Name
    end.

%% @doc Extract all used placeholders from a template.
-spec placeholders(t()) -> [varname()].
placeholders(Template) when is_list(Template) ->
    [Name || {var, Name, _} <- Template];
placeholders({'$tpl', Template}) ->
    placeholders_deep(Template).

%% @doc Extract all used placeholders from a template
%% and partition them into allowed and disallowed.
-spec placeholders([varname() | {var_namespace, varname()}], t()) ->
    {_UsedAllowed :: [varname()], _UsedDisallowed :: [varname()]}.
placeholders(Allowed, Template) ->
    UsedAll = placeholders(Template),
    UsedUnique = lists:usort(UsedAll),
    UsedDisallowed = find_disallowed(UsedUnique, Allowed),
    UsedAllowed = UsedUnique -- UsedDisallowed,
    {UsedAllowed, UsedDisallowed}.

%% @doc Validate a template against a set of allowed variables.
%% If the given template contains any variable not in the allowed set, an error
%% is returned.
-spec validate([varname() | {var_namespace, varname()}], t()) ->
    ok | {error, [_Error :: {varname(), disallowed}]}.
validate(Allowed, Template) ->
    case placeholders(Allowed, Template) of
        {_UsedAllowed, []} ->
            ok;
        {_UsedAllowed, Disallowed} ->
            {error, [{Var, disallowed} || Var <- Disallowed]}
    end.

%% @doc Escape `$' with `${$}' for the variable references
%% which are not allowed, so the original variable name
%% can be preserved instead of rendered as `undefined'.
%% E.g. to render `${var1}/${clientid}', if only `clientid'
%% is allowed, the rendering result should be `${var1}/client1'
%% but not `undefined/client1'.
escape_disallowed(Template, Allowed) ->
    {Result, _} = render(Template, #{}, #{
        var_trans => fun(Name, _) ->
            case is_allowed(Name, Allowed) of
                true -> "${" ++ Name ++ "}";
                false -> "${$}{" ++ Name ++ "}"
            end
        end
    }),
    Result.

find_disallowed(Vars, Allowed) ->
    lists:filter(fun(Var) -> not is_allowed(Var, Allowed) end, Vars).

%% @private Return 'true' if a variable reference matches
%% at least one allowed variables.
%% For `"${var_name}"' kind of reference, its a `=:=' compare
%% for `{var_namespace, "namespace"}' kind of reference
%% it matches the `"namespace."' prefix.
is_allowed(_Var, []) ->
    false;
is_allowed(Var, [{var_namespace, VarPrefix} | Allowed]) ->
    case lists:prefix(VarPrefix ++ ".", Var) of
        true ->
            true;
        false ->
            is_allowed(Var, Allowed)
    end;
is_allowed(Var, [VarAllowed | Rest]) ->
    is_same_varname(Var, VarAllowed) orelse is_allowed(Var, Rest).

is_same_varname("", ".") ->
    true;
is_same_varname(V1, V2) ->
    V1 =:= V2.

%% @doc Check if a template is constant with respect to rendering, i.e. does not
%% contain any placeholders.
-spec is_const(t()) ->
    boolean().
is_const(Template) ->
    validate([], Template) == ok.

%% @doc Restore original term from a parsed template.
-spec unparse(t()) ->
    term().
unparse({'$tpl', Template}) ->
    unparse_deep(Template);
unparse(Template) ->
    unicode:characters_to_list(lists:map(fun unparse_part/1, Template)).

unparse_part({var, Name, _Accessor}) ->
    render_placeholder(Name);
unparse_part($$) ->
    <<"${$}">>;
unparse_part(Part) ->
    Part.

render_placeholder(Name) ->
    "${" ++ Name ++ "}".

%% @doc Render a template with given bindings.
%% Returns a term with all placeholders replaced with values from bindings.
%% If one or more placeholders are not found in bindings, an error is returned.
%% By default, all binding values are converted to strings using `to_string/1`
%% function. Option `var_trans` can be used to override this behaviour.
-spec render(t(), context()) ->
    {term(), [_Error :: {varname(), reason()}]}.
render(Template, Context) ->
    render(Template, Context, #{}).

-spec render(t(), context(), render_opts()) ->
    {term(), [_Error :: {varname(), undefined}]}.
render(Template, Context, Opts) when is_list(Template) ->
    lists:mapfoldl(
        fun
            ({var, Name, Accessor}, EAcc) ->
                {String, Errors} = render_binding(Name, Accessor, Context, Opts),
                {String, Errors ++ EAcc};
            (String, EAcc) ->
                {String, EAcc}
        end,
        [],
        Template
    );
render({'$tpl', Template}, Context, Opts) ->
    render_deep(Template, Context, Opts).

render_binding(Name, Accessor, Context, Opts) ->
    case lookup_value(Accessor, Context) of
        {ok, Value} ->
            {render_value(Name, Value, Opts), []};
        {error, Reason} ->
            % TODO
            % Currently, it's not possible to distinguish between a missing value
            % and an atom `undefined` in `TransFun`.
            {render_value(Name, undefined, Opts), [{Name, Reason}]}
    end.

lookup_value(Accessor, {AccessMod, Bindings}) ->
    AccessMod:lookup(Accessor, Bindings);
lookup_value(Accessor, Bindings) ->
    lookup_var(Accessor, Bindings).

render_value(_Name, Value, #{var_trans := TransFun}) when is_function(TransFun, 1) ->
    TransFun(Value);
render_value(Name, Value, #{var_trans := TransFun}) when is_function(TransFun, 2) ->
    TransFun(Name, Value);
render_value(_Name, Value, #{}) ->
    to_string(Value).

%% @doc Render a template with given bindings.
%% Behaves like `render/2`, but raises an error exception if one or more placeholders
%% are not found in the bindings.
-spec render_strict(t(), context()) ->
    term().
render_strict(Template, Context) ->
    render_strict(Template, Context, #{}).

-spec render_strict(t(), context(), render_opts()) ->
    term().
render_strict(Template, Context, Opts) ->
    case render(Template, Context, Opts) of
        {Render, []} ->
            Render;
        {_, Errors = [_ | _]} ->
            error(Errors, [unparse(Template), Context])
    end.

%% @doc Parse an arbitrary Erlang term into a "deep" template.
%% Any binaries nested in the term are treated as string templates, while
%% lists are not analyzed for "printability" and are treated as nested terms.
%% The result is a usual template, and can be fed to other functions in this
%% module.
-spec parse_deep(term()) ->
    t().
parse_deep(Term) ->
    parse_deep(Term, #{}).

-spec parse_deep(term(), parse_opts()) ->
    t().
parse_deep(Term, Opts) ->
    {'$tpl', parse_deep_term(Term, Opts)}.

parse_deep_term(Term, Opts) when is_map(Term) ->
    maps:fold(
        fun(K, V, Acc) ->
            Acc#{parse_deep_term(K, Opts) => parse_deep_term(V, Opts)}
        end,
        #{},
        Term
    );
parse_deep_term(Term, Opts) when is_list(Term) ->
    {list, [parse_deep_term(E, Opts) || E <- Term]};
parse_deep_term(Term, Opts) when is_tuple(Term) ->
    {tuple, [parse_deep_term(E, Opts) || E <- tuple_to_list(Term)]};
parse_deep_term(Term, Opts) when is_binary(Term) ->
    parse(Term, Opts);
parse_deep_term(Term, _Opts) ->
    Term.

-spec placeholders_deep(deeptpl()) -> [varname()].
placeholders_deep(Template) when is_map(Template) ->
    maps:fold(
        fun(KT, VT, Acc) -> placeholders_deep(KT) ++ placeholders_deep(VT) ++ Acc end,
        [],
        Template
    );
placeholders_deep({list, Template}) when is_list(Template) ->
    lists:flatmap(fun placeholders_deep/1, Template);
placeholders_deep({tuple, Template}) when is_list(Template) ->
    lists:flatmap(fun placeholders_deep/1, Template);
placeholders_deep(Template) when is_list(Template) ->
    placeholders(Template);
placeholders_deep(_Term) ->
    [].

render_deep(Template, Context, Opts) when is_map(Template) ->
    maps:fold(
        fun(KT, VT, {Acc, Errors}) ->
            {K, KErrors} = render_deep(KT, Context, Opts),
            {V, VErrors} = render_deep(VT, Context, Opts),
            {Acc#{K => V}, KErrors ++ VErrors ++ Errors}
        end,
        {#{}, []},
        Template
    );
render_deep({list, Template}, Context, Opts) when is_list(Template) ->
    lists:mapfoldr(
        fun(T, Errors) ->
            {E, VErrors} = render_deep(T, Context, Opts),
            {E, VErrors ++ Errors}
        end,
        [],
        Template
    );
render_deep({tuple, Template}, Context, Opts) when is_list(Template) ->
    {Term, Errors} = render_deep({list, Template}, Context, Opts),
    {list_to_tuple(Term), Errors};
render_deep(Template, Context, Opts) when is_list(Template) ->
    {String, Errors} = render(Template, Context, Opts),
    {character_segments_to_binary(String), Errors};
render_deep(Term, _Bindings, _Opts) ->
    {Term, []}.

unparse_deep(Template) when is_map(Template) ->
    maps:fold(
        fun(K, V, Acc) ->
            Acc#{unparse_deep(K) => unparse_deep(V)}
        end,
        #{},
        Template
    );
unparse_deep({list, Template}) when is_list(Template) ->
    [unparse_deep(E) || E <- Template];
unparse_deep({tuple, Template}) when is_list(Template) ->
    list_to_tuple(unparse_deep({list, Template}));
unparse_deep(Template) when is_list(Template) ->
    unicode:characters_to_binary(unparse(Template));
unparse_deep(Term) ->
    Term.

%%

%% @doc Lookup a variable in the bindings accessible through the accessor.
%% Lookup is "loose" in the sense that atom and binary keys in the bindings are
%% treated equally. This is useful for both hand-crafted and JSON-like bindings.
%% This is the default lookup function used by rendering functions.
-spec lookup_var(accessor(), bindings()) ->
    {ok, binding()} | {error, reason()}.
lookup_var(Var, Bindings) ->
    lookup_var(0, Var, Bindings).

lookup_var(_, [], Value) ->
    {ok, Value};
lookup_var(Loc, [Prop | Rest], Bindings) when is_map(Bindings) ->
    case lookup(Prop, Bindings) of
        {ok, Value} ->
            lookup_var(Loc + 1, Rest, Value);
        {error, Reason} ->
            {error, Reason}
    end;
lookup_var(Loc, _, Invalid) ->
    {error, {Loc, type_name(Invalid)}}.

type_name(Term) when is_atom(Term) -> atom;
type_name(Term) when is_number(Term) -> number;
type_name(Term) when is_binary(Term) -> binary;
type_name(Term) when is_list(Term) -> list.

-spec lookup(Prop :: binary(), bindings()) ->
    {ok, binding()} | {error, undefined}.
lookup(Prop, Bindings) when is_binary(Prop) ->
    case maps:get(Prop, Bindings, undefined) of
        undefined ->
            try
                {ok, maps:get(binary_to_existing_atom(Prop, utf8), Bindings)}
            catch
                error:{badkey, _} ->
                    {error, undefined};
                error:badarg ->
                    {error, undefined}
            end;
        Value ->
            {ok, Value}
    end.

-spec to_string(binding()) ->
    unicode:chardata().
to_string(Bin) when is_binary(Bin) -> Bin;
to_string(Num) when is_integer(Num) -> integer_to_binary(Num);
to_string(Num) when is_float(Num) -> float_to_binary(Num, [{decimals, 10}, compact]);
to_string(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_string(Map) when is_map(Map) -> emqx_utils_json:encode(Map);
to_string(List) when is_list(List) ->
    case io_lib:printable_unicode_list(List) of
        true -> List;
        false -> emqx_utils_json:encode(List)
    end.

character_segments_to_binary(StringSegments) ->
    iolist_to_binary(
        lists:map(
            fun
                ($$) ->
                    $$;
                (Bin) when is_binary(Bin) -> Bin;
                (Chars) when is_list(Chars) ->
                    case unicode:characters_to_binary(Chars) of
                        Bin when is_binary(Bin) -> Bin;
                        _ -> emqx_utils_json:encode(Chars)
                    end
            end,
            StringSegments
        )
    ).
