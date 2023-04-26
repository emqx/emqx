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

-module(emqx_connector_template).

-include_lib("emqx/include/emqx_placeholder.hrl").

-export([parse/1]).
-export([parse/2]).
-export([parse_deep/1]).
-export([parse_deep/2]).
-export([validate/2]).
-export([trivial/1]).
-export([unparse/1]).
-export([render/2]).
-export([render/3]).
-export([render_strict/2]).
-export([render_strict/3]).

-export([lookup_var/2]).
-export([to_string/1]).

-export_type([t/0]).
-export_type([str/0]).
-export_type([deep/0]).
-export_type([placeholder/0]).
-export_type([varname/0]).
-export_type([bindings/0]).

-type t() :: str() | {'$tpl', deeptpl()}.

-type str() :: [unicode:chardata() | placeholder()].
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

-type scalar() :: atom() | unicode:chardata() | number().
-type binding() :: scalar() | list(scalar()) | bindings().
-type bindings() :: #{atom() | binary() => binding()}.

-type var_trans() ::
    fun((Value :: term()) -> unicode:chardata())
    | fun((varname(), Value :: term()) -> unicode:chardata()).

-type parse_opts() :: #{
    strip_double_quote => boolean()
}.

-type render_opts() :: #{
    var_trans => var_trans()
}.

-define(RE_PLACEHOLDER, "\\$(\\$?)\\{[.]?([a-zA-Z0-9._]*)\\}").

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
                <<"((?|" ?RE_PLACEHOLDER "|\"" ?RE_PLACEHOLDER "\"))">>;
            #{} ->
                <<"(" ?RE_PLACEHOLDER ")">>
        end,
    Splits = re:split(String, RE, [{return, binary}, group, trim, unicode]),
    Components = lists:flatmap(fun parse_split/1, Splits),
    Components.

parse_split([Part, _PH, <<>>, Var]) ->
    % Regular placeholder
    prepend(Part, [{var, unicode:characters_to_list(Var), parse_accessor(Var)}]);
parse_split([Part, _PH = <<B1, $$, Rest/binary>>, <<"$">>, _]) ->
    % Escaped literal, take all but the second byte, which is always `$`.
    % Important to make a whole token starting with `$` so the `unparse/11`
    % function can distinguish escaped literals.
    prepend(Part, [<<B1, Rest/binary>>]);
parse_split([Tail]) ->
    [Tail].

prepend(<<>>, To) ->
    To;
prepend(Head, To) ->
    [Head | To].

parse_accessor(Var) ->
    case string:split(Var, <<".">>, all) of
        [<<>>] ->
            ?PH_VAR_THIS;
        Name ->
            % TODO: lowercase?
            Name
    end.

-spec validate([varname()], t()) ->
    ok | {error, [_Error :: {varname(), disallowed}]}.
validate(Allowed, Template) ->
    {_, Errors} = render(Template, #{}),
    {Used, _} = lists:unzip(Errors),
    case lists:usort(Used) -- Allowed of
        [] ->
            ok;
        Disallowed ->
            {error, [{Var, disallowed} || Var <- Disallowed]}
    end.

-spec trivial(t()) ->
    boolean().
trivial(Template) ->
    validate([], Template) == ok.

-spec unparse(t()) ->
    unicode:chardata().
unparse({'$tpl', Template}) ->
    unparse_deep(Template);
unparse(Template) ->
    unicode:characters_to_list(lists:map(fun unparse_part/1, Template)).

unparse_part({var, Name, _Accessor}) ->
    render_placeholder(Name);
unparse_part(Part = <<"${", _/binary>>) ->
    <<"$", Part/binary>>;
unparse_part(Part) ->
    Part.

render_placeholder(Name) ->
    "${" ++ Name ++ "}".

%% @doc Render a template with given bindings.
%% Returns a term with all placeholders replaced with values from bindings.
%% If one or more placeholders are not found in bindings, an error is returned.
%% By default, all binding values are converted to strings using `to_string/1`
%% function. Option `var_trans` can be used to override this behaviour.
-spec render(t(), bindings()) ->
    {term(), [_Error :: {varname(), undefined}]}.
render(Template, Bindings) ->
    render(Template, Bindings, #{}).

-spec render(t(), bindings(), render_opts()) ->
    {term(), [_Error :: {varname(), undefined}]}.
render(Template, Bindings, Opts) when is_list(Template) ->
    lists:mapfoldl(
        fun
            ({var, Name, Accessor}, EAcc) ->
                {String, Errors} = render_binding(Name, Accessor, Bindings, Opts),
                {String, Errors ++ EAcc};
            (String, EAcc) ->
                {String, EAcc}
        end,
        [],
        Template
    );
render({'$tpl', Template}, Bindings, Opts) ->
    render_deep(Template, Bindings, Opts).

render_binding(Name, Accessor, Bindings, Opts) ->
    case lookup_var(Accessor, Bindings) of
        {ok, Value} ->
            {render_value(Name, Value, Opts), []};
        {error, Reason} ->
            % TODO
            % Currently, it's not possible to distinguish between a missing value
            % and an atom `undefined` in `TransFun`.
            {render_value(Name, undefined, Opts), [{Name, Reason}]}
    end.

render_value(_Name, Value, #{var_trans := TransFun}) when is_function(TransFun, 1) ->
    TransFun(Value);
render_value(Name, Value, #{var_trans := TransFun}) when is_function(TransFun, 2) ->
    TransFun(Name, Value);
render_value(_Name, Value, #{}) ->
    to_string(Value).

-spec render_strict(t(), bindings()) ->
    unicode:chardata().
render_strict(Template, Bindings) ->
    render_strict(Template, Bindings, #{}).

-spec render_strict(t(), bindings(), render_opts()) ->
    unicode:chardata().
render_strict(Template, Bindings, Opts) ->
    case render(Template, Bindings, Opts) of
        {String, []} ->
            String;
        {_, Errors = [_ | _]} ->
            error(Errors, [unparse(Template), Bindings])
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

render_deep(Template, Bindings, Opts) when is_map(Template) ->
    maps:fold(
        fun(KT, VT, {Acc, Errors}) ->
            {K, KErrors} = render_deep(KT, Bindings, Opts),
            {V, VErrors} = render_deep(VT, Bindings, Opts),
            {Acc#{K => V}, KErrors ++ VErrors ++ Errors}
        end,
        {#{}, []},
        Template
    );
render_deep({list, Template}, Bindings, Opts) when is_list(Template) ->
    lists:mapfoldr(
        fun(T, Errors) ->
            {E, VErrors} = render_deep(T, Bindings, Opts),
            {E, VErrors ++ Errors}
        end,
        [],
        Template
    );
render_deep({tuple, Template}, Bindings, Opts) when is_list(Template) ->
    {Term, Errors} = render_deep({list, Template}, Bindings, Opts),
    {list_to_tuple(Term), Errors};
render_deep(Template, Bindings, Opts) when is_list(Template) ->
    {String, Errors} = render(Template, Bindings, Opts),
    {unicode:characters_to_binary(String), Errors};
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

-spec lookup_var(accessor(), bindings()) ->
    {ok, binding()} | {error, undefined}.
lookup_var(Var, Value) when Var == ?PH_VAR_THIS orelse Var == [] ->
    {ok, Value};
lookup_var([Prop | Rest], Bindings) ->
    case lookup(Prop, Bindings) of
        {ok, Value} ->
            lookup_var(Rest, Value);
        {error, Reason} ->
            {error, Reason}
    end.

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
to_string(undefined) ->
    [];
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
