%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_transform).

-include_lib("syntax_tools/include/merl.hrl").

-export([parse_transform/2]).

parse_transform(Forms, _Opts) ->
    Mod = hd([M || {attribute, _, module, M} <- Forms]),
    AST = trans(Mod, proplists:delete(eof, Forms)),
    debug_print(Mod, AST),
    AST.

-ifdef(RESOURCE_DEBUG).

debug_print(Mod, Ts) ->
    {ok, Io} = file:open("./" ++ atom_to_list(Mod) ++ ".trans.erl", [write]),
    do_debug_print(Io, Ts),
    file:close(Io).

do_debug_print(Io, Ts) when is_list(Ts) ->
    lists:foreach(fun(T) -> do_debug_print(Io, T) end, Ts);
do_debug_print(Io, T) ->
    io:put_chars(Io, erl_prettypr:format(merl:tree(T))),
    io:nl(Io).
-else.
debug_print(_Mod, _AST) ->
    ok.
-endif.

trans(Mod, Forms) ->
    forms(Mod, Forms) ++ [erl_syntax:revert(erl_syntax:eof_marker())].

forms(Mod, [F0 | Fs0]) ->
    case form(Mod, F0) of
        {CurrForm, AppendedForms} ->
            CurrForm ++ forms(Mod, Fs0) ++ AppendedForms;
        {AHeadForms, CurrForm, AppendedForms} ->
            AHeadForms ++ CurrForm ++ forms(Mod, Fs0) ++ AppendedForms
    end;
forms(_, []) -> [].

form(Mod, Form) ->
    case Form of
        ?Q("-emqx_resource_api_path('@Path').") ->
            {fix_spec_attrs() ++ fix_api_attrs(Mod, erl_syntax:concrete(Path))
             ++ fix_api_exports(),
             [],
             fix_spec_funcs(Mod) ++ fix_api_funcs(Mod)};
        _ ->
            %io:format("---other form: ~p~n", [Form]),
            {[], [Form], []}
    end.

fix_spec_attrs() ->
    [ ?Q("-export([emqx_resource_schema/0]).")
    , ?Q("-export([structs/0]).")
    , ?Q("-behaviour(hocon_schema).")
    ].
fix_spec_funcs(_Mod) ->
    [ (?Q("emqx_resource_schema() -> <<\"demo_swagger_schema\">>."))
    , ?Q("structs() -> [\"config\"].")
    ].

fix_api_attrs(Mod, Path) ->
    BaseName = atom_to_list(Mod),
    [erl_syntax:revert(
        erl_syntax:attribute(?Q("rest_api"), [
            erl_syntax:abstract(#{
                name => list_to_atom(Act ++ "_" ++ BaseName),
                method => Method,
                path => mk_path(Path, WithId),
                func => Func,
                descr => Act ++ " the " ++ BaseName})]))
       || {Act, Method, WithId, Func} <- [
            {"list", 'GET', noid, api_get_all},
            {"get", 'GET', id, api_get},
            {"update", 'PUT', id, api_put},
            {"delete", 'DELETE', id, api_delete}]].

fix_api_exports() ->
    [?Q("-export([api_get_all/2, api_get/2, api_put/2, api_delete/2]).")].

fix_api_funcs(Mod) ->
    [erl_syntax:revert(?Q(
        "api_get_all(Binding, Params) ->
            emqx_resource_api:get_all('@Mod@', Binding, Params).")),
     erl_syntax:revert(?Q(
        "api_get(Binding, Params) ->
            emqx_resource_api:get('@Mod@', Binding, Params).")),
     erl_syntax:revert(?Q(
        "api_put(Binding, Params) ->
            emqx_resource_api:put('@Mod@', Binding, Params).")),
     erl_syntax:revert(?Q(
        "api_delete(Binding, Params) ->
            emqx_resource_api:delete('@Mod@', Binding, Params)."))
    ].

mk_path(Path, id) -> string:trim(Path, trailing, "/") ++ "/:bin:id";
mk_path(Path, noid) -> Path.
