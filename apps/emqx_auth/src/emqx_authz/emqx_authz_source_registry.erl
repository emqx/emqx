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

-module(emqx_authz_source_registry).

-export([
    create/0,
    register/2,
    unregister/1,
    get/0,
    get/1,
    module/1
]).

-define(TAB, ?MODULE).

-type fuzzy_type() :: emqx_authz_source:source_type() | binary().

-spec create() -> ok.
create() ->
    _ = ets:new(?TAB, [named_table, public, set]),
    ok.

-spec register(emqx_authz_source:source_type(), module()) -> ok | {error, term()}.
register(Type, Module) when is_atom(Type) andalso is_atom(Module) ->
    case ets:insert_new(?TAB, {Type, Type, Module}) of
        true ->
            _ = ets:insert(?TAB, {atom_to_binary(Type), Type, Module}),
            ok;
        false ->
            {error, {already_registered, Type}}
    end.

-spec unregister(emqx_authz_source:source_type()) -> ok.
unregister(Type) when is_atom(Type) ->
    _ = ets:delete(?TAB, Type),
    _ = ets:delete(?TAB, atom_to_binary(Type)),
    ok.

-spec get(fuzzy_type()) ->
    emqx_authz_source:source_type() | no_return().
get(FuzzyType) when is_atom(FuzzyType) orelse is_binary(FuzzyType) ->
    case ets:lookup(?TAB, FuzzyType) of
        [] ->
            throw({unknown_authz_source_type, FuzzyType});
        [{FuzzyType, Type, _Module}] ->
            Type
    end.

-spec get() -> [emqx_authz_source:source_type()].
get() ->
    Types = lists:map(
        fun({_, Type, _}) -> Type end,
        ets:tab2list(?TAB)
    ),
    lists:usort(Types).

-spec module(fuzzy_type()) -> module() | no_return().
module(FuzzyType) when is_atom(FuzzyType) orelse is_binary(FuzzyType) ->
    case ets:lookup(?TAB, FuzzyType) of
        [] ->
            throw({unknown_authz_source_type, FuzzyType});
        [{FuzzyType, _Type, Module}] ->
            Module
    end.
