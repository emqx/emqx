%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_source_registry).

-export([
    create/0,
    register/2,
    unregister/1,
    registered_types/0,
    module/1
]).

-define(TAB, ?MODULE).

-type type() :: emqx_authz_source:source_type().

-record(source, {
    type :: type(),
    module :: module()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create() -> ok.
create() ->
    _ = ets:new(?TAB, [named_table, public, set, {keypos, #source.type}]),
    ok.

-spec register(type(), module()) -> ok | {error, term()}.
register(Type, Module) when is_atom(Type) andalso is_atom(Module) ->
    case ets:insert_new(?TAB, #source{type = Type, module = Module}) of
        true ->
            ok;
        false ->
            {error, {already_registered, Type}}
    end.

-spec unregister(type()) -> ok.
unregister(Type) when is_atom(Type) ->
    _ = ets:delete(?TAB, Type),
    ok.

-spec registered_types() -> [type()].
registered_types() ->
    Types = lists:map(
        fun(#source{type = Type}) -> Type end,
        ets:tab2list(?TAB)
    ),
    lists:sort(Types).

-spec module(type()) -> module() | no_return().
module(Type) ->
    case ets:lookup(?TAB, Type) of
        [] ->
            throw({unknown_authz_source_type, Type});
        [#source{module = Module}] ->
            Module
    end.
