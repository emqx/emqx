%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight).

-compile(inline).

%% APIs
-export([
    new/0,
    new/1,
    contain/2,
    lookup/2,
    insert/3,
    update/3,
    resize/2,
    delete/2,
    fold/3,
    values/1,
    to_list/1,
    to_list/2,
    size/1,
    max_size/1,
    is_full/1,
    is_empty/1,
    window/1
]).

-export_type([inflight/0]).

-type key() :: term().

-type max_size() :: pos_integer().

-opaque inflight() :: {inflight, max_size(), #{key() => term()}}.

-define(INFLIGHT(Map), {inflight, _MaxSize, Map}).

-define(INFLIGHT(MaxSize, Map), {inflight, MaxSize, (Map)}).

-spec new() -> inflight().
new() -> new(0).

-spec new(non_neg_integer()) -> inflight().
new(MaxSize) when MaxSize >= 0 ->
    ?INFLIGHT(MaxSize, #{}).

-spec contain(key(), inflight()) -> boolean().
contain(Key, ?INFLIGHT(Map)) ->
    is_map_key(Key, Map).

-spec lookup(key(), inflight()) -> {value, term()} | none.
lookup(Key, ?INFLIGHT(Map)) ->
    case Map of
        #{Key := Val} -> {value, Val};
        #{} -> none
    end.

-spec insert(key(), Val :: term(), inflight()) -> inflight().
insert(Key, _Val, ?INFLIGHT(Map)) when is_map_key(Key, Map) ->
    erlang:error({key_exists, Key});
insert(Key, Val, ?INFLIGHT(MaxSize, Map)) ->
    ?INFLIGHT(MaxSize, Map#{Key => Val}).

-spec delete(key(), inflight()) -> inflight().
delete(Key, ?INFLIGHT(MaxSize, Map)) when is_map_key(Key, Map) ->
    ?INFLIGHT(MaxSize, maps:remove(Key, Map)).

-spec update(key(), Val :: term(), inflight()) -> inflight().
update(Key, Val, ?INFLIGHT(MaxSize, Map)) when is_map_key(Key, Map) ->
    ?INFLIGHT(MaxSize, Map#{Key := Val}).

-spec fold(fun((key(), Val :: term(), Acc) -> Acc), Acc, inflight()) -> Acc.
fold(FoldFun, AccIn, ?INFLIGHT(Map)) ->
    maps:fold(FoldFun, AccIn, Map).

-spec resize(integer(), inflight()) -> inflight().
resize(MaxSize, ?INFLIGHT(_, Map)) ->
    ?INFLIGHT(MaxSize, Map).

-spec is_full(inflight()) -> boolean().
is_full(?INFLIGHT(0, _Map)) ->
    false;
is_full(?INFLIGHT(MaxSize, Map)) ->
    MaxSize =< map_size(Map).

-spec is_empty(inflight()) -> boolean().
is_empty(?INFLIGHT(Map)) ->
    map_size(Map) =:= 0.

-doc "Return the values, ordered by key.".
-spec values(inflight()) -> list().
values(Inflight) ->
    [Val || {_Key, Val} <- to_list(Inflight)].

-doc "Return the entries, ordered by key.".
-spec to_list(inflight()) -> list({key(), term()}).
to_list(?INFLIGHT(Map)) ->
    lists:sort(maps:to_list(Map)).

-spec to_list(fun(), inflight()) -> list({key(), term()}).
to_list(SortFun, ?INFLIGHT(Map)) ->
    lists:sort(SortFun, maps:to_list(Map)).

-doc "Return the smallest and the largest key, or `[]` when empty.".
-spec window(inflight()) -> list().
window(?INFLIGHT(Map)) when map_size(Map) =:= 0 ->
    [];
window(?INFLIGHT(Map)) ->
    Keys = maps:keys(Map),
    [lists:min(Keys), lists:max(Keys)].

-spec size(inflight()) -> non_neg_integer().
size(?INFLIGHT(Map)) ->
    map_size(Map).

-spec max_size(inflight()) -> non_neg_integer().
max_size(?INFLIGHT(MaxSize, _Map)) ->
    MaxSize.
