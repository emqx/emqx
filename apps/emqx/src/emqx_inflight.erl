%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight).

-compile(inline).

-include("emqx.hrl").

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
    bytes_size/1,
    max_size/1,
    is_full/1,
    is_empty/1,
    window/1
]).

-export_type([inflight/0]).

-type key() :: term().

-type max_size() :: pos_integer().

-opaque inflight() :: {inflight, max_size(), non_neg_integer(), gb_trees:tree()}.

-define(INFLIGHT(Tree), {inflight, _MaxSize, _BytesSize, Tree}).

-define(INFLIGHT(MaxSize, BytesSize, Tree), {inflight, MaxSize, BytesSize, (Tree)}).

-spec new() -> inflight().
new() -> new(0).

-spec new(non_neg_integer()) -> inflight().
new(MaxSize) when MaxSize >= 0 ->
    ?INFLIGHT(MaxSize, 0, gb_trees:empty()).

-spec contain(key(), inflight()) -> boolean().
contain(Key, ?INFLIGHT(Tree)) ->
    gb_trees:is_defined(Key, Tree).

-spec lookup(key(), inflight()) -> {value, term()} | none.
lookup(Key, ?INFLIGHT(Tree)) ->
    gb_trees:lookup(Key, Tree).

-spec insert(key(), Val :: term(), inflight()) -> inflight().
insert(Key, Val, ?INFLIGHT(MaxSize, BytesSize, Tree)) ->
    ?INFLIGHT(MaxSize, BytesSize + val_bytes(Val), gb_trees:insert(Key, Val, Tree)).

-spec delete(key(), inflight()) -> inflight().
delete(Key, ?INFLIGHT(MaxSize, BytesSize, Tree)) ->
    Val = gb_trees:get(Key, Tree),
    ?INFLIGHT(MaxSize, dec_bytes(BytesSize, Val), gb_trees:delete(Key, Tree)).

-spec update(key(), Val :: term(), inflight()) -> inflight().
update(Key, Val, ?INFLIGHT(MaxSize, BytesSize, Tree)) ->
    OldVal = gb_trees:get(Key, Tree),
    BytesSize1 = dec_bytes(BytesSize, OldVal) + val_bytes(Val),
    ?INFLIGHT(MaxSize, BytesSize1, gb_trees:update(Key, Val, Tree)).

-spec fold(fun((key(), Val :: term(), Acc) -> Acc), Acc, inflight()) -> Acc.
fold(FoldFun, AccIn, ?INFLIGHT(Tree)) ->
    fold_iterator(FoldFun, AccIn, gb_trees:iterator(Tree)).

fold_iterator(FoldFun, Acc, It) ->
    case gb_trees:next(It) of
        {Key, Val, ItNext} ->
            fold_iterator(FoldFun, FoldFun(Key, Val, Acc), ItNext);
        none ->
            Acc
    end.

-spec resize(integer(), inflight()) -> inflight().
resize(MaxSize, ?INFLIGHT(_OldMaxSize, BytesSize, Tree)) ->
    ?INFLIGHT(MaxSize, BytesSize, Tree).

-spec is_full(inflight()) -> boolean().
is_full(?INFLIGHT(0, _BytesSize, _Tree)) ->
    false;
is_full(?INFLIGHT(MaxSize, _BytesSize, Tree)) ->
    MaxSize =< gb_trees:size(Tree).

-spec is_empty(inflight()) -> boolean().
is_empty(?INFLIGHT(Tree)) ->
    gb_trees:is_empty(Tree).

-spec smallest(inflight()) -> {key(), term()}.
smallest(?INFLIGHT(Tree)) ->
    gb_trees:smallest(Tree).

-spec largest(inflight()) -> {key(), term()}.
largest(?INFLIGHT(Tree)) ->
    gb_trees:largest(Tree).

-spec values(inflight()) -> list().
values(?INFLIGHT(Tree)) ->
    gb_trees:values(Tree).

-spec to_list(inflight()) -> list({key(), term()}).
to_list(?INFLIGHT(Tree)) ->
    gb_trees:to_list(Tree).

-spec to_list(fun(), inflight()) -> list({key(), term()}).
to_list(SortFun, ?INFLIGHT(Tree)) ->
    lists:sort(SortFun, gb_trees:to_list(Tree)).

-spec window(inflight()) -> list().
window(Inflight = ?INFLIGHT(Tree)) ->
    case gb_trees:is_empty(Tree) of
        true -> [];
        false -> [Key || {Key, _Val} <- [smallest(Inflight), largest(Inflight)]]
    end.

-spec size(inflight()) -> non_neg_integer().
size(?INFLIGHT(Tree)) ->
    gb_trees:size(Tree).

-spec bytes_size(inflight()) -> non_neg_integer().
bytes_size(?INFLIGHT(_MaxSize, BytesSize, _Tree)) ->
    BytesSize.

-spec max_size(inflight()) -> non_neg_integer().
max_size(?INFLIGHT(MaxSize, _BytesSize, _Tree)) ->
    MaxSize.

dec_bytes(BytesSize, Val) ->
    BytesSize1 = BytesSize - val_bytes(Val),
    true = BytesSize1 >= 0,
    BytesSize1.

val_bytes({inflight_data, _Phase, #message{} = Msg, _Timestamp}) ->
    emqx_message:payload_size(Msg);
val_bytes(#message{} = Msg) ->
    emqx_message:payload_size(Msg);
val_bytes(_Val) ->
    0.
