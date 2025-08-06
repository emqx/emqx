%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub_buffer).

-moduledoc """
Buffer of messages received from the Message Queue consumer by a channel
""".

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_mq_internal.hrl").

-export([
    new/0,
    take/2,
    add/2,
    size/1
]).

-type key() :: {non_neg_integer(), emqx_ds:slab()}.

-type t() :: gb_trees:tree(key(), emqx_types:msg()).

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    gb_trees:empty().

-spec add(t(), emqx_types:msg()) -> t().
add(Tree, Msg) ->
    {Slab, SlabMessageId} = emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg),
    gb_trees:insert({SlabMessageId, Slab}, Msg, Tree).

-spec take(t(), non_neg_integer()) -> {[{emqx_mq_types:message_id(), emqx_types:msg()}], t()}.
take(Tree, N) ->
    do_take(Tree, N, []).

-spec size(t()) -> non_neg_integer().
size(Tree) ->
    gb_trees:size(Tree).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_take(Tree, 0, Acc) ->
    {lists:reverse(Acc), Tree};
do_take(Tree, N, Acc) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {lists:reverse(Acc), Tree};
        false ->
            {{SlabMessageId, Slab}, Msg, Tree2} = gb_trees:take_smallest(Tree),
            do_take(Tree2, N - 1, [{{Slab, SlabMessageId}, Msg} | Acc])
    end.
