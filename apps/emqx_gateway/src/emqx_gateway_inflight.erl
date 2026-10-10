%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_inflight).

-moduledoc """
Bounded map of frames awaiting acknowledgement, keyed by a protocol-defined
term. Used by gateway channels that retransmit frames. The MQTT session uses
`emqx_inflight`, which is keyed by packet id and allocates them.
""".

-compile(inline).

%% APIs
-export([
    new/0,
    new/1,
    contain/2,
    insert/3,
    update/3,
    delete/2,
    to_list/1,
    size/1,
    max_size/1,
    is_full/1,
    is_empty/1
]).

-export_type([inflight/0]).

-type key() :: term().

-type max_size() :: non_neg_integer().

-opaque inflight() :: {inflight, max_size(), #{key() => term()}}.

-define(INFLIGHT(Map), {inflight, _MaxSize, Map}).

-define(INFLIGHT(MaxSize, Map), {inflight, MaxSize, (Map)}).

-spec new() -> inflight().
new() -> new(0).

-doc "A `MaxSize` of 0 means no limit.".
-spec new(max_size()) -> inflight().
new(MaxSize) when MaxSize >= 0 ->
    ?INFLIGHT(MaxSize, #{}).

-spec contain(key(), inflight()) -> boolean().
contain(Key, ?INFLIGHT(Map)) ->
    is_map_key(Key, Map).

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

-spec is_full(inflight()) -> boolean().
is_full(?INFLIGHT(0, _Map)) ->
    false;
is_full(?INFLIGHT(MaxSize, Map)) ->
    MaxSize =< map_size(Map).

-spec is_empty(inflight()) -> boolean().
is_empty(?INFLIGHT(Map)) ->
    map_size(Map) =:= 0.

-doc "Return the entries, ordered by key.".
-spec to_list(inflight()) -> list({key(), term()}).
to_list(?INFLIGHT(Map)) ->
    lists:keysort(1, maps:to_list(Map)).

-spec size(inflight()) -> non_neg_integer().
size(?INFLIGHT(Map)) ->
    map_size(Map).

-spec max_size(inflight()) -> max_size().
max_size(?INFLIGHT(MaxSize, _Map)) ->
    MaxSize.
