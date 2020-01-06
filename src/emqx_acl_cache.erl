%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_cache).

-include("emqx.hrl").

-export([ list_acl_cache/0
        , get_acl_cache/2
        , put_acl_cache/3
        , cleanup_acl_cache/0
        , empty_acl_cache/0
        , dump_acl_cache/0
        , get_cache_max_size/0
        , get_cache_ttl/0
        , is_enabled/0
        ]).

%% export for test
-export([ cache_k/2
        , cache_v/1
        , get_cache_size/0
        , get_newest_key/0
        , get_oldest_key/0
        ]).

-type(acl_result() :: allow | deny).
-type(system_time() :: integer()).
-type(cache_key() :: {emqx_types:pubsub(), emqx_types:topic()}).
-type(cache_val() :: {acl_result(), system_time()}).

-type(acl_cache_entry() :: {cache_key(), cache_val()}).

%% Wrappers for key and value
cache_k(PubSub, Topic)-> {PubSub, Topic}.
cache_v(AclResult)-> {AclResult, time_now()}.

-spec(is_enabled() -> boolean()).
is_enabled() ->
    application:get_env(emqx, enable_acl_cache, true).

-spec(get_cache_max_size() -> integer()).
get_cache_max_size() ->
    application:get_env(emqx, acl_cache_max_size, 32).

-spec(get_cache_ttl() -> integer()).
get_cache_ttl() ->
     application:get_env(emqx, acl_cache_ttl, 60000).

-spec(list_acl_cache() -> [acl_cache_entry()]).
list_acl_cache() ->
    cleanup_acl_cache(),
    map_acl_cache(fun(Cache) -> Cache end).

%% We'll cleanup the cache before replacing an expired acl.
-spec(get_acl_cache(emqx_types:pubsub(), emqx_topic:topic()) -> (acl_result() | not_found)).
get_acl_cache(PubSub, Topic) ->
    case erlang:get(cache_k(PubSub, Topic)) of
        undefined -> not_found;
        {AclResult, CachedAt} ->
            if_expired(CachedAt,
                fun(false) ->
                      AclResult;
                   (true) ->
                      cleanup_acl_cache(),
                      not_found
                end)
    end.

%% If the cache get full, and also the latest one
%%   is expired, then delete all the cache entries
-spec(put_acl_cache(emqx_types:pubsub(), emqx_topic:topic(), acl_result()) -> ok).
put_acl_cache(PubSub, Topic, AclResult) ->
    MaxSize = get_cache_max_size(), true = (MaxSize =/= 0),
    Size = get_cache_size(),
    if
        Size < MaxSize ->
            add_acl(PubSub, Topic, AclResult);
        Size =:= MaxSize ->
            NewestK = get_newest_key(),
            {_AclResult, CachedAt} = erlang:get(NewestK),
            if_expired(CachedAt,
                fun(true) ->
                      % all cache expired, cleanup first
                      empty_acl_cache(),
                      add_acl(PubSub, Topic, AclResult);
                   (false) ->
                      % cache full, perform cache replacement
                      evict_acl_cache(),
                      add_acl(PubSub, Topic, AclResult)
                end)
    end.

%% delete all the acl entries
-spec(empty_acl_cache() -> ok).
empty_acl_cache() ->
    map_acl_cache(fun({CacheK, _CacheV}) ->
            erlang:erase(CacheK)
        end),
    set_cache_size(0),
    keys_queue_set(queue:new()).

%% delete the oldest acl entry
-spec(evict_acl_cache() -> ok).
evict_acl_cache() ->
    OldestK = keys_queue_out(),
    erlang:erase(OldestK),
    decr_cache_size().

%% cleanup all the expired cache entries
-spec(cleanup_acl_cache() -> ok).
cleanup_acl_cache() ->
    keys_queue_set(
        cleanup_acl(keys_queue_get())).

get_oldest_key() ->
    keys_queue_pick(queue_front()).
get_newest_key() ->
    keys_queue_pick(queue_rear()).

get_cache_size() ->
    case erlang:get(acl_cache_size) of
        undefined -> 0;
        Size -> Size
    end.

dump_acl_cache() ->
    map_acl_cache(fun(Cache) -> Cache end).
map_acl_cache(Fun) ->
    [Fun(R) || R = {{SubPub, _T}, _Acl} <- get(), SubPub =:= publish
                                           orelse SubPub =:= subscribe].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_acl(PubSub, Topic, AclResult) ->
    K = cache_k(PubSub, Topic),
    V = cache_v(AclResult),
    case erlang:get(K) of
        undefined -> add_new_acl(K, V);
        {_AclResult, _CachedAt} ->
            update_acl(K, V)
    end.

add_new_acl(K, V) ->
    erlang:put(K, V),
    keys_queue_in(K),
    incr_cache_size().

update_acl(K, V) ->
    erlang:put(K, V),
    keys_queue_update(K).

cleanup_acl(KeysQ) ->
    case queue:out(KeysQ) of
        {{value, OldestK}, KeysQ2} ->
            {_AclResult, CachedAt} = erlang:get(OldestK),
            if_expired(CachedAt,
                fun(false) -> KeysQ;
                   (true) ->
                      erlang:erase(OldestK),
                      decr_cache_size(),
                      cleanup_acl(KeysQ2)
                end);
        {empty, KeysQ} -> KeysQ
    end.

incr_cache_size() ->
    erlang:put(acl_cache_size, get_cache_size() + 1), ok.
decr_cache_size() ->
    Size = get_cache_size(),
    if Size > 1 ->
          erlang:put(acl_cache_size, Size-1);
       Size =< 1 ->
          erlang:put(acl_cache_size, 0)
    end, ok.
set_cache_size(N) ->
    erlang:put(acl_cache_size, N), ok.

%%% Ordered Keys Q %%%
keys_queue_in(Key) ->
    %% delete the key first if exists
    KeysQ = keys_queue_get(),
    keys_queue_set(queue:in(Key, KeysQ)).

keys_queue_out() ->
    case queue:out(keys_queue_get()) of
        {{value, OldestK}, Q2} ->
            keys_queue_set(Q2), OldestK;
        {empty, _Q} ->
            undefined
    end.

keys_queue_update(Key) ->
    NewKeysQ = keys_queue_remove(Key, keys_queue_get()),
    keys_queue_set(queue:in(Key, NewKeysQ)).

keys_queue_pick(Pick) ->
    KeysQ = keys_queue_get(),
    case queue:is_empty(KeysQ) of
        true -> undefined;
        false -> Pick(KeysQ)
    end.

keys_queue_remove(Key, KeysQ) ->
    queue:filter(fun
        (K) when K =:= Key -> false; (_) -> true
      end, KeysQ).

keys_queue_set(KeysQ) ->
    erlang:put(acl_keys_q, KeysQ), ok.
keys_queue_get() ->
    case erlang:get(acl_keys_q) of
        undefined -> queue:new();
        KeysQ -> KeysQ
    end.

queue_front() -> fun queue:get/1.
queue_rear() -> fun queue:get_r/1.

time_now() -> erlang:system_time(millisecond).

if_expired(CachedAt, Fun) ->
    TTL = get_cache_ttl(),
    Now = time_now(),
    if (CachedAt + TTL) =< Now ->
           Fun(true);
       true ->
           Fun(false)
    end.
