%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_cache).

-include("emqx_access_control.hrl").

-export([
    list_authz_cache/0,
    get_authz_cache/2,
    put_authz_cache/3,
    cleanup_authz_cache/0,
    empty_authz_cache/0,
    get_cache_max_size/0,
    get_cache_ttl/0,
    is_enabled/1,
    drain_cache/0,
    drain_cache/1
]).

%% export for test
-export([
    cache_k/2,
    cache_v/1,
    get_cache_size/0,
    get_newest_key/0,
    get_oldest_key/0
]).

-type authz_result() :: allow | deny.
-type system_time() :: integer().
-type cache_key() :: {emqx_types:pubsub(), emqx_types:topic()}.
-type cache_val() :: {authz_result(), system_time()}.

-type authz_cache_entry() :: {cache_key(), cache_val()}.

%% Wrappers for key and value
cache_k(PubSub, Topic) -> {PubSub, Topic}.
cache_v(AuthzResult) -> {AuthzResult, time_now()}.
drain_k() -> {?MODULE, drain_timestamp}.

%% @doc Check if the authz cache is enabled for the given topic.
-spec is_enabled(emqx_types:topic()) -> boolean().
is_enabled(Topic) ->
    case emqx:get_config([authorization, cache]) of
        #{enable := true, excludes := Filters} when Filters =/= [] ->
            not is_excluded(Topic, Filters);
        #{enable := IsEnabled} ->
            IsEnabled
    end.

is_excluded(_Topic, []) ->
    false;
is_excluded(Topic, [Filter | Filters]) ->
    emqx_topic:match(Topic, Filter) orelse is_excluded(Topic, Filters).

-spec get_cache_max_size() -> integer().
get_cache_max_size() ->
    emqx:get_config([authorization, cache, max_size]).

-spec get_cache_ttl() -> integer().
get_cache_ttl() ->
    emqx:get_config([authorization, cache, ttl]).

-spec list_authz_cache() -> [authz_cache_entry()].
list_authz_cache() ->
    cleanup_authz_cache(),
    map_authz_cache(fun(Cache) -> Cache end).

%% We'll cleanup the cache before replacing an expired authz.
-spec get_authz_cache(emqx_types:pubsub(), emqx_types:topic()) ->
    authz_result() | not_found.
get_authz_cache(PubSub, Topic) ->
    case erlang:get(cache_k(PubSub, Topic)) of
        undefined ->
            not_found;
        {AuthzResult, CachedAt} ->
            if_expired(
                get_cache_ttl(),
                CachedAt,
                fun
                    (false) ->
                        AuthzResult;
                    (true) ->
                        cleanup_authz_cache(),
                        not_found
                end
            )
    end.

%% If the cache get full, and also the latest one
%%   is expired, then delete all the cache entries
-spec put_authz_cache(emqx_types:pubsub(), emqx_types:topic(), authz_result()) ->
    ok.
put_authz_cache(PubSub, Topic, AuthzResult) ->
    MaxSize = get_cache_max_size(),
    true = (MaxSize =/= 0),
    Size = get_cache_size(),
    case Size < MaxSize of
        true ->
            add_authz(PubSub, Topic, AuthzResult);
        false ->
            NewestK = get_newest_key(),
            {_AuthzResult, CachedAt} = erlang:get(NewestK),
            if_expired(
                get_cache_ttl(),
                CachedAt,
                fun
                    (true) ->
                        % all cache expired, cleanup first
                        empty_authz_cache(),
                        add_authz(PubSub, Topic, AuthzResult);
                    (false) ->
                        % cache full, perform cache replacement
                        evict_authz_cache(),
                        add_authz(PubSub, Topic, AuthzResult)
                end
            )
    end.

%% delete all the authz entries
-spec empty_authz_cache() -> ok.
empty_authz_cache() ->
    foreach_authz_cache(fun({CacheK, _CacheV}) -> erlang:erase(CacheK) end),
    set_cache_size(0),
    keys_queue_set(queue:new()).

%% delete the oldest authz entry
-spec evict_authz_cache() -> ok.
evict_authz_cache() ->
    OldestK = keys_queue_out(),
    erlang:erase(OldestK),
    decr_cache_size().

%% cleanup all the expired cache entries
-spec cleanup_authz_cache() -> ok.
cleanup_authz_cache() ->
    keys_queue_set(
        cleanup_authz(get_cache_ttl(), keys_queue_get())
    ).

get_oldest_key() ->
    keys_queue_pick(queue_front()).
get_newest_key() ->
    keys_queue_pick(queue_rear()).

get_cache_size() ->
    case erlang:get(authz_cache_size) of
        undefined -> 0;
        Size -> Size
    end.

map_authz_cache(Fun) ->
    map_authz_cache(Fun, erlang:get()).

map_authz_cache(Fun, Dict) ->
    [
        Fun(R)
     || R = {{?authz_action, _T}, _Authz} <- Dict
    ].

foreach_authz_cache(Fun) ->
    _ = map_authz_cache(Fun),
    ok.

%% All authz cache entries added before `drain_cache()` invocation will become expired
drain_cache() ->
    _ = persistent_term:put(drain_k(), time_now()),
    ok.

-spec drain_cache(emqx_types:clientid()) -> ok | {error, not_found}.
drain_cache(ClientId) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            {error, not_found};
        Pids when is_list(Pids) ->
            erlang:send(lists:last(Pids), clean_authz_cache),
            ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_authz(PubSub, Topic, AuthzResult) ->
    K = cache_k(PubSub, Topic),
    V = cache_v(AuthzResult),
    case erlang:get(K) of
        undefined -> add_new_authz(K, V);
        {_AuthzResult, _CachedAt} -> update_authz(K, V)
    end.

add_new_authz(K, V) ->
    erlang:put(K, V),
    keys_queue_in(K),
    incr_cache_size().

update_authz(K, V) ->
    erlang:put(K, V),
    keys_queue_update(K).

cleanup_authz(TTL, KeysQ) ->
    case queue:out(KeysQ) of
        {{value, OldestK}, KeysQ2} ->
            {_AuthzResult, CachedAt} = erlang:get(OldestK),
            if_expired(
                TTL,
                CachedAt,
                fun
                    (false) ->
                        KeysQ;
                    (true) ->
                        erlang:erase(OldestK),
                        decr_cache_size(),
                        cleanup_authz(TTL, KeysQ2)
                end
            );
        {empty, KeysQ} ->
            KeysQ
    end.

incr_cache_size() ->
    erlang:put(authz_cache_size, get_cache_size() + 1),
    ok.
decr_cache_size() ->
    Size = get_cache_size(),
    case Size > 1 of
        true ->
            erlang:put(authz_cache_size, Size - 1);
        false ->
            erlang:put(authz_cache_size, 0)
    end,
    ok.

set_cache_size(N) ->
    erlang:put(authz_cache_size, N),
    ok.

%%% Ordered Keys Q %%%
keys_queue_in(Key) ->
    %% delete the key first if exists
    KeysQ = keys_queue_get(),
    keys_queue_set(queue:in(Key, KeysQ)).

keys_queue_out() ->
    case queue:out(keys_queue_get()) of
        {{value, OldestK}, Q2} ->
            keys_queue_set(Q2),
            OldestK;
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
    queue:filter(
        fun
            (K) when K =:= Key -> false;
            (_) -> true
        end,
        KeysQ
    ).

keys_queue_set(KeysQ) ->
    erlang:put(authz_keys_q, KeysQ),
    ok.
keys_queue_get() ->
    case erlang:get(authz_keys_q) of
        undefined -> queue:new();
        KeysQ -> KeysQ
    end.

queue_front() -> fun queue:get/1.
queue_rear() -> fun queue:get_r/1.

time_now() -> erlang:system_time(millisecond).

if_expired(TTL, CachedAt, Fun) ->
    Now = time_now(),
    CurrentEvictTimestamp = persistent_term:get(drain_k(), 0),
    case CachedAt =< CurrentEvictTimestamp orelse (CachedAt + TTL) =< Now of
        true -> Fun(true);
        false -> Fun(false)
    end.
