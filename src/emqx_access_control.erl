%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).
-export([authenticate/2]).
-export([check_acl/3, reload_acl/0, lookup_mods/1]).
-export([register_mod/3, register_mod/4, unregister_mod/2]).
-export([stop/0]).

-export([get_acl_cache/2,
         put_acl_cache/3,
         cleanup_acl_cache/0,
         dump_acl_cache/0,
         get_cache_size/0,
         get_newest_key/0,
         get_oldest_key/0,
         cache_k/2,
         cache_v/1
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-type(password() :: undefined | binary()).
-type(acl_result() :: allow | deny).

-record(state, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start access control server.
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            ok = register_default_mod(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

register_default_mod() ->
    case emqx_config:get_env(acl_file) of
        undefined -> ok;
        File ->
            emqx_access_control:register_mod(acl, emqx_acl_internal, [File])
    end.

%% @doc Authenticate Client.
-spec(authenticate(Client :: client(), Password :: password())
      -> ok | {ok, boolean()} | {error, term()}).
authenticate(Client, Password) when is_record(Client, client) ->
    authenticate(Client, Password, lookup_mods(auth)).

authenticate(#client{zone = Zone}, _Password, []) ->
    case emqx_zone:get_env(Zone, allow_anonymous, false) of
        true  -> ok;
        false -> {error, "No auth module to check!"}
    end;

authenticate(Client, Password, [{Mod, State, _Seq} | Mods]) ->
    case catch Mod:check(Client, Password, State) of
        ok              -> ok;
        {ok, IsSuper}   -> {ok, IsSuper};
        ignore          -> authenticate(Client, Password, Mods);
        {error, Reason} -> {error, Reason};
        {'EXIT', Error} -> {error, Error}
    end.

%% @doc Check ACL
-spec(check_acl(client(), pubsub(), topic()) -> allow | deny).
check_acl(Client, PubSub, Topic) when ?PS(PubSub) ->
    CacheEnabled = (get_cache_max_size() =/= 0),
    check_acl(Client, PubSub, Topic, lookup_mods(acl), CacheEnabled).

check_acl(Client, PubSub, Topic, AclMods, false) ->
    check_acl_from_plugins(Client, PubSub, Topic, AclMods);
check_acl(Client, PubSub, Topic, AclMods, true) ->
    case get_acl_cache(PubSub, Topic) of
        not_found ->
            AclResult = check_acl_from_plugins(Client, PubSub, Topic, AclMods),
            put_acl_cache(PubSub, Topic, AclResult),
            AclResult;
        AclResult ->
            AclResult
    end.

%% @doc Reload ACL Rules.
-spec(reload_acl() -> list(ok | {error, already_existed})).
reload_acl() ->
    [Mod:reload_acl(State) || {Mod, State, _Seq} <- lookup_mods(acl)].

%% @doc Register Authentication or ACL module.
-spec(register_mod(auth | acl, atom(), list()) -> ok | {error, term()}).
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl ->
    register_mod(Type, Mod, Opts, 0).

-spec(register_mod(auth | acl, atom(), list(), non_neg_integer())
      -> ok | {error, term()}).
register_mod(Type, Mod, Opts, Seq) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts, Seq}).

%% @doc Unregister authentication or ACL module
-spec(unregister_mod(Type :: auth | acl, Mod :: atom())
      -> ok | {error, not_found | term()}).
unregister_mod(Type, Mod) when Type =:= auth; Type =:= acl ->
    gen_server:call(?SERVER, {unregister_mod, Type, Mod}).

%% @doc Lookup authentication or ACL modules.
-spec(lookup_mods(auth | acl) -> list()).
lookup_mods(Type) ->
    case ets:lookup(?TAB, tab_key(Type)) of
        [] -> [];
        [{_, Mods}] -> Mods
    end.

tab_key(auth) -> auth_modules;
tab_key(acl)  -> acl_modules.

%% @doc Stop access control server.
stop() ->
    gen_server:stop(?MODULE, normal, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:new(?TAB, [set, protected, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({register_mod, Type, Mod, Opts, Seq}, _From, State) ->
    Mods = lookup_mods(Type),
    Existed = lists:keyfind(Mod, 1, Mods),
    {reply, if_existed(Existed, fun() ->
                case catch Mod:init(Opts) of
                    {ok, ModState} ->
                        NewMods = lists:sort(fun({_, _, Seq1}, {_, _, Seq2}) ->
                                            Seq1 >= Seq2
                                    end, [{Mod, ModState, Seq} | Mods]),
                        ets:insert(?TAB, {tab_key(Type), NewMods}),
                        ok;
                    {error, Error} ->
                        {error, Error};
                    {'EXIT', Reason} ->
                        {error, Reason}
                end
            end), State};

handle_call({unregister_mod, Type, Mod}, _From, State) ->
    Mods = lookup_mods(Type),
    case lists:keyfind(Mod, 1, Mods) of
        false ->
            {reply, {error, not_found}, State};
        _ ->
            _ = ets:insert(?TAB, {tab_key(Type), lists:keydelete(Mod, 1, Mods)}),
            {reply, ok, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[AccessControl] unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[AccessControl] unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[AccessControl] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

check_acl_from_plugins(#client{zone = Zone}, _PubSub, _Topic, []) ->
    emqx_zone:get_env(Zone, acl_nomatch, deny);
check_acl_from_plugins(Client, PubSub, Topic, [{Mod, State, _Seq}|AclMods]) ->
    case Mod:check_acl({Client, PubSub, Topic}, State) of
        allow  -> allow;
        deny   -> deny;
        ignore -> check_acl_from_plugins(Client, PubSub, Topic, AclMods)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

if_existed(false, Fun) ->
    Fun();
if_existed(_Mod, _Fun) ->
    {error, already_existed}.

%%--------------------------------------------------------------------
%% ACL cache
%%--------------------------------------------------------------------

%% We'll cleanup the cache before repalcing an expired acl.
-spec(get_acl_cache(PubSub :: publish | subscribe, Topic :: topic())
        -> (acl_result() | not_found)).
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
-spec(put_acl_cache(PubSub :: publish | subscribe,
                    Topic :: topic(), AclResult :: acl_result()) -> ok).
put_acl_cache(PubSub, Topic, AclResult) ->
    MaxSize = get_cache_max_size(), true = (MaxSize =/= 0),
    Size = get_cache_size(),
    if
        Size < MaxSize ->
            add_acl_cache(PubSub, Topic, AclResult);
        Size =:= MaxSize ->
            NewestK = get_newest_key(),
            {_AclResult, CachedAt} = erlang:get(NewestK),
            if_expired(CachedAt,
                fun(true) ->
                      % all cache expired, cleanup first
                      empty_acl_cache(),
                      add_acl_cache(PubSub, Topic, AclResult);
                   (false) ->
                      % cache full, perform cache replacement
                      evict_acl_cache(),
                      add_acl_cache(PubSub, Topic, AclResult)
                end)
    end.

empty_acl_cache() ->
    map_acl_cache(fun({CacheK, _CacheV}) ->
            erlang:erase(CacheK)
        end),
    set_cache_size(0),
    set_keys_queue(queue:new()).

evict_acl_cache() ->
    {{value, OldestK}, RemKeys} = queue:out(get_keys_queue()),
    set_keys_queue(RemKeys),
    erlang:erase(OldestK),
    decr_cache_size().

add_acl_cache(PubSub, Topic, AclResult) ->
    K = cache_k(PubSub, Topic),
    V = cache_v(AclResult),
    case get(K) of
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

%% cleanup all the exipired cache entries
-spec(cleanup_acl_cache() -> ok).
cleanup_acl_cache() ->
    set_keys_queue(
        cleanup_acl_cache(get_keys_queue())).

cleanup_acl_cache(KeysQ) ->
    case queue:out(KeysQ) of
        {{value, OldestK}, RemKeys} ->
            {_AclResult, CachedAt} = erlang:get(OldestK),
            if_expired(CachedAt,
                fun(false) -> KeysQ;
                   (true) ->
                      erlang:erase(OldestK),
                      decr_cache_size(),
                      cleanup_acl_cache(RemKeys)
                end);
        {empty, KeysQ} -> KeysQ
    end.

get_newest_key() ->
    get_key(fun(KeysQ) -> queue:get_r(KeysQ) end).

get_oldest_key() ->
    get_key(fun(KeysQ) -> queue:get(KeysQ) end).

get_key(Pick) ->
    KeysQ = get_keys_queue(),
    case queue:is_empty(KeysQ) of
        true -> undefined;
        false -> Pick(KeysQ)
    end.

%% for test only
dump_acl_cache() ->
    map_acl_cache(fun(Cache) -> Cache end).
map_acl_cache(Fun) ->
    [Fun(R) || R = {{SubPub, _T}, _Acl} <- get(), SubPub =:= publish
                                             orelse SubPub =:= subscribe].


cache_k(PubSub, Topic)-> {PubSub, Topic}.
cache_v(AclResult)-> {AclResult, time_now()}.

get_cache_max_size() ->
    application:get_env(emqx, acl_cache_max_size, 0).

get_cache_size() ->
    case erlang:get(acl_cache_size) of
        undefined -> 0;
        Size -> Size
    end.
incr_cache_size() ->
    erlang:put(acl_cache_size, get_cache_size() + 1), ok.
decr_cache_size() ->
    erlang:put(acl_cache_size, get_cache_size() - 1), ok.
set_cache_size(N) ->
    erlang:put(acl_cache_size, N), ok.

keys_queue_in(Key) ->
    %% delete the key first if exists
    KeysQ = get_keys_queue(),
    set_keys_queue(queue:in(Key, KeysQ)).

keys_queue_update(Key) ->
    NewKeysQ = remove_key(Key, get_keys_queue()),
    set_keys_queue(queue:in(Key, NewKeysQ)).

remove_key(Key, KeysQ) ->
    queue:filter(fun
        (K) when K =:= Key -> false; (_) -> true
      end, KeysQ).

set_keys_queue(KeysQ) ->
    erlang:put(acl_keys_q, KeysQ), ok.
get_keys_queue() ->
    case erlang:get(acl_keys_q) of
        undefined -> queue:new();
        KeysQ -> KeysQ
    end.

time_now() -> erlang:system_time(millisecond).

if_expired(CachedAt, Fun) ->
    TTL = application:get_env(emqx, acl_cache_ttl, 60000),
    Now = time_now(),
    if (CachedAt + TTL) =< Now ->
           Fun(true);
       true ->
           Fun(false)
    end.
