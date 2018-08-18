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
         delete_acl_cache/2,
         cleanup_acl_cache/0,
         dump_acl_cache/0,
         get_cache_size/0,
         get_newest_key/0
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

-spec(get_acl_cache(PubSub :: publish | subscribe, Topic :: topic())
        -> (acl_result() | not_found)).
get_acl_cache(PubSub, Topic) ->
    case erlang:get({PubSub, Topic}) of
        undefined -> not_found;
        {AclResult, CachedAt, _NextK, _PrevK} ->
            if_acl_cache_expired(CachedAt,
                fun(false) ->
                      AclResult;
                   (true) ->
                      %% this expired entry will get updated in
                      %%   put_acl_cache/3
                      not_found
                end)
    end.

-spec(put_acl_cache(PubSub :: publish | subscribe,
                    Topic :: topic(), AclResult :: acl_result()) -> ok).
put_acl_cache(PubSub, Topic, AclResult) ->
    MaxSize = get_cache_max_size(), true = (MaxSize =/= 0),
    Size = get_cache_size(),
    if
        Size =:= 0 ->
            create_first(PubSub, Topic, AclResult);
        Size < MaxSize ->
            append(PubSub, Topic, AclResult);
        Size =:= MaxSize ->
            %% when the cache get full, and also the latest one
            %%   is expired, we'll perform a cleanup.
            NewestK = get_newest_key(),
            {_AclResult, CachedAt, OldestK, _PrevK} = erlang:get(NewestK),
            if_acl_cache_expired(CachedAt,
                fun(true) ->
                      % try to cleanup first
                      cleanup_acl_cache(OldestK),
                      add_cache(PubSub, Topic, AclResult);
                   (false) ->
                      % cache full, perform cache replacement
                      delete_acl_cache(OldestK),
                      append(PubSub, Topic, AclResult)
                end)
    end.

-spec(delete_acl_cache(PubSub :: publish | subscribe, Topic :: topic()) -> ok).
delete_acl_cache(PubSub, Topic) ->
    delete_acl_cache(_K = {PubSub, Topic}).
delete_acl_cache(K) ->
    case erlang:get(K) of
        undefined -> ok;
        {_AclResult, _CachedAt, NextK, PrevK} when NextK =:= PrevK ->
            %% there is only one entry in the cache
            erlang:erase(K),
            decr_cache_size(),
            set_newest_key(undefined);
        {_AclResult, _CachedAt, NextK, PrevK} ->
            update_next(PrevK, NextK),
            update_prev(NextK, PrevK),
            erlang:erase(K),

            decr_cache_size(),
            NewestK = get_newest_key(),
            if
                K =:= NewestK -> set_newest_key(NextK);
                true -> ok
            end
    end.

%% evict all the exipired cache entries
-spec(cleanup_acl_cache() -> ok).
cleanup_acl_cache() ->
    case get_newest_key() of
        undefined -> ok;
        NewestK ->
            {_AclResult, _CachedAt, OldestK, _PrevK} = erlang:get(NewestK),
            cleanup_acl_cache(OldestK)
    end.
cleanup_acl_cache(FromK) ->
    case erlang:get(FromK) of
        undefined -> ok;
        {_AclResult, CachedAt, NextK, _PrevK} ->
            if_acl_cache_expired(CachedAt,
                fun(false) ->
                      ok;
                   (true) ->
                      delete_acl_cache(FromK),
                      cleanup_acl_cache(NextK)
                end)
    end.

%% for test only
dump_acl_cache() ->
    [R || R = {{SubPub, _T}, _Acl} <- get(), SubPub =:= publish
                                             orelse SubPub =:= subscribe].

add_cache(PubSub, Topic, AclResult) ->
    Size = get_cache_size(),
    MaxSize = get_cache_max_size(), true = (MaxSize =/= 0),
    if
        Size =:= 0 ->
            create_first(PubSub, Topic, AclResult);
        Size =:= MaxSize ->
            OldestK = get_next_key(get_newest_key()),
            delete_acl_cache(OldestK),
            case get_cache_size() =:= 0 of
                true -> create_first(PubSub, Topic, AclResult);
                false -> append(PubSub, Topic, AclResult)
            end;
        true ->
            append(PubSub, Topic, AclResult)
    end.

create_first(PubSub, Topic, AclResult) ->
    K = cache_k(PubSub, Topic),
    V = cache_v(AclResult, _NextK = K, _PrevK = K),
    erlang:put(K, V),
    set_cache_size(1),
    set_newest_key(K).

append(PubSub, Topic, AclResult) ->
    %% try to update the existing one:
    %% - we delete it and then append it at the tail
    delete_acl_cache(PubSub, Topic),

    case get_cache_size() =:= 0 of
        true -> create_first(PubSub, Topic, AclResult);
        false ->
            NewestK = get_newest_key(),
            OldestK = get_next_key(NewestK),
            K = cache_k(PubSub, Topic),
            V = cache_v(AclResult, OldestK, NewestK),
            erlang:put(K, V),

            update_next(NewestK, K),
            update_prev(OldestK, K),
            incr_cache_size(),
            set_newest_key(K)
    end.

get_next_key(K) ->
    erlang:element(3, erlang:get(K)).
update_next(K, NextK) ->
    NoNext = erlang:delete_element(3, erlang:get(K)),
    erlang:put(K, erlang:insert_element(3, NoNext, NextK)).
update_prev(K, PrevK) ->
    NoPrev = erlang:delete_element(4, erlang:get(K)),
    erlang:put(K, erlang:insert_element(4, NoPrev, PrevK)).

cache_k(PubSub, Topic)-> {PubSub, Topic}.
cache_v(AclResult, NextK, PrevK)-> {AclResult, time_now(), NextK, PrevK}.

get_cache_max_size() ->
    application:get_env(emqx, acl_cache_size, 100).

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

get_newest_key() ->
    erlang:get(acl_cache_newest_key).

set_newest_key(Key) ->
    erlang:put(acl_cache_newest_key, Key), ok.

time_now() -> erlang:system_time(millisecond).

if_acl_cache_expired(CachedAt, Fun) ->
    TTL = application:get_env(emqx, acl_cache_ttl, 60000),
    Now = time_now(),
    if (CachedAt + TTL) =< Now ->
           Fun(true);
       true ->
           Fun(false)
    end.
