%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Registry of shared limiter instances (buckets).
%%
%% `emqx_limiter_registry' holds limiter prototypes (options); this module
%% holds the live buckets of the shared limiter kind, keyed by group.
%%
%% Buckets are stored in `persistent_term', one key per group, so that a
%% consumer reads its bucket as a literal reference: no ETS copy, no
%% garbage. Writes go through this process. A group's buckets are stored
%% once at creation (a new key: no global GC) and erased with the group.
%% Option updates reset the atomics in place and never rewrite the key.
-module(emqx_limiter_bucket_registry).

-include_lib("emqx/include/logger.hrl").

-behaviour(gen_server).

-export([
    start_link/0,
    find_bucket/1,
    insert_buckets/2,
    delete_buckets/1,
    delete_all_buckets/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type group() :: emqx_limiter:group().
-type name() :: emqx_limiter:name().
-type bucket_ref() :: emqx_limiter_shared:bucket_ref().
-type limiter_id() :: emqx_limiter:id().

-define(PT_KEY(GROUP), {?MODULE, GROUP}).

%%--------------------------------------------------------------------
%% gen_server messages
%%--------------------------------------------------------------------

-record(insert_buckets, {
    group :: group(),
    buckets :: [{name(), bucket_ref()}]
}).

-record(delete_buckets, {
    group :: group()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec find_bucket(limiter_id()) -> bucket_ref() | undefined.
find_bucket({Group, Name}) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        #{Name := BucketRef} -> BucketRef;
        _ -> undefined
    end.

-spec insert_buckets(group(), [{name(), bucket_ref()}]) -> ok.
insert_buckets(Group, Buckets) ->
    gen_server:call(?MODULE, #insert_buckets{group = Group, buckets = Buckets}, infinity).

-spec delete_buckets(group()) -> ok.
delete_buckets(Group) ->
    gen_server:call(?MODULE, #delete_buckets{group = Group}, infinity).

-doc """
Erases the buckets of every group, without asking whether the groups still
exist. Call it when the whole limiter is going away, from application stop or
from this process's shutdown, so that no keys are left in `persistent_term'.
Do not call it while the limiter is running: a registered group whose buckets
are erased cannot consume until the group is created again.
""".
-spec delete_all_buckets() -> ok.
delete_all_buckets() ->
    lists:foreach(
        fun
            ({?PT_KEY(_Group) = Key, _Buckets}) ->
                _ = persistent_term:erase(Key),
                ok;
            ({_OtherKey, _Value}) ->
                ok
        end,
        persistent_term:get()
    ).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ok = erase_stale_buckets(),
    {ok, #{}}.

handle_call(#insert_buckets{group = Group, buckets = Buckets}, _From, State) ->
    _ = persistent_term:put(?PT_KEY(Group), maps:from_list(Buckets)),
    {reply, ok, State};
handle_call(#delete_buckets{group = Group}, _From, State) ->
    _ = persistent_term:erase(?PT_KEY(Group)),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Req}),
    {noreply, State}.

%% The supervisor stops this process before `emqx_limiter_registry', so at
%% shutdown every group is still registered and the startup sweep would keep
%% every key. Erase them all instead. A crash is different: the limiter keeps
%% running, and the groups still need their buckets.
terminate(shutdown, _State) ->
    delete_all_buckets();
terminate({shutdown, _}, _State) ->
    delete_all_buckets();
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-doc """
Drops bucket keys left behind by a previous incarnation of this process.

`delete_buckets/1` erases a group's key on the normal path, but a brutal kill
runs no `terminate/2`, so keys can outlive the groups they belong to. A key is
stale only when its group is no longer registered: that way a restart of this
process alone leaves the buckets of a live group untouched.

`persistent_term:get/0` copies neither the keys nor the values of the terms it
returns, only the list that holds them, so the sweep costs the number of
persistent terms rather than their size.
""".
erase_stale_buckets() ->
    lists:foreach(
        fun
            ({?PT_KEY(Group) = Key, _Buckets}) ->
                case emqx_limiter_registry:find_group(Group) of
                    undefined ->
                        _ = persistent_term:erase(Key),
                        ok;
                    {_Module, _LimiterOptions} ->
                        ok
                end;
            ({_OtherKey, _Value}) ->
                ok
        end,
        persistent_term:get()
    ).
