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
    delete_buckets/1
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

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #{groups => sets:new([{version, 2}])}}.

handle_call(#insert_buckets{group = Group, buckets = Buckets}, _From, #{groups := Groups} = State) ->
    _ = persistent_term:put(?PT_KEY(Group), maps:from_list(Buckets)),
    {reply, ok, State#{groups := sets:add_element(Group, Groups)}};
handle_call(#delete_buckets{group = Group}, _From, #{groups := Groups} = State) ->
    _ = persistent_term:erase(?PT_KEY(Group)),
    {reply, ok, State#{groups := sets:del_element(Group, Groups)}};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Req}),
    {noreply, State}.

terminate(_Reason, #{groups := Groups} = _State) ->
    lists:foreach(
        fun(Group) ->
            _ = persistent_term:erase(?PT_KEY(Group))
        end,
        sets:to_list(Groups)
    ).
