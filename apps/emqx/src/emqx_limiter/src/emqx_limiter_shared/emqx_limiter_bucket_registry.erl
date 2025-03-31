%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Registry for shared limiter buckets.

-module(emqx_limiter_bucket_registry).
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

-export([
    start_link/0,
    find_bucket/1,
    insert_buckets/2,
    delete_buckets/1
]).

-export([
    create_table/0
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

-define(TABLE, ?MODULE).

-record(bucket_ref, {
    key :: {group(), name() | '_'},
    bucket_ref :: bucket_ref() | '_'
}).

%%--------------------------------------------------------------------
%%  Messages
%%--------------------------------------------------------------------

-record(insert_buckets, {
    group :: emqx_limiter:group(),
    buckets :: [{emqx_limiter:name(), bucket_ref()}]
}).

-record(delete_buckets, {
    group :: emqx_limiter:group()
}).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec find_bucket(limiter_id()) -> bucket_ref() | undefined.
find_bucket(LimiterId) ->
    try
        ets:lookup_element(?TABLE, LimiterId, #bucket_ref.bucket_ref)
    catch
        error:badarg -> undefined
    end.

-spec insert_buckets(group(), [{limiter_id(), bucket_ref()}]) -> ok.
insert_buckets(Group, Buckets) ->
    gen_server:call(?MODULE, #insert_buckets{group = Group, buckets = Buckets}, infinity).

-spec delete_buckets(group()) -> ok.
delete_buckets(Group) ->
    gen_server:call(?MODULE, #delete_buckets{group = Group}, infinity).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

create_table() ->
    ok = emqx_utils_ets:new(?TABLE, [named_table, ordered_set, public, {keypos, #bucket_ref.key}]).

%%--------------------------------------------------------------------
%%  gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(
    #insert_buckets{group = Group, buckets = Buckets},
    _From,
    State
) ->
    Records = lists:map(
        fun({Name, BucketRef}) ->
            #bucket_ref{key = {Group, Name}, bucket_ref = BucketRef}
        end,
        Buckets
    ),
    _ = ets:insert(?TABLE, Records),
    {reply, ok, State};
handle_call(#delete_buckets{group = Group}, _From, State) ->
    MatchSpec = #bucket_ref{key = {Group, '_'}, _ = '_'},
    _ = ets:match_delete(?TABLE, MatchSpec),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    ?SLOG(warning, #{msg => unknown_request, request => Request}),
    {reply, {error, not_implemented}, State}.

handle_cast(Request, State) ->
    ?SLOG(warning, #{msg => unknown_cast, request => Request}),
    {noreply, State}.

handle_info(Request, State) ->
    ?SLOG(warning, #{msg => unknown_info, request => Request}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
