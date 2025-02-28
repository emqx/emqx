%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    try ets:lookup(?TABLE, LimiterId) of
        [#bucket_ref{bucket_ref = BucketRef}] -> BucketRef;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

-spec insert_buckets(group(), [{limiter_id(), bucket_ref()}]) -> ok.
insert_buckets(Group, Buckets) ->
    gen_server:call(?MODULE, #insert_buckets{group = Group, buckets = Buckets}).

-spec delete_buckets(group()) -> ok.
delete_buckets(Group) ->
    gen_server:call(?MODULE, #delete_buckets{group = Group}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%%  gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_utils_ets:new(?TABLE, [named_table, ordered_set, protected, {keypos, #bucket_ref.key}]),
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
