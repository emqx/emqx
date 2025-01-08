%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_message_ds_gc_worker).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("session_internals.hrl").

%% API
-export([
    start_link/0,
    gc/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% call/cast/info records
-record(gc, {}).

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% For testing or manual ops
gc() ->
    gen_server:call(?MODULE, #gc{}, infinity).

%%--------------------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------------------

init(_Opts) ->
    ensure_gc_timer(),
    State = #{},
    {ok, State}.

handle_call(#gc{}, _From, State) ->
    maybe_gc(),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, error, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    try_gc(),
    ensure_gc_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------------------

ensure_gc_timer() ->
    Timeout = emqx_config:get([durable_sessions, message_retention_period]),
    _ = erlang:send_after(Timeout, self(), #gc{}),
    ok.

try_gc() ->
    %% Only cores should run GC.
    CoreNodes = mria_membership:running_core_nodelist(),
    Res = global:trans(
        {?MODULE, self()},
        fun maybe_gc/0,
        CoreNodes,
        %% Note: we set retries to 1 here because, in rare occasions, GC might start at the
        %% same time in more than one node, and each one will abort the other.  By allowing
        %% one retry, at least one node will (hopefully) get to enter the transaction and
        %% the other will abort.  If GC runs too fast, both nodes might run in sequence.
        %% But, in that case, GC is clearly not too costly, and that shouldn't be a problem,
        %% resource-wise.
        _Retries = 1
    ),
    case Res of
        aborted ->
            ?tp(ds_message_gc_lock_taken, #{}),
            ok;
        ok ->
            ok
    end.

now_ms() ->
    erlang:system_time(millisecond).

maybe_gc() ->
    AllGens = emqx_ds:list_generations_with_lifetimes(?PERSISTENT_MESSAGE_DB),
    NowMS = now_ms(),
    RetentionPeriod = emqx_config:get([durable_sessions, message_retention_period]),
    TimeThreshold = NowMS - RetentionPeriod,
    maybe_create_new_generation(AllGens, TimeThreshold),
    ?tp_span(
        ps_message_gc,
        #{},
        begin
            ExpiredGens =
                maps:filter(
                    fun(_GenId, #{until := Until}) ->
                        is_number(Until) andalso Until =< TimeThreshold
                    end,
                    AllGens
                ),
            ExpiredGenIds = maps:keys(ExpiredGens),
            lists:foreach(
                fun(GenId) ->
                    ok = emqx_ds:drop_generation(?PERSISTENT_MESSAGE_DB, GenId),
                    ?tp(message_gc_generation_dropped, #{gen_id => GenId})
                end,
                ExpiredGenIds
            )
        end
    ).

maybe_create_new_generation(AllGens, TimeThreshold) ->
    NeedNewGen =
        lists:all(
            fun({_GenId, #{created_at := CreatedAt}}) ->
                CreatedAt =< TimeThreshold
            end,
            maps:to_list(AllGens)
        ),
    case NeedNewGen of
        false ->
            ?tp(ps_message_gc_too_early, #{}),
            ok;
        true ->
            ok = emqx_ds:add_generation(?PERSISTENT_MESSAGE_DB),
            ?tp(ps_message_gc_added_gen, #{})
    end.
