%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_shared_sub).

-behaviour(gen_server).

-include("emqx.hrl").

%% API
-export([start_link/0]).

-export([strategy/0]).

-export([subscribe/3, unsubscribe/3]).

-export([dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(TAB, emqx_shared_subscription).

-record(state, {pmon}).

-record(shared_subscription, {group, topic, subpid}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, shared_subscription},
                {attributes, record_info(fields, shared_subscription)}]),
    ok = ekka_mnesia:copy_table(?TAB),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(strategy() -> random | hash).
strategy() ->
    emqx_config:get_env(shared_subscription_strategy, random).

subscribe(undefined, _Topic, _SubPid) ->
    ok;
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_write(?TAB, r(Group, Topic, SubPid)),
    gen_server:cast(?SERVER, {monitor, SubPid}).

unsubscribe(undefined, _Topic, _SubPid) ->
    ok;
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_delete_object(?TAB, r(Group, Topic, SubPid)).

r(Group, Topic, SubPid) ->
    #shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

dispatch({Cluster, Group}, Topic, Delivery) ->
    case ekka:cluster_name() of
        Cluster ->
            dispatch(Group, Topic, Delivery);
        _ -> Delivery
    end;

%% TODO: ensure the delivery...
dispatch(Group, Topic, Delivery = #delivery{message = Msg, flows = Flows}) ->
    case pick(subscribers(Group, Topic)) of
        false  -> Delivery;
        SubPid -> SubPid ! {dispatch, Topic, Msg},
                  Delivery#delivery{flows = [{dispatch, {Group, Topic}, 1} | Flows]}
    end.

pick([]) ->
    false;
pick([SubPid]) ->
    SubPid;
pick(SubPids) ->
    X = abs(erlang:monotonic_time()
		bxor erlang:unique_integer()),
    lists:nth((X rem length(SubPids)) + 1, SubPids).

subscribers(Group, Topic) ->
    ets:select(?TAB, [{{shared_subscription, Group, Topic, '$1'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {atomic, PMon} = mnesia:transaction(fun init_monitors/0),
    mnesia:subscribe({table, ?TAB, simple}),
    {ok, #state{pmon = PMon}}.

init_monitors() ->
    mnesia:foldl(
      fun(#shared_subscription{subpid = SubPid}, Mon) ->
          Mon:monitor(SubPid)
      end, emqx_pmon:new(), ?TAB).

handle_call(Req, _From, State) ->
    emqx_log:error("[Shared] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({monitor, SubPid}, State= #state{pmon = PMon}) ->
    {noreply, State#state{pmon = PMon:monitor(SubPid)}};

handle_cast(Msg, State) ->
    emqx_log:error("[Shared] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) ->
    emqx_log:info("Shared subscription created: ~p", [NewRecord]),
    #shared_subscription{subpid = SubPid} = NewRecord,
    {noreply, State#state{pmon = PMon:monitor(SubPid)}};

handle_info({mnesia_table_event, {delete_object, OldRecord, _}}, State = #state{pmon = PMon}) ->
    emqx_log:info("Shared subscription deleted: ~p", [OldRecord]),
    #shared_subscription{subpid = SubPid} = OldRecord,
    {noreply, State#state{pmon = PMon:demonitor(SubPid)}};

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{pmon = PMon}) ->
    emqx_log:info("Shared subscription down: ~p", [SubPid]),
    mnesia:async_dirty(fun cleanup_down/1, [SubPid]),
    {noreply, State#state{pmon = PMon:erase(SubPid)}};

handle_info(Info, State) ->
    emqx_log:error("[Shared] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, ?TAB, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_down(SubPid) ->
    Pat = #shared_subscription{_ = '_', subpid = SubPid},
    lists:foreach(fun(Record) ->
                      mnesia:delete_object(?TAB, Record)
                  end, mnesia:match_object(Pat)).

