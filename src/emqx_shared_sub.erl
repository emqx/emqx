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

-module(emqx_shared_sub).

-behaviour(gen_server).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/0]).

-export([strategy/0]).
-export([subscribe/3, unsubscribe/3]).
-export([dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).

-record(state, {pmon}).
-record(emqx_shared_subscription, {group, topic, subpid}).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, emqx_shared_subscription},
                {attributes, record_info(fields, emqx_shared_subscription)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(strategy() -> round_robin | random | hash).
strategy() ->
    emqx_config:get_env(shared_subscription_strategy, random).

subscribe(undefined, _Topic, _SubPid) ->
    ok;
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_write(?TAB, record(Group, Topic, SubPid)),
    gen_server:cast(?SERVER, {monitor, SubPid}).

unsubscribe(undefined, _Topic, _SubPid) ->
    ok;
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_delete_object(?TAB, record(Group, Topic, SubPid)).

record(Group, Topic, SubPid) ->
    #emqx_shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

%% TODO: dispatch strategy, ensure the delivery...
dispatch(Group, Topic, Delivery = #delivery{message = Msg, results = Results}) ->
    case pick(subscribers(Group, Topic)) of
        false  -> Delivery;
        SubPid -> SubPid ! {dispatch, Topic, Msg},
                  Delivery#delivery{results = [{dispatch, {Group, Topic}, 1} | Results]}
    end.

pick([]) ->
    false;
pick([SubPid]) ->
    SubPid;
pick(SubPids) ->
    lists:nth(rand:uniform(length(SubPids)), SubPids).

subscribers(Group, Topic) ->
    ets:select(?TAB, [{{emqx_shared_subscription, Group, Topic, '$1'}, [], ['$1']}]).
%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    {atomic, PMon} = mnesia:transaction(fun init_monitors/0),
    mnesia:subscribe({table, ?TAB, simple}),
    {ok, update_stats(#state{pmon = PMon})}.

init_monitors() ->
    mnesia:foldl(
      fun(#emqx_shared_subscription{subpid = SubPid}, Mon) ->
          emqx_pmon:monitor(SubPid, Mon)
      end, emqx_pmon:new(), ?TAB).

handle_call(Req, _From, State) ->
    emqx_logger:error("[SharedSub] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({monitor, SubPid}, State= #state{pmon = PMon}) ->
    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_cast(Msg, State) ->
    emqx_logger:error("[SharedSub] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) ->
    #emqx_shared_subscription{subpid = SubPid} = NewRecord,
    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_info({mnesia_table_event, {delete_object, OldRecord, _}}, State = #state{pmon = PMon}) ->
    #emqx_shared_subscription{subpid = SubPid} = OldRecord,
    {noreply, update_stats(State#state{pmon = emqx_pmon:demonitor(SubPid, PMon)})};

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{pmon = PMon}) ->
    emqx_logger:info("[SharedSub] shared subscriber down: ~p", [SubPid]),
    cleanup_down(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:erase(SubPid, PMon)})};

handle_info(Info, State) ->
    emqx_logger:error("[SharedSub] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, ?TAB, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_down(SubPid) ->
    lists:foreach(
        fun(Record) ->
            mnesia:dirty_delete_object(?TAB, Record)
        end,mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})).

update_stats(State) ->
    emqx_stats:setstat('subscriptions/shared/count', 'subscriptions/shared/max', ets:info(?TAB, size)), State.

