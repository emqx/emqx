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

-export([subscribe/3, unsubscribe/3]).
-export([dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).

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

-spec(strategy() -> random | round_robin | sticky | hash).
strategy() ->
    emqx_config:get_env(shared_subscription_strategy, round_robin).

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

dispatch(Group, Topic, Delivery = #delivery{message = Msg, results = Results}) ->
    #message{from = ClientId} = Msg,
    case pick(strategy(), ClientId, Group, Topic) of
        false  -> Delivery;
        SubPid -> SubPid ! {dispatch, Topic, Msg},
                  Delivery#delivery{results = [{dispatch, {Group, Topic}, 1} | Results]}
    end.

pick(sticky, ClientId, Group, Topic) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    case is_sub_alive(Sub0) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            Sub0;
        false ->
            %% randomly pick one for the first message
            Sub = do_pick(random, ClientId, Group, Topic),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            Sub
    end;
pick(Strategy, ClientId, Group, Topic) ->
    do_pick(Strategy, ClientId, Group, Topic).

do_pick(Strategy, ClientId, Group, Topic) ->
    case subscribers(Group, Topic) of
        [] -> false;
        [Sub] -> Sub;
        All -> pick_subscriber(Group, Topic, Strategy, ClientId, All)
    end.

pick_subscriber(Group, Topic, Strategy, ClientId, Subs) ->
    Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, length(Subs)),
    lists:nth(Nth, Subs).

do_pick_subscriber(_Group, _Topic, random, _ClientId, Count) ->
    rand:uniform(Count);
do_pick_subscriber(_Group, _Topic, hash, ClientId, Count) ->
    1 + erlang:phash2(ClientId) rem Count;
do_pick_subscriber(Group, Topic, round_robin, _ClientId, Count) ->
    Rem = case erlang:get({shared_sub_round_robin, Group, Topic}) of
              undefined -> 0;
              N -> (N + 1) rem Count
          end,
    _ = erlang:put({shared_sub_round_robin, Group, Topic}, Rem),
    Rem + 1.

subscribers(Group, Topic) ->
    ets:select(?TAB, [{{emqx_shared_subscription, Group, Topic, '$1'}, [], ['$1']}]).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    {atomic, PMon} = mnesia:transaction(fun init_monitors/0),
    mnesia:subscribe({table, ?TAB, simple}),
    ets:new(?ALIVE_SUBS, [named_table, {read_concurrency, true}, protected]),
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
    NewPmon = emqx_pmon:monitor(SubPid, PMon),
    ets:insert(?ALIVE_SUBS, {SubPid}),
    {noreply, update_stats(State#state{pmon = NewPmon})};
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
    ets:delete(?ALIVE_SUBS, SubPid),
    lists:foreach(
        fun(Record) ->
            mnesia:dirty_delete_object(?TAB, Record)
        end,mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})).

update_stats(State) ->
    emqx_stats:setstat('subscriptions/shared/count', 'subscriptions/shared/max', ets:info(?TAB, size)), State.

%% erlang:is_process_alive/1 is expensive
%% and does not work with remote pids
is_sub_alive(Sub) -> [] =/= ets:lookup(?ALIVE_SUBS, Sub).

