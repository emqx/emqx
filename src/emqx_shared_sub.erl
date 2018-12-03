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
-export([dispatch/3, maybe_ack/1, maybe_nack_dropped/1, nack_no_connection/1, is_ack_required/1]).

-export([subscribers/2, groups/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscriber).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).
-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(ack, shared_sub_ack).
-define(nack(Reason), {shared_sub_nack, Reason}).
-define(IS_LOCAL_PID(Pid), (is_pid(Pid) andalso node(Pid) =:= node())).
-define(no_ack, no_ack).

-define(SHARED_SUB(Group, Topic, SubPid),
        #emqx_shared_subscriber{key = {Group, Topic},
                                topic = Topic,
                                group = Group,
                                subpid = SubPid}).
-record(state, {}).
-record(emqx_shared_subscriber, {
            key :: {emqx_types:topic(), emqx_types:subgroup()},
            topic :: emqx_types:topic(),    %% for buiding extra index
            group :: emqx_types:subgroup(), %% for buiding extra index
            subpid :: pid()
        }).

-include("emqx_mqtt.hrl").

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {index, [topic, group, subpid]},
                {record_name, emqx_shared_subscriber},
                {attributes, record_info(fields, emqx_shared_subscriber)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

subscribe(undefined, _Topic, _SubPid) ->
    ok;
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_write(?TAB, ?SHARED_SUB(Group, Topic, SubPid)),
    gen_server:cast(?SERVER, {subscribe, SubPid}).

unsubscribe(undefined, _Topic, _SubPid) ->
    ok;
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    mnesia:dirty_delete(?TAB, {Group, Topic}).

dispatch(Group, Topic, Delivery) ->
    dispatch(Group, Topic, Delivery, _FailedSubs = []).

dispatch(Group, Topic, Delivery = #delivery{message = Msg, results = Results}, FailedSubs) ->
    #message{from = ClientId} = Msg,
    case pick(strategy(), ClientId, Group, Topic, FailedSubs) of
        false ->
            Delivery;
        SubPid ->
            case do_dispatch(SubPid, Group, Topic, Msg) of
                ok ->
                    Delivery#delivery{results = [{dispatch_shared, {Group, Topic}, 1} | Results]};
                {error, _Reason} ->
                    %% failed to dispatch to this sub, try next
                    %% 'Reason' is discarded so far, meaning for QoS1/2 messages
                    %% if all subscribers are off line, the dispatch would faile
                    %% even if there are sessions not expired yet.
                    %% If required, we can make use of the 'no_connection' reason to perform
                    %% retry without requiring acks, so the messages can be delivered
                    %% to sessions of offline clients
                    dispatch(Group, Topic, Delivery, [SubPid | FailedSubs])
            end
    end.

-spec(strategy() -> random | round_robin | sticky | hash).
strategy() ->
    emqx_config:get_env(shared_subscription_strategy, round_robin).

-spec(ack_enabled() -> boolean()).
ack_enabled() ->
    emqx_config:get_env(shared_dispatch_ack_enabled, false).

do_dispatch(SubPid, Group, Topic, Msg) when SubPid =:= self() ->
    %% Deadlock otherwise
    _ = erlang:send(SubPid, {dispatch_shared, Group, Topic, Msg}),
    ok;
do_dispatch(SubPid, Group, Topic, Msg) ->
    dispatch_per_qos(SubPid, Group, Topic, Msg).

%% return either 'ok' (when everything is fine) or 'error'
dispatch_per_qos(SubPid, Group, Topic, #message{qos = ?QOS_0} = Msg) ->
    %% For QoS 0 message, send it as regular dispatch
    _ = erlang:send(SubPid, {dispatch_shared, Group, Topic, Msg}),
    ok;
dispatch_per_qos(SubPid, Group, Topic, Msg) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Group, Topic, Msg);
        false ->
            _ = erlang:send(SubPid, {dispatch_shared, Group, Topic, Msg}),
            ok
    end.

dispatch_with_ack(SubPid, Group, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    _ = erlang:send(SubPid, {dispatch_shared, Group, Topic, with_ack_ref(Msg, {Sender, Ref})}),
    Timeout = case Msg#message.qos of
                  ?QOS_1 -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS);
                  ?QOS_2 -> infinity
              end,
    try
        receive
            {Ref, ?ack} ->
                ok;
            {Ref, ?nack(Reason)} ->
                %% the receive session may nack this message when its queue is full
                {error, Reason};
            {'DOWN', Ref, process, SubPid, Reason} ->
                {error, Reason}
        after
            Timeout ->
                {error, timeout}
        end
    after
        _ = erlang:demonitor(Ref, [flush])
    end.

with_ack_ref(Msg, SenderRef) ->
    emqx_message:set_headers(#{shared_dispatch_ack => SenderRef}, Msg).

without_ack_ref(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?no_ack}, Msg).

get_ack_ref(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?no_ack).

-spec(is_ack_required(emqx_types:message()) -> boolean()).
is_ack_required(Msg) -> ?no_ack =/= get_ack_ref(Msg).

%% @doc Negative ack dropped message due to message queue being full.
-spec(maybe_nack_dropped(emqx_types:message()) -> ok).
maybe_nack_dropped(Msg) ->
    case get_ack_ref(Msg) of
        ?no_ack -> ok;
        {Sender, Ref} -> nack(Sender, Ref, drpped)
    end.

%% @doc Negative ack message due to connection down.
%% Assuming this function is always called when ack is required
%% i.e is_ack_required returned true.
-spec(nack_no_connection(emqx_types:message()) -> ok).
nack_no_connection(Msg) ->
    {Sender, Ref} = get_ack_ref(Msg),
    nack(Sender, Ref, no_connection).

-spec(nack(pid(), reference(), dropped | no_connection) -> ok).
nack(Sender, Ref, Reason) ->
    erlang:send(Sender, {Ref, ?nack(Reason)}),
    ok.

-spec(maybe_ack(emqx_types:message()) -> emqx_types:message()).
maybe_ack(Msg) ->
    case get_ack_ref(Msg) of
        ?no_ack ->
            Msg;
        {Sender, Ref} ->
            erlang:send(Sender, {Ref, ?ack}),
            without_ack_ref(Msg)
    end.

pick(sticky, ClientId, Group, Topic, FailedSubs) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    case is_active_sub(Sub0, FailedSubs) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            Sub0;
        false ->
            %% randomly pick one for the first message
            Sub = do_pick(random, ClientId, Group, Topic, FailedSubs),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            Sub
    end;
pick(Strategy, ClientId, Group, Topic, FailedSubs) ->
    do_pick(Strategy, ClientId, Group, Topic, FailedSubs).

do_pick(Strategy, ClientId, Group, Topic, FailedSubs) ->
    case subscribers(Group, Topic) -- FailedSubs of
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
   [Pid || #emqx_shared_subscriber{subpid = Pid}
                <- mnesia:dirty_read(?TAB, {Group, Topic})].

groups(Topic) ->
   lists:usort([Group || #emqx_shared_subscriber{group = Group}
                <- mnesia:dirty_index_read(?TAB, Topic, #emqx_shared_subscriber.topic)]).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    ets:new(?ALIVE_SUBS, [named_table, {read_concurrency, true}, protected]),
    {ok, update_stats(#state{})}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[SharedSub] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({subscribe, SubPid}, State= #state{}) ->
    ok = maybe_insert_alive_tab(SubPid),
    {noreply, update_stats(State#state{})};
handle_cast(Msg, State) ->
    emqx_logger:error("[SharedSub] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    emqx_logger:error("[SharedSub] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% keep track of alive remote pids
maybe_insert_alive_tab(Pid) when ?IS_LOCAL_PID(Pid) -> ok;
maybe_insert_alive_tab(Pid) when is_pid(Pid) -> ets:insert(?ALIVE_SUBS, {Pid}), ok.

update_stats(State) ->
    emqx_stats:setstat('subscriptions/shared/count', 'subscriptions/shared/max', ets:info(?TAB, size)), State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs) ->
    is_alive_sub(Pid) andalso not lists:member(Pid, FailedSubs).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when ?IS_LOCAL_PID(Pid) ->
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    [] =/= ets:lookup(?ALIVE_SUBS, Pid).

