%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Shared Sub]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% APIs
-export([start_link/0]).

-export([ subscribe/3
        , unsubscribe/3
        ]).

-export([dispatch/3]).

-export([ maybe_ack/1
        , maybe_nack_dropped/1
        , nack_no_connection/1
        , is_ack_required/1
        ]).

%% for testing
-export([subscribers/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).
-define(SHARED_SUBS, emqx_shared_subscriber).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).
-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(ack, shared_sub_ack).
-define(nack(Reason), {shared_sub_nack, Reason}).
-define(IS_LOCAL_PID(Pid), (is_pid(Pid) andalso node(Pid) =:= node())).
-define(no_ack, no_ack).

-record(state, {pmon}).

-record(emqx_shared_subscription, {group, topic, subpid}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, emqx_shared_subscription},
                {attributes, record_info(fields, emqx_shared_subscription)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(subscribe(emqx_topic:group(), emqx_topic:topic(), pid()) -> ok).
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid}).

-spec(unsubscribe(emqx_topic:group(), emqx_topic:topic(), pid()) -> ok).
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).

record(Group, Topic, SubPid) ->
    #emqx_shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

-spec(dispatch(emqx_topic:group(), emqx_topic:topic(), emqx_types:delivery())
      -> emqx_types:deliver_result()).
dispatch(Group, Topic, Delivery) ->
    dispatch(Group, Topic, Delivery, _FailedSubs = []).

dispatch(Group, Topic, Delivery = #delivery{message = Msg}, FailedSubs) ->
    #message{from = ClientId} = Msg,
    case pick(strategy(), ClientId, Group, Topic, FailedSubs) of
        false ->
            {error, no_subscribers};
        {Type, SubPid} ->
            case do_dispatch(SubPid, Topic, Msg, Type) of
                ok ->
                    ok;
                {error, _Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Group, Topic, Delivery, [SubPid | FailedSubs])
            end
    end.

-spec(strategy() -> random | round_robin | sticky | hash).
strategy() ->
    emqx:get_env(shared_subscription_strategy, round_robin).

-spec(ack_enabled() -> boolean()).
ack_enabled() ->
    emqx:get_env(shared_dispatch_ack_enabled, false).

do_dispatch(SubPid, Topic, Msg, _Type) when SubPid =:= self() ->
    %% Deadlock otherwise
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
do_dispatch(SubPid, Topic, Msg, Type) ->
    dispatch_per_qos(SubPid, Topic, Msg, Type).

%% return either 'ok' (when everything is fine) or 'error'
dispatch_per_qos(SubPid, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
dispatch_per_qos(SubPid, Topic, Msg, retry) ->
    %% Retry implies all subscribers nack:ed, send again without ack
    _ = erlang:send(SubPid, {deliver, Topic, Msg}),
    ok;
dispatch_per_qos(SubPid, Topic, Msg, fresh) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Topic, Msg);
        false ->
            _ = erlang:send(SubPid, {deliver, Topic, Msg}),
            ok
    end.

dispatch_with_ack(SubPid, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    _ = erlang:send(SubPid, {deliver, Topic, with_ack_ref(Msg, {Sender, Ref})}),
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

%% @doc Negative ack dropped message due to inflight window or message queue being full.
-spec(maybe_nack_dropped(emqx_types:message()) -> ok).
maybe_nack_dropped(Msg) ->
    case get_ack_ref(Msg) of
        ?no_ack -> ok;
        {Sender, Ref} -> nack(Sender, Ref, dropped)
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
            {fresh, Sub0};
        false ->
            %% randomly pick one for the first message
            {Type, Sub} = do_pick(random, ClientId, Group, Topic, [Sub0 | FailedSubs]),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            {Type, Sub}
    end;
pick(Strategy, ClientId, Group, Topic, FailedSubs) ->
    do_pick(Strategy, ClientId, Group, Topic, FailedSubs).

do_pick(Strategy, ClientId, Group, Topic, FailedSubs) ->
    All = subscribers(Group, Topic),
    case All -- FailedSubs of
        [] when FailedSubs =:= [] ->
            %% Genuinely no subscriber
            false;
        [] ->
            %% All offline? pick one anyway
            {retry, pick_subscriber(Group, Topic, Strategy, ClientId, All)};
        Subs ->
            %% More than one available
            {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, Subs)}
    end.

pick_subscriber(_Group, _Topic, _Strategy, _ClientId, [Sub]) -> Sub;
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

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
    {atomic, PMon} = mnesia:transaction(fun init_monitors/0),
    ok = emqx_tables:new(?SHARED_SUBS, [protected, bag]),
    ok = emqx_tables:new(?ALIVE_SUBS, [protected, set, {read_concurrency, true}]),
    {ok, update_stats(#state{pmon = PMon})}.

init_monitors() ->
    mnesia:foldl(
      fun(#emqx_shared_subscription{subpid = SubPid}, Mon) ->
          emqx_pmon:monitor(SubPid, Mon)
      end, emqx_pmon:new(), ?TAB).

handle_call({subscribe, Group, Topic, SubPid}, _From, State = #state{pmon = PMon}) ->
    mnesia:dirty_write(?TAB, record(Group, Topic, SubPid)),
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true  -> ok;
        false -> ok = emqx_router:do_add_route(Topic, {Group, node()})
    end,
    ok = maybe_insert_alive_tab(SubPid),
    true = ets:insert(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    {reply, ok, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_call({unsubscribe, Group, Topic, SubPid}, _From, State) ->
    mnesia:dirty_delete_object(?TAB, record(Group, Topic, SubPid)),
    true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true  -> ok;
        false -> ok = emqx_router:do_delete_route(Topic, {Group, node()})
    end,
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
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
    ?LOG(info, "Shared subscriber down: ~p", [SubPid]),
    cleanup_down(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:erase(SubPid, PMon)})};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, ?TAB, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% keep track of alive remote pids
maybe_insert_alive_tab(Pid) when ?IS_LOCAL_PID(Pid) -> ok;
maybe_insert_alive_tab(Pid) when is_pid(Pid) -> ets:insert(?ALIVE_SUBS, {Pid}), ok.

cleanup_down(SubPid) ->
    ?IS_LOCAL_PID(SubPid) orelse ets:delete(?ALIVE_SUBS, SubPid),
    lists:foreach(
        fun(Record = #emqx_shared_subscription{topic = Topic, group = Group}) ->
            ok = mnesia:dirty_delete_object(?TAB, Record),
            true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
            case ets:member(?SHARED_SUBS, {Group, Topic}) of
                true -> ok;
                false -> ok = emqx_router:do_delete_route(Topic, {Group, node()})
            end
        end, mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})).

update_stats(State) ->
    emqx_stats:setstat('subscriptions.shared.count', 'subscriptions.shared.max', ets:info(?TAB, size)),
    State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs) ->
    is_alive_sub(Pid) andalso not lists:member(Pid, FailedSubs).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when ?IS_LOCAL_PID(Pid) ->
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    [] =/= ets:lookup(?ALIVE_SUBS, Pid).

