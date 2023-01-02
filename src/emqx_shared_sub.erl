%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ dispatch/3
        , redispatch/1
        ]).

-export([ maybe_ack/1
        , maybe_nack_dropped/1
        , nack_no_connection/1
        , is_ack_required/1
        , is_retry_dispatch/1
        ]).

%% for testing
-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([strategy/0]).

-type strategy() :: random
                  | round_robin
                  | sticky
                  | local
                  | hash %% same as hash_clientid, backward compatible
                  | hash_clientid
                  | hash_topic.

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).
-define(SHARED_SUBS, emqx_shared_subscriber).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).
-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(IS_LOCAL_PID(Pid), (is_pid(Pid) andalso node(Pid) =:= node())).
-define(ACK, shared_sub_ack).
-define(NACK(Reason), {shared_sub_nack, Reason}).
-define(NO_ACK, no_ack).
-define(REDISPATCH_TO(GROUP, TOPIC), {GROUP, TOPIC}).

-type redispatch_to() :: ?REDISPATCH_TO(emqx_topic:group(), emqx_topic:topic()).

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
    ok = ekka_mnesia:copy_table(?TAB, ram_copies).

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

-spec(dispatch_to_non_self(emqx_topic:group(), emqx_topic:topic(), emqx_types:delivery())
      -> emqx_types:deliver_result()).
dispatch_to_non_self(Group, Topic, Delivery) ->
    Strategy = strategy(Group),
    dispatch(Strategy, Group, Topic, Delivery, _FailedSubs = #{self() => sender}).

-spec(dispatch(emqx_topic:group(), emqx_topic:topic(), emqx_types:delivery())
      -> emqx_types:deliver_result()).
dispatch(Group, Topic, Delivery) ->
    Strategy = strategy(Group),
    dispatch(Strategy, Group, Topic, Delivery, _FailedSubs = #{}).

dispatch(Strategy, Group, Topic, Delivery = #delivery{message = Msg0}, FailedSubs) ->
    #message{from = ClientId, topic = SourceTopic} = Msg0,
    case pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) of
        false -> {error, no_subscribers};
        {Type, SubPid} ->
            Msg = with_redispatch_to(Msg0, Group, Topic),
            case do_dispatch(SubPid, Group, Topic, Msg, Type) of
                ok -> {ok, 1};
                {error, Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Strategy, Group, Topic, Delivery, FailedSubs#{SubPid => Reason})
            end
    end.

-spec(strategy(emqx_topic:group()) -> strategy()).
strategy(Group) ->
    case emqx:get_env(shared_subscription_strategy_per_group, #{}) of
        #{Group := Strategy} ->
            Strategy;
        _ ->
            emqx:get_env(shared_subscription_strategy, random)
    end.


-spec(ack_enabled() -> boolean()).
ack_enabled() ->
    emqx:get_env(shared_dispatch_ack_enabled, false).

do_dispatch(SubPid, _Group, Topic, Msg, _Type) when SubPid =:= self() ->
    %% dispatch without ack, deadlock otherwise
    send(SubPid, Topic, {deliver, Topic, Msg});
%% return either 'ok' (when everything is fine) or 'error'
do_dispatch(SubPid, _Group, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    send(SubPid, Topic, {deliver, Topic, Msg});
do_dispatch(SubPid, Group, Topic, Msg, Type) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Group, Topic, Msg, Type);
        false ->
            send(SubPid, Topic, {deliver, Topic, Msg})
    end.

with_redispatch_to(#message{qos = ?QOS_0} = Msg, _Group, _Topic) -> Msg;
with_redispatch_to(Msg, Group, Topic) ->
    emqx_message:set_headers(#{redispatch_to => ?REDISPATCH_TO(Group, Topic)}, Msg).

dispatch_with_ack(SubPid, Group, Topic, Msg, Type) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    send(SubPid, Topic, {deliver, Topic, with_group_ack(Msg, Group, Type, Sender, Ref)}),
    Timeout = case Msg#message.qos of
                  ?QOS_1 -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS);
                  ?QOS_2 -> infinity
              end,

    %% This OpaqueRef is a forward compatibilty workaround. Pre 4.3.15 versions
    %% pass Ref from `{Sender, Ref}` ack header back as it is.
    OpaqueRef = old_ref(Type, Group, Ref),
    try
        receive
            {ReceivedRef, ?ACK} when ReceivedRef =:= Ref; ReceivedRef =:= OpaqueRef ->
                ok;
            {ReceivedRef, ?NACK(Reason)} when ReceivedRef =:= Ref; ReceivedRef =:= OpaqueRef ->
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

send(Pid, Topic, Msg) ->
    Node = node(Pid),
    if Node =:= node() ->
            Pid ! Msg;
       true ->
            emqx_rpc:cast(Topic, Node, erlang, send, [Pid, Msg])
    end,
    ok.

with_group_ack(Msg, Group, Type, Sender, Ref) ->
    emqx_message:set_headers(#{shared_dispatch_ack => {Sender, old_ref(Type, Group, Ref)}}, Msg).

old_ref(Type, Group, Ref) ->
    {Type, Group, Ref}.

-spec(without_group_ack(emqx_types:message()) -> emqx_types:message()).
without_group_ack(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?NO_ACK}, Msg).

get_group_ack(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?NO_ACK).

%% @hidden Redispatch is neede only for the messages with redispatch_to header added.
is_redispatch_needed(#message{} = Msg) ->
    case get_redispatch_to(Msg) of
        ?REDISPATCH_TO(_, _) ->
            true;
        _ ->
            false
    end.

%% @hidden Return the `redispatch_to` group-topic in the message header.
%% `false` is returned if the message is not a shared dispatch.
%% or when it's a QoS 0 message.
-spec(get_redispatch_to(emqx_types:message()) -> redispatch_to() | false).
get_redispatch_to(Msg) ->
    emqx_message:get_header(redispatch_to, Msg, false).

-spec(is_ack_required(emqx_types:message()) -> boolean()).
is_ack_required(Msg) -> ?NO_ACK =/= get_group_ack(Msg).

-spec(is_retry_dispatch(emqx_types:message()) -> boolean()).
is_retry_dispatch(Msg) ->
    case get_group_ack(Msg) of
        {_Sender, {retry, _Group, _Ref}} -> true;
        _ -> false
    end.

%% @doc Redispatch shared deliveries to other members in the group.
redispatch(Messages0) ->
    Messages = lists:filter(fun is_redispatch_needed/1, Messages0),
    case length(Messages) of
        L when L > 0 ->
            ?LOG(info, "Redispatching ~p shared subscription message(s)", [L]),
            lists:foreach(fun redispatch_shared_message/1, Messages);
        _ ->
            ok
    end.

redispatch_shared_message(#message{} = Msg) ->
    %% As long as it's still a #message{} record in inflight,
    %% we should try to re-dispatch
    ?REDISPATCH_TO(Group, Topic) = get_redispatch_to(Msg),
    %% Note that dispatch is called with self() in failed subs
    %% This is done to avoid dispatching back to caller
    Delivery = #delivery{sender = self(), message = Msg},
    dispatch_to_non_self(Group, Topic, Delivery).

%% @doc Negative ack dropped message due to inflight window or message queue being full.
-spec(maybe_nack_dropped(emqx_types:message()) -> store | drop).
maybe_nack_dropped(Msg) ->
    case get_group_ack(Msg) of
        %% No ack header is present, put it into mqueue
        ?NO_ACK                          -> store;

        %% For fresh Ref we send a nack and return true, to note that the inflight is full
        {Sender, {fresh, _Group, Ref}}   -> nack(Sender, Ref, dropped), drop;

        %% For retry Ref we can't reject a message if inflight is full, so we mark it as
        %% acknowledged and put it into mqueue
        {_Sender, {retry, _Group, _Ref}} -> _ = maybe_ack(Msg), store;

        %% This clause is for backward compatibility
        Ack ->
            {Sender, Ref} = fetch_sender_ref(Ack),
            nack(Sender, Ref, dropped),
            drop
    end.

%% @doc Negative ack message due to connection down.
%% Assuming this function is always called when ack is required
%% i.e is_ack_required returned true.
-spec(nack_no_connection(emqx_types:message()) -> ok).
nack_no_connection(Msg) ->
    {Sender, Ref} = fetch_sender_ref(get_group_ack(Msg)),
    nack(Sender, Ref, no_connection).

-spec(nack(pid(), reference(), dropped | no_connection) -> ok).
nack(Sender, Ref, Reason) ->
    Sender ! {Ref, ?NACK(Reason)},
    ok.

-spec(maybe_ack(emqx_types:message()) -> emqx_types:message()).
maybe_ack(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK ->
            Msg;
        Ack ->
            {Sender, Ref} = fetch_sender_ref(Ack),
            ack(Sender, Ref),
            without_group_ack(Msg)
    end.

-spec(ack(pid(), reference()) -> ok).
ack(Sender, Ref) ->
    Sender ! {Ref, ?ACK},
    ok.

fetch_sender_ref({Sender, {_Type, _Group, Ref}}) -> {Sender, Ref};
%% These clauses are for backward compatibility
fetch_sender_ref({Sender, Ref}) -> {Sender, Ref}.

pick(sticky, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    All = subscribers(Group, Topic),
    case is_active_sub(Sub0, FailedSubs, All) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            {fresh, Sub0};
        false ->
            %% randomly pick one for the first message
            FailedSubs1 = maps_put_new(FailedSubs, Sub0, inactive),
            case do_pick(random, ClientId, SourceTopic, Group, Topic, FailedSubs1) of
              false -> false;
              {Type, Sub} ->
                %% stick to whatever pick result
                erlang:put({shared_sub_sticky, Group, Topic}, Sub),
                {Type, Sub}
            end
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    All = subscribers(Group, Topic),
    case lists:filter(fun(Sub) -> not maps:is_key(Sub, FailedSubs) end, All) of
        [] when All =:= [] ->
            %% Genuinely no subscriber
            false;
        [] ->
            %% We try redispatch to subs who dropped the message because inflight was full.
            Found = maps_find_by(FailedSubs, fun(SubPid, FailReason) ->
                FailReason == dropped andalso is_alive_sub(SubPid)
            end),
            case Found of
                {ok, Dropped, dropped} ->
                    %% Found dropped client
                    {retry, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, [Dropped])};
                error ->
                    %% All offline? pick one anyway
                    {retry, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, All)}
            end;
        Subs ->
            %% More than one available
            {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs)}
    end.

pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, [Sub]) -> Sub;
pick_subscriber(Group, Topic, local, ClientId, SourceTopic, Subs) ->
    case lists:filter(fun(Pid) -> erlang:node(Pid) =:= node() end, Subs) of
        [_ | _] = LocalSubs ->
            pick_subscriber(Group, Topic, random, ClientId, SourceTopic, LocalSubs);
        [] ->
            pick_subscriber(Group, Topic, random, ClientId, SourceTopic, Subs)
    end;
pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs) ->
    Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, length(Subs)),
    lists:nth(Nth, Subs).

do_pick_subscriber(_Group, _Topic, random, _ClientId, _SourceTopic, Count) ->
    rand:uniform(Count);
do_pick_subscriber(Group, Topic, hash, ClientId, SourceTopic, Count) ->
    %% backward compatible
    do_pick_subscriber(Group, Topic, hash_clientid, ClientId, SourceTopic, Count);
do_pick_subscriber(_Group, _Topic, hash_clientid, ClientId, _SourceTopic, Count) ->
    1 + erlang:phash2(ClientId) rem Count;
do_pick_subscriber(_Group, _Topic, hash_topic, _ClientId, SourceTopic, Count) ->
    1 + erlang:phash2(SourceTopic) rem Count;
do_pick_subscriber(Group, Topic, round_robin, _ClientId, _SourceTopic, Count) ->
    Rem = case erlang:get({shared_sub_round_robin, Group, Topic}) of
              undefined -> rand:uniform(Count) - 1;
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
    delete_route_if_needed({Group, Topic}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) ->
    #emqx_shared_subscription{subpid = SubPid} = NewRecord,
    ok = maybe_insert_alive_tab(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

%% The subscriber may have subscribed multiple topics, so we need to keep monitoring the PID until
%% it `unsubscribed` the last topic.
%% The trick is we don't demonitor the subscriber here, and (after a long time) it will eventually
%% be disconnected.
% handle_info({mnesia_table_event, {delete_object, OldRecord, _}}, State = #state{pmon = PMon}) ->
%     #emqx_shared_subscription{subpid = SubPid} = OldRecord,
%     {noreply, update_stats(State#state{pmon = emqx_pmon:demonitor(SubPid, PMon)})};

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{pmon = PMon}) ->
    ?LOG(info, "Shared subscriber down: ~p", [SubPid]),
    cleanup_down(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:erase(SubPid, PMon)})};

handle_info(_Info, State) ->
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
            delete_route_if_needed({Group, Topic})
        end, mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})).

update_stats(State) ->
    emqx_stats:setstat('subscriptions.shared.count',
                       'subscriptions.shared.max',
                       ets:info(?TAB, size)
                      ),
    State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs, All) ->
    lists:member(Pid, All) andalso
        (not maps:is_key(Pid, FailedSubs)) andalso
        is_alive_sub(Pid).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when ?IS_LOCAL_PID(Pid) ->
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    [] =/= ets:lookup(?ALIVE_SUBS, Pid).

delete_route_if_needed({Group, Topic}) ->
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true -> ok;
        false -> ok = emqx_router:do_delete_route(Topic, {Group, node()})
    end.

maps_find_by(Map, Predicate) when is_map(Map) ->
    maps_find_by(maps:iterator(Map), Predicate);

maps_find_by(Iterator, Predicate) ->
    case maps:next(Iterator) of
      none -> error;
      {Key, Value, NewIterator} ->
        case Predicate(Key, Value) of
          true -> {ok, Key, Value};
          false -> maps_find_by(NewIterator, Predicate)
        end
    end.

maps_put_new(Map, Key, Value) ->
   case Map of
     #{Key := _} -> Map;
     _ -> Map#{Key => Value}
   end.
