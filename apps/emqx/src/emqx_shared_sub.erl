%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

%% APIs
-export([start_link/0]).

-export([
    subscribe/3,
    unsubscribe/3
]).

-export([
    dispatch/3,
    dispatch/4
]).

-export([
    maybe_ack/1,
    maybe_nack_dropped/1,
    nack_no_connection/1,
    is_ack_required/1,
    get_group/1
]).

%% for testing
-ifdef(TEST).
-export([
    subscribers/2,
    strategy/1
]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([strategy/0]).

-type strategy() ::
    random
    | round_robin
    | sticky
    | local
    %% same as hash_clientid, backward compatible
    | hash
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

-record(state, {pmon}).

-record(emqx_shared_subscription, {group, topic, subpid}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {type, bag},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, emqx_shared_subscription},
        {attributes, record_info(fields, emqx_shared_subscription)}
    ]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec subscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid}).

-spec unsubscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).

record(Group, Topic, SubPid) ->
    #emqx_shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

-spec dispatch(emqx_types:group(), emqx_types:topic(), emqx_types:delivery()) ->
    emqx_types:deliver_result().
dispatch(Group, Topic, Delivery) ->
    dispatch(Group, Topic, Delivery, _FailedSubs = []).

dispatch(Group, Topic, Delivery = #delivery{message = Msg}, FailedSubs) ->
    #message{from = ClientId, topic = SourceTopic} = Msg,
    case pick(strategy(Group), ClientId, SourceTopic, Group, Topic, FailedSubs) of
        false ->
            {error, no_subscribers};
        {Type, SubPid} ->
            case do_dispatch(SubPid, Group, Topic, Msg, Type) of
                ok ->
                    {ok, 1};
                {error, _Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Group, Topic, Delivery, [SubPid | FailedSubs])
            end
    end.

-spec strategy(emqx_topic:group()) -> strategy().
strategy(Group) ->
    case emqx:get_config([broker, shared_subscription_group, Group, strategy], undefined) of
        undefined -> emqx:get_config([broker, shared_subscription_strategy]);
        Strategy -> Strategy
    end.

-spec ack_enabled() -> boolean().
ack_enabled() ->
    emqx:get_config([broker, shared_dispatch_ack_enabled]).

do_dispatch(SubPid, _Group, Topic, Msg, _Type) when SubPid =:= self() ->
    %% Deadlock otherwise
    SubPid ! {deliver, Topic, Msg},
    ok;
%% return either 'ok' (when everything is fine) or 'error'
do_dispatch(SubPid, _Group, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    SubPid ! {deliver, Topic, Msg},
    ok;
do_dispatch(SubPid, _Group, Topic, Msg, retry) ->
    %% Retry implies all subscribers nack:ed, send again without ack
    SubPid ! {deliver, Topic, Msg},
    ok;
do_dispatch(SubPid, Group, Topic, Msg, fresh) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Group, Topic, Msg);
        false ->
            SubPid ! {deliver, Topic, Msg},
            ok
    end.

dispatch_with_ack(SubPid, Group, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    SubPid ! {deliver, Topic, with_group_ack(Msg, Group, Sender, Ref)},
    Timeout =
        case Msg#message.qos of
            ?QOS_1 -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS);
            ?QOS_2 -> infinity
        end,
    try
        receive
            {Ref, ?ACK} ->
                ok;
            {Ref, ?NACK(Reason)} ->
                %% the receive session may nack this message when its queue is full
                {error, Reason};
            {'DOWN', Ref, process, SubPid, Reason} ->
                {error, Reason}
        after Timeout ->
            {error, timeout}
        end
    after
        ok = emqx_pmon:demonitor(Ref)
    end.

with_group_ack(Msg, Group, Sender, Ref) ->
    emqx_message:set_headers(#{shared_dispatch_ack => {Group, Sender, Ref}}, Msg).

-spec without_group_ack(emqx_types:message()) -> emqx_types:message().
without_group_ack(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?NO_ACK}, Msg).

get_group_ack(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?NO_ACK).

-spec is_ack_required(emqx_types:message()) -> boolean().
is_ack_required(Msg) -> ?NO_ACK =/= get_group_ack(Msg).

-spec get_group(emqx_types:message()) -> {ok, any()} | error.
get_group(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK -> error;
        {Group, _Sender, _Ref} -> {ok, Group}
    end.

%% @doc Negative ack dropped message due to inflight window or message queue being full.
-spec maybe_nack_dropped(emqx_types:message()) -> boolean().
maybe_nack_dropped(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK -> false;
        {_Group, Sender, Ref} -> ok == nack(Sender, Ref, dropped)
    end.

%% @doc Negative ack message due to connection down.
%% Assuming this function is always called when ack is required
%% i.e is_ack_required returned true.
-spec nack_no_connection(emqx_types:message()) -> ok.
nack_no_connection(Msg) ->
    {_Group, Sender, Ref} = get_group_ack(Msg),
    nack(Sender, Ref, no_connection).

-spec nack(pid(), reference(), dropped | no_connection) -> ok.
nack(Sender, Ref, Reason) ->
    Sender ! {Ref, ?NACK(Reason)},
    ok.

-spec maybe_ack(emqx_types:message()) -> emqx_types:message().
maybe_ack(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK ->
            Msg;
        {_Group, Sender, Ref} ->
            Sender ! {Ref, ?ACK},
            without_group_ack(Msg)
    end.

pick(sticky, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    case is_active_sub(Sub0, FailedSubs) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            {fresh, Sub0};
        false ->
            %% randomly pick one for the first message
            {Type, Sub} = do_pick(random, ClientId, SourceTopic, Group, Topic, [Sub0 | FailedSubs]),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            {Type, Sub}
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    All = subscribers(Group, Topic),
    case All -- FailedSubs of
        [] when All =:= [] ->
            %% Genuinely no subscriber
            false;
        [] ->
            %% All offline? pick one anyway
            {retry, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, All)};
        Subs ->
            %% More than one available
            {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs)}
    end.

pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, [Sub]) ->
    Sub;
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
    Rem =
        case erlang:get({shared_sub_round_robin, Group, Topic}) of
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
    ok = mria:wait_for_tables([?TAB]),
    {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
    {atomic, PMon} = mria:transaction(?SHARED_SUB_SHARD, fun init_monitors/0),
    ok = emqx_tables:new(?SHARED_SUBS, [protected, bag]),
    ok = emqx_tables:new(?ALIVE_SUBS, [protected, set, {read_concurrency, true}]),
    {ok, update_stats(#state{pmon = PMon})}.

init_monitors() ->
    mnesia:foldl(
        fun(#emqx_shared_subscription{subpid = SubPid}, Mon) ->
            emqx_pmon:monitor(SubPid, Mon)
        end,
        emqx_pmon:new(),
        ?TAB
    ).

handle_call({subscribe, Group, Topic, SubPid}, _From, State = #state{pmon = PMon}) ->
    mria:dirty_write(?TAB, record(Group, Topic, SubPid)),
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true -> ok;
        false -> ok = emqx_router:do_add_route(Topic, {Group, node()})
    end,
    ok = maybe_insert_alive_tab(SubPid),
    true = ets:insert(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    {reply, ok, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};
handle_call({unsubscribe, Group, Topic, SubPid}, _From, State) ->
    mria:dirty_delete_object(?TAB, record(Group, Topic, SubPid)),
    true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    delete_route_if_needed({Group, Topic}),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info(
    {mnesia_table_event, {write, #emqx_shared_subscription{subpid = SubPid}, _}},
    State = #state{pmon = PMon}
) ->
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
handle_info({'DOWN', _MRef, process, SubPid, Reason}, State = #state{pmon = PMon}) ->
    ?SLOG(info, #{msg => "shared_subscriber_down", sub_pid => SubPid, reason => Reason}),
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
maybe_insert_alive_tab(Pid) when is_pid(Pid) ->
    ets:insert(?ALIVE_SUBS, {Pid}),
    ok.

cleanup_down(SubPid) ->
    ?IS_LOCAL_PID(SubPid) orelse ets:delete(?ALIVE_SUBS, SubPid),
    lists:foreach(
        fun(Record = #emqx_shared_subscription{topic = Topic, group = Group}) ->
            ok = mria:dirty_delete_object(?TAB, Record),
            true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
            delete_route_if_needed({Group, Topic})
        end,
        mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})
    ).

update_stats(State) ->
    emqx_stats:setstat(
        'subscriptions.shared.count',
        'subscriptions.shared.max',
        ets:info(?TAB, size)
    ),
    State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs) ->
    is_alive_sub(Pid) andalso not lists:member(Pid, FailedSubs).

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
