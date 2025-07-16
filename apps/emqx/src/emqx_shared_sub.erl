%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% TODO
%% This server might crash during the runtime. Naturally, `?SHARED_SUBSCRIPTION`
%% table can be used to reconstruct local state in `init/1`.

-module(emqx_shared_sub).

-behaviour(gen_server).

-include("emqx_schema.hrl").
-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_shared_sub.hrl").
-include("logger.hrl").
-include("types.hrl").

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Mnesia bootstrap
-export([create_tables/0]).

%% APIs
-export([
    start_link/0,
    post_start/0
]).

-export([
    subscribe/3,
    unsubscribe/3,
    purge_node/1
]).

-export([
    dispatch/3,
    dispatch/4,
    do_dispatch_with_ack/4,
    redispatch/1
]).

-export([
    maybe_ack/1,
    maybe_nack_dropped/1,
    nack_no_connection/1,
    is_ack_required/1
]).

%% for testing
-ifdef(TEST).
-export([
    subscribers/2,
    strategy/1,
    initial_sticky_pick/1
]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports
-export([
    %% RPC Targets:
    init_monitors/0,
    %% Other:
    update_stats/0
]).

-export_type([strategy/0]).

-type strategy() ::
    random
    | round_robin
    | round_robin_per_group
    | sticky
    | local
    | hash_clientid
    | hash_topic.

-export_type([initial_sticky_pick/0]).

-type initial_sticky_pick() ::
    random
    | local
    | hash_clientid
    | hash_topic.

-define(SERVER, ?MODULE).

-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(ACK, shared_sub_ack).
-define(NACK(Reason), {shared_sub_nack, Reason}).
-define(NO_ACK, no_ack).
-define(SUBSCRIBER_DOWN, noproc).

-type redispatch_to() :: ?REDISPATCH_TO(emqx_types:group(), emqx_types:topic()).

-record(state, {pmon}).

-record(?SHARED_SUBSCRIPTION, {group, topic, subpid}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?SHARED_SUBSCRIPTION, [
        {type, bag},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, ?SHARED_SUBSCRIPTION},
        {attributes, record_info(fields, ?SHARED_SUBSCRIPTION)}
    ]),
    [?SHARED_SUBSCRIPTION].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec post_start() -> ignore.
post_start() ->
    %% There may be leftovers from older incarnation of this node, clean them up:
    %% Might take lots of time.
    _ = mria:wait_for_tables([?SHARED_SUBSCRIPTION]),
    ok = purge_node(node()),
    ignore.

%% @doc Subscribe `SubPid` to the shared subscription group-topic.
%% This call *is not perfectly idempotent*, and currently `emqx_broker`
%% guarantees that it's called only once (per new shared subscription).
-spec subscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid}).

%% @doc Unsubscribe `SubPid` from the shared subscription group-topic.
%% This call *is not perfectly idempotent*, and currently `emqx_broker`
%% guarantees that it's called only once (per existing shared subscription).
-spec unsubscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).

-spec purge_node(node()) -> ok | false.
purge_node(Node) ->
    %% TODO: Refactor `emqx_router_helper` to avoid this hack.
    erlang:whereis(?SERVER) =/= undefined andalso
        gen_server:call(?SERVER, {purge, Node}, infinity).

record(Group, Topic, SubPid) ->
    #?SHARED_SUBSCRIPTION{group = Group, topic = Topic, subpid = SubPid}.

-spec dispatch(emqx_types:group(), emqx_types:topic(), emqx_types:delivery()) ->
    emqx_types:deliver_result().
dispatch(Group, Topic, Delivery) ->
    dispatch(Group, Topic, Delivery, _FailedSubs = #{}).

dispatch(Group, Topic, Delivery = #delivery{message = Msg}, FailedSubs) ->
    #message{from = ClientId, topic = SourceTopic} = Msg,
    case pick(strategy(Group), ClientId, SourceTopic, Group, Topic, FailedSubs) of
        false ->
            {error, no_subscribers};
        {Type, SubPid} ->
            Msg1 = with_redispatch_to(Msg, Group, Topic),
            case do_dispatch(SubPid, Group, Topic, Msg1, Type) of
                ok ->
                    {ok, 1};
                {error, Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Group, Topic, Delivery, FailedSubs#{SubPid => Reason})
            end
    end.

-spec strategy(emqx_types:group()) -> strategy().
strategy(Group) ->
    try binary_to_existing_atom(Group) of
        GroupAtom ->
            Key = [broker, shared_subscription_group, GroupAtom, strategy],
            case emqx:get_config(Key, ?CONFIG_NOT_FOUND_MAGIC) of
                ?CONFIG_NOT_FOUND_MAGIC -> get_default_shared_subscription_strategy();
                Strategy -> Strategy
            end
    catch
        error:badarg ->
            get_default_shared_subscription_strategy()
    end.

-spec initial_sticky_pick(emqx_types:group()) -> initial_sticky_pick().
initial_sticky_pick(Group) ->
    try binary_to_existing_atom(Group) of
        GroupAtom ->
            Key = [broker, shared_subscription_group, GroupAtom, initial_sticky_pick],
            case emqx:get_config(Key, ?CONFIG_NOT_FOUND_MAGIC) of
                ?CONFIG_NOT_FOUND_MAGIC -> get_default_shared_subscription_initial_sticky_pick();
                InitialStickyPick -> InitialStickyPick
            end
    catch
        error:badarg ->
            get_default_shared_subscription_initial_sticky_pick()
    end.

-spec ack_enabled() -> boolean().
ack_enabled() ->
    emqx:get_config([broker, shared_dispatch_ack_enabled], false).

do_dispatch(SubPid, _Group, Topic, Msg, _Type) when SubPid =:= self() ->
    %% Deadlock otherwise
    SubPid ! {deliver, Topic, Msg},
    ok;
%% return either 'ok' (when everything is fine) or 'error'
do_dispatch(SubPid, _Group, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    send(SubPid, Topic, {deliver, Topic, Msg});
do_dispatch(SubPid, _Group, Topic, Msg, retry) ->
    %% Retry implies all subscribers nack:ed, send again without ack
    send(SubPid, Topic, {deliver, Topic, Msg});
do_dispatch(SubPid, Group, Topic, Msg, fresh) ->
    case ack_enabled() of
        true ->
            %% TODO: delete this case after 5.1.0
            do_dispatch_with_ack(SubPid, Group, Topic, Msg);
        false ->
            send(SubPid, Topic, {deliver, Topic, Msg})
    end.

-spec do_dispatch_with_ack(pid(), emqx_types:group(), emqx_types:topic(), emqx_types:message()) ->
    ok | {error, _}.
do_dispatch_with_ack(SubPid, Group, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    %% FIXME: replace with regular send in 5.2
    send(SubPid, Topic, {deliver, Topic, with_group_ack(Msg, Group, Sender, Ref)}),
    Timeout =
        case Msg#message.qos of
            ?QOS_2 -> infinity;
            _ -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS)
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
        erlang:demonitor(Ref, [flush])
    end.

with_group_ack(Msg, Group, Sender, Ref) ->
    emqx_message:set_headers(#{shared_dispatch_ack => {Group, Sender, Ref}}, Msg).

-spec without_group_ack(emqx_types:message()) -> emqx_types:message().
without_group_ack(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?NO_ACK}, Msg).

get_group_ack(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?NO_ACK).

%% always add `redispatch_to` header to the message
%% for QOS_0 msgs, redispatch_to is not needed and filtered out in is_redispatch_needed/1
with_redispatch_to(Msg, Group, Topic) ->
    emqx_message:set_headers(#{redispatch_to => ?REDISPATCH_TO(Group, Topic)}, Msg).

%% @hidden Redispatch is needed only for the messages which not QOS_0
is_redispatch_needed(#message{qos = ?QOS_0}) ->
    false;
is_redispatch_needed(#message{headers = #{redispatch_to := ?REDISPATCH_TO(_, _)}}) ->
    true;
is_redispatch_needed(#message{}) ->
    false.

%% @doc Redispatch shared deliveries to other members in the group.
redispatch(Messages0) ->
    Messages = lists:filter(fun is_redispatch_needed/1, Messages0),
    case length(Messages) of
        L when L > 0 ->
            ?SLOG(info, #{
                msg => "redispatching_shared_subscription_message",
                count => L
            }),
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
    %% Self is terminating, it makes no sense to loop-back the dispatch
    FailedSubs = #{self() => ?SUBSCRIBER_DOWN},
    dispatch(Group, Topic, Delivery, FailedSubs).

%% @hidden Return the `redispatch_to` group-topic in the message header.
%% `false` is returned if the message is not a shared dispatch.
%% or when it's a QoS 0 message.
-spec get_redispatch_to(emqx_types:message()) -> redispatch_to() | false.
get_redispatch_to(Msg) ->
    emqx_message:get_header(redispatch_to, Msg, false).

-spec is_ack_required(emqx_types:message()) -> boolean().
is_ack_required(Msg) -> ?NO_ACK =/= get_group_ack(Msg).

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
    All = subscribers(Group, Topic, FailedSubs),
    case is_active_sub(Sub0, FailedSubs, All) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            {fresh, Sub0};
        false ->
            %% pick the initial subscriber by the configured strategy
            InitialStrategy = initial_sticky_pick(Group),
            FailedSubs1 = FailedSubs#{Sub0 => ?SUBSCRIBER_DOWN},
            Res = do_pick(All, InitialStrategy, ClientId, SourceTopic, Group, Topic, FailedSubs1),
            case Res of
                {_, Sub} ->
                    %% stick to whatever pick result
                    erlang:put({shared_sub_sticky, Group, Topic}, Sub);
                _ ->
                    ok
            end,
            Res
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    All = subscribers(Group, Topic, FailedSubs),
    do_pick(All, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

do_pick([], _Strategy, _ClientId, _SourceTopic, _Group, _Topic, _FailedSubs) ->
    false;
do_pick(All, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    case lists:filter(fun(Sub) -> not maps:is_key(Sub, FailedSubs) end, All) of
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
    Rem + 1;
do_pick_subscriber(Group, Topic, round_robin_per_group, _ClientId, _SourceTopic, Count) ->
    %% reset the counter to 1 if counter > subscriber count to avoid the counter to grow larger
    %% than the current subscriber count.
    %% if no counter for the given group topic exists - due to a configuration change - create a new one starting at 0
    ets:update_counter(?SHARED_SUBS_ROUND_ROBIN_COUNTER, {Group, Topic}, {2, 1, Count, 1}, {
        {Group, Topic}, 0
    }).

%% Select ETS table to get all subscriber pids which are not down.
subscribers(Group, Topic, FailedSubs) ->
    lists:filter(
        fun(P) ->
            ?SUBSCRIBER_DOWN =/= maps:get(P, FailedSubs, false)
        end,
        subscribers(Group, Topic)
    ).

%% Select ETS table to get all subscriber pids.
subscribers(Group, Topic) ->
    ets:select(?SHARED_SUBSCRIPTION, [{{?SHARED_SUBSCRIPTION, Group, Topic, '$1'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Set up tables:
    ok = mria:wait_for_tables([?SHARED_SUBSCRIPTION]),
    ok = emqx_utils_ets:new(?SHARED_SUBSCRIBER, [protected, bag]),
    ok = emqx_utils_ets:new(?SHARED_SUBS_ROUND_ROBIN_COUNTER, [
        public, set, {write_concurrency, true}
    ]),
    %% Let `emqx_stats` periodically update stats instead of relying on mnesia events:
    ok = emqx_stats:update_interval(?MODULE, fun ?MODULE:update_stats/0),
    {ok, #state{pmon = pmon_new()}}.

%% Deprecated, left for backwards compatibility.
%% Should be removed in the next release.
init_monitors() ->
    pmon_new().

handle_call({subscribe, Group, Topic, SubPid}, _From, State = #state{pmon = PMon}) ->
    handle_subscribe(Group, Topic, SubPid),
    update_stats(),
    NPMon = pmon_inc_monitor(SubPid, PMon),
    {reply, ok, State#state{pmon = NPMon}};
handle_call({unsubscribe, Group, Topic, SubPid}, _From, State = #state{pmon = PMon}) ->
    handle_unsubscribe(Group, Topic, SubPid),
    update_stats(),
    case pmon_dec_monitor(SubPid, PMon) of
        NPMon = #{} ->
            ok;
        {error, inconsistent} ->
            NPMon = PMon,
            ?tp(warning, "shared_subcriber_monitor_inconsistent", #{
                subpid => SubPid,
                group => Group,
                topic => Topic
            })
    end,
    {reply, ok, State#state{pmon = NPMon}};
handle_call({purge, Node}, _From, State) ->
    handle_purge_node(Node),
    update_stats(),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};
handle_info({'DOWN', _MRef, process, SubPid, Reason}, State = #state{pmon = PMon}) ->
    ?SLOG(debug, #{msg => "shared_subscriber_down", sub_pid => SubPid, reason => Reason}),
    cleanup_down(SubPid),
    update_stats(),
    NPMon = pmon_erase(SubPid, PMon),
    {noreply, State#state{pmon = NPMon}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

send(Pid, Topic, Msg) ->
    Node = node(Pid),
    _ =
        case Node =:= node() of
            true ->
                Pid ! Msg;
            false ->
                emqx_shared_sub_proto_v1:send(Node, Pid, Topic, Msg)
        end,
    ok.

handle_subscribe(Group, Topic, SubPid) ->
    mria:dirty_write(?SHARED_SUBSCRIPTION, record(Group, Topic, SubPid)),
    case ets:member(?SHARED_SUBSCRIBER, {Group, Topic}) of
        true ->
            ok;
        false ->
            ok = emqx_router:do_add_route(Topic, {Group, node()}),
            emqx_external_broker:add_shared_route(Topic, Group)
    end,
    maybe_insert_round_robin_count({Group, Topic}),
    ets:insert(?SHARED_SUBSCRIBER, {{Group, Topic}, SubPid}).

maybe_insert_round_robin_count({Group, _Topic} = GroupTopic) ->
    strategy(Group) =:= round_robin_per_group andalso
        ets:insert(?SHARED_SUBS_ROUND_ROBIN_COUNTER, {GroupTopic, 0}).

maybe_delete_round_robin_count({Group, _Topic} = GroupTopic) ->
    strategy(Group) =:= round_robin_per_group andalso
        ets:delete(?SHARED_SUBS_ROUND_ROBIN_COUNTER, GroupTopic).

handle_unsubscribe(Group, Topic, SubPid) ->
    handle_unsubscribe(record(Group, Topic, SubPid)).

handle_unsubscribe(
    Record = #?SHARED_SUBSCRIPTION{
        group = Group,
        topic = Topic,
        subpid = SubPid
    }
) ->
    GroupTopic = {Group, Topic},
    mria:dirty_delete_object(?SHARED_SUBSCRIPTION, Record),
    true = ets:delete_object(?SHARED_SUBSCRIBER, {GroupTopic, SubPid}),
    if_no_more_subscribers(GroupTopic, fun() ->
        maybe_delete_round_robin_count(GroupTopic),
        delete_route(Group, Topic)
    end).

if_no_more_subscribers(GroupTopic, Fn) ->
    case ets:member(?SHARED_SUBSCRIBER, GroupTopic) of
        true -> ok;
        false -> Fn()
    end,
    ok.

cleanup_down(SubPid) ->
    Records = mnesia:dirty_match_object(#?SHARED_SUBSCRIPTION{_ = '_', subpid = SubPid}),
    lists:foreach(fun handle_unsubscribe/1, Records).

handle_purge_node(Node) ->
    Records = mnesia:dirty_select(
        ?SHARED_SUBSCRIPTION,
        ets:fun2ms(
            fun(Record = #?SHARED_SUBSCRIPTION{subpid = SubPid}) when node(SubPid) == Node ->
                Record
            end
        )
    ),
    handle_purge_node(Node, Records).

handle_purge_node(Node, Records = [_ | _]) ->
    Routes = lists:foldl(
        fun(Record = #?SHARED_SUBSCRIPTION{group = Group, topic = Topic}, Acc) ->
            mria:dirty_delete_object(?SHARED_SUBSCRIPTION, Record),
            maps:put({Group, Topic}, true, Acc)
        end,
        #{},
        Records
    ),
    maps:foreach(
        fun({Group, Topic}, _) ->
            delete_route(Group, Topic, Node)
        end,
        Routes
    );
handle_purge_node(_Node, []) ->
    ok.

update_stats() ->
    emqx_stats:setstat(
        'subscriptions.shared.count',
        'subscriptions.shared.max',
        ets:info(?SHARED_SUBSCRIPTION, size)
    ).

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs, All) ->
    lists:member(Pid, All) andalso
        (not maps:is_key(Pid, FailedSubs)) andalso
        is_alive_sub(Pid).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when node(Pid) == node() ->
    %% The race is when the pid is actually down cleanup_down is not evaluated yet.
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    %% When process is not local, the best guess is it's alive.
    emqx_router_helper:is_routable(node(Pid)).

delete_route(Group, Topic) ->
    delete_route(Group, Topic, node()).

delete_route(Group, Topic, Node) ->
    ok = emqx_router:do_delete_route(Topic, {Group, Node}),
    _ = emqx_external_broker:delete_shared_route(Topic, Group),
    ok.

get_default_shared_subscription_strategy() ->
    emqx:get_config([mqtt, shared_subscription_strategy]).

get_default_shared_subscription_initial_sticky_pick() ->
    emqx:get_config([mqtt, shared_subscription_initial_sticky_pick]).

%%--------------------------------------------------------------------

pmon_new() ->
    #{}.

pmon_inc_monitor(Pid, PMon) ->
    case PMon of
        #{Pid := {MRef, N}} ->
            %% Process is already being monitored.
            %% Bump number of monitor requests.
            PMon#{Pid := {MRef, N + 1}};
        #{} ->
            %% Process is not being monitored yet.
            MRef = erlang:monitor(process, Pid),
            PMon#{Pid => {MRef, 1}}
    end.

pmon_dec_monitor(Pid, PMon) ->
    case PMon of
        #{Pid := {MRef, N}} when N =< 1 ->
            %% Process is being monitored once.
            _ = erlang:demonitor(MRef, [flush]),
            maps:remove(Pid, PMon);
        #{Pid := {MRef, N}} ->
            %% Decrement number of monitor requests.
            PMon#{Pid := {MRef, N - 1}};
        #{} ->
            {error, inconsistent}
    end.

pmon_erase(Pid, PMon) ->
    maps:remove(Pid, PMon).
