%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    unsubscribe_down/3,
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
    initial_sticky_pick/1,
    members_cache_ttl_ms/0
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

%% Node-local cache of the members of each shared subscription.
%% Row: {{Group, Topic}, Stamp, AllPids, LocalPids}
%%   Stamp = {BuiltAt, Unique}; BuiltAt is erlang:monotonic_time(millisecond),
%%   taken before the members are read from ?SHARED_SUBSCRIPTION.
%%   AllPids: tuple of all members in the cluster, in ?SHARED_SUBSCRIPTION order.
%%   LocalPids: tuple of the members on this node, in the same order.
%% A row is used for at most ?MEMBERS_CACHE_TTL_MS after BuiltAt.
-define(MEMBERS_CACHE, emqx_shared_sub_members_cache).
-define(MEMBERS_CACHE_TTL_MS, 100).
-define(MEMBERS_CACHE_SWEEP_INTERVAL_MS, 10_000).
-define(MEMBERS_CACHE_SWEEP, members_cache_sweep).
-define(IS_FRESH_STAMP(STAMP, NOW),
    (is_tuple(STAMP) andalso NOW - element(1, STAMP) < ?MEMBERS_CACHE_TTL_MS)
).

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

-spec subscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
subscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid}).

-spec unsubscribe(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).

-spec unsubscribe_down(emqx_types:group(), emqx_types:topic(), pid()) -> ok.
unsubscribe_down(Group, Topic, SubPid) when is_pid(SubPid) ->
    gen_server:cast(?SERVER, {unsubscribe, Group, Topic, SubPid}).

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
    {Sub0, Stamp0} = get_sticky(Group, Topic),
    case is_pid(Sub0) andalso is_sticky_still_valid(Sub0, Stamp0, Group, Topic, FailedSubs) of
        true ->
            {fresh, Sub0};
        false ->
            Members = members(Group, Topic),
            pick_sticky(Sub0, Members, ClientId, SourceTopic, Group, Topic, FailedSubs)
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    Members = members(Group, Topic),
    pick_member(Members, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

pick_sticky(Sub0, Members, ClientId, SourceTopic, Group, Topic, FailedSubs) when is_pid(Sub0) ->
    case is_active_member(Sub0, FailedSubs, Members) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            ok = put_sticky(Group, Topic, Sub0, Members),
            {fresh, Sub0};
        false ->
            %% pick the initial subscriber by the configured strategy,
            %% fall back to Sub0 when no other member is left
            All = live_members(Members, FailedSubs),
            FailedSubs1 = FailedSubs#{Sub0 => ?SUBSCRIBER_DOWN},
            InitialStrategy = initial_sticky_pick(Group),
            Res = do_pick(All, InitialStrategy, ClientId, SourceTopic, Group, Topic, FailedSubs1),
            stick_to(Res, Group, Topic, Members)
    end;
pick_sticky(_NoSub, Members, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    InitialStrategy = initial_sticky_pick(Group),
    Res = pick_member(Members, InitialStrategy, ClientId, SourceTopic, Group, Topic, FailedSubs),
    stick_to(Res, Group, Topic, Members).

%% stick to whatever pick result
stick_to({_Type, Sub} = Res, Group, Topic, Members) ->
    ok = put_sticky(Group, Topic, Sub, Members),
    Res;
stick_to(false, _Group, _Topic, _Members) ->
    false.

pick_member({_Stamp, {}, _Local}, _Strategy, _ClientId, _SourceTopic, _Group, _Topic, _FailedSubs) ->
    false;
pick_member({_Stamp, All, Local}, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) when
    map_size(FailedSubs) =:= 0
->
    {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, All, Local)};
pick_member(Members, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    %% Retry path: some members failed already.
    All = live_members(Members, FailedSubs),
    do_pick(All, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

%% The members as a list, without the ones that failed because they are down.
live_members({_Stamp, All, _Local}, FailedSubs) ->
    lists:filter(
        fun(P) -> ?SUBSCRIBER_DOWN =/= maps:get(P, FailedSubs, false) end,
        tuple_to_list(All)
    ).

do_pick([], _Strategy, _ClientId, _SourceTopic, _Group, _Topic, _FailedSubs) ->
    false;
do_pick(All, Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    case lists:filter(fun(Sub) -> not maps:is_key(Sub, FailedSubs) end, All) of
        [] ->
            %% All offline? pick one anyway
            {retry, pick_subscriber_from_list(Group, Topic, Strategy, ClientId, SourceTopic, All)};
        Subs ->
            %% More than one available
            {fresh, pick_subscriber_from_list(Group, Topic, Strategy, ClientId, SourceTopic, Subs)}
    end.

pick_subscriber_from_list(Group, Topic, Strategy, ClientId, SourceTopic, Subs) ->
    Local =
        case Strategy of
            local -> list_to_tuple(local_pids(Subs));
            _ -> {}
        end,
    pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, list_to_tuple(Subs), Local).

%% Subs and LocalSubs are tuples. LocalSubs is the members of Subs on this node,
%% it is only used by the `local' strategy.
pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, {Sub}, _LocalSubs) ->
    Sub;
pick_subscriber(Group, Topic, local, ClientId, SourceTopic, Subs, {}) ->
    pick_subscriber(Group, Topic, random, ClientId, SourceTopic, Subs, {});
pick_subscriber(Group, Topic, local, ClientId, SourceTopic, _Subs, LocalSubs) ->
    pick_subscriber(Group, Topic, random, ClientId, SourceTopic, LocalSubs, LocalSubs);
pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs, _LocalSubs) ->
    Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, tuple_size(Subs)),
    element(Nth, Subs).

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

%% The sticky subscriber is kept in the publisher's process dictionary, together with
%% the stamp of the cache row that it was last found in.
get_sticky(Group, Topic) ->
    case erlang:get({shared_sub_sticky, Group, Topic}) of
        {Sub, Stamp} -> {Sub, Stamp};
        %% Stored by an older version of this module.
        Sub -> {Sub, undefined}
    end.

put_sticky(Group, Topic, Sub, {Stamp, _All, _Local}) ->
    _ = erlang:put({shared_sub_sticky, Group, Topic}, {Sub, Stamp}),
    ok.

%% Check the sticky subscriber without a copy of the member tuples:
%% when the cache row is still the one that the subscriber was found in, it is still a member.
is_sticky_still_valid(Sub, Stamp, Group, Topic, FailedSubs) ->
    Stamp =/= undefined andalso
        Stamp =:= cached_stamp({Group, Topic}) andalso
        is_fresh_stamp(Stamp, now_ms()) andalso
        (not maps:is_key(Sub, FailedSubs)) andalso
        is_alive_sub(Sub).

%% Return 'true' if the subscriber is a member AND not in the failed list AND alive.
is_active_member(Pid, FailedSubs, {_Stamp, All, _Local}) ->
    is_tuple_member(Pid, All, tuple_size(All)) andalso
        (not maps:is_key(Pid, FailedSubs)) andalso
        is_alive_sub(Pid).

is_tuple_member(_X, _Tuple, 0) ->
    false;
is_tuple_member(X, Tuple, I) ->
    element(I, Tuple) =:= X orelse is_tuple_member(X, Tuple, I - 1).

%%--------------------------------------------------------------------
%% Member cache
%%--------------------------------------------------------------------

%% Return the members of a shared subscription: {Stamp, AllPids, LocalPids}.
members(Group, Topic) ->
    Key = {Group, Topic},
    Now = now_ms(),
    case cache_lookup(Key) of
        [{_, Stamp, All, Local}] when ?IS_FRESH_STAMP(Stamp, Now) ->
            {Stamp, All, Local};
        _ ->
            build_members(Key, Now)
    end.

build_members({Group, Topic} = Key, Now) ->
    Stamp = {Now, erlang:unique_integer()},
    All = subscribers(Group, Topic),
    Local = local_pids(All),
    Members = {Stamp, list_to_tuple(All), list_to_tuple(Local)},
    _ =
        All =/= [] andalso
            is_local_view_settled(Key, Local) andalso
            cache_insert(erlang:insert_element(1, Members, Key)),
    Members.

%% The cache table is owned by the server, it is missing while the server restarts.
cache_lookup(Key) ->
    try
        ets:lookup(?MEMBERS_CACHE, Key)
    catch
        error:badarg -> []
    end.

cache_insert(Row) ->
    try
        ets:insert(?MEMBERS_CACHE, Row)
    catch
        error:badarg -> false
    end.

%% The local members in the replica of ?SHARED_SUBSCRIPTION must agree with
%% ?SHARED_SUBSCRIBER, which this node's server updates at once. They disagree on a
%% replicant until the replication of a local subscribe or unsubscribe arrives,
%% and the row is not cached until then, so that a local change is not hidden for the TTL.
is_local_view_settled(Key, Local) ->
    lists:sort(Local) =:= lists:sort([Pid || {_, Pid} <- ets:lookup(?SHARED_SUBSCRIBER, Key)]).

cached_stamp(Key) ->
    try
        ets:lookup_element(?MEMBERS_CACHE, Key, 2, undefined)
    catch
        error:badarg -> undefined
    end.

is_fresh_stamp(Stamp, Now) when ?IS_FRESH_STAMP(Stamp, Now) -> true;
is_fresh_stamp(_Stamp, _Now) -> false.

local_pids(Pids) ->
    Node = node(),
    [Pid || Pid <- Pids, node(Pid) =:= Node].

now_ms() ->
    erlang:monotonic_time(millisecond).

invalidate_members(Key) ->
    ets:delete(?MEMBERS_CACHE, Key).

sweep_members_cache() ->
    Cutoff = now_ms() - ?MEMBERS_CACHE_TTL_MS,
    ets:select_delete(?MEMBERS_CACHE, [
        {{'_', {'$1', '_'}, '_', '_'}, [{'<', '$1', Cutoff}], [true]}
    ]).

schedule_sweep() ->
    _ = erlang:send_after(?MEMBERS_CACHE_SWEEP_INTERVAL_MS, self(), ?MEMBERS_CACHE_SWEEP),
    ok.

members_cache_ttl_ms() ->
    ?MEMBERS_CACHE_TTL_MS.

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
    %% Written by publishers on a cache miss, and by this server to invalidate.
    ok = emqx_utils_ets:new(?MEMBERS_CACHE, [public, set, {read_concurrency, true}]),
    ok = schedule_sweep(),
    %% Let `emqx_stats` periodically update stats instead of relying on mnesia events:
    ok = emqx_stats:update_interval(?MODULE, fun ?MODULE:update_stats/0),
    {ok, #state{}}.

%% Deprecated, left for backwards compatibility.
%% Should be removed in the next release.
init_monitors() ->
    emqx_pmon:new().

handle_call({subscribe, Group, Topic, SubPid}, _From, State) ->
    handle_subscribe(Group, Topic, SubPid),
    update_stats(),
    {reply, ok, State};
handle_call({unsubscribe, Group, Topic, SubPid}, _From, State) ->
    handle_unsubscribe(Group, Topic, SubPid),
    update_stats(),
    {reply, ok, State};
handle_call({purge, Node}, _From, State) ->
    handle_purge_node(Node),
    update_stats(),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast({unsubscribe, Group, Topic, SubPid}, State) ->
    handle_unsubscribe(Group, Topic, SubPid),
    update_stats(),
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info(?MEMBERS_CACHE_SWEEP, State) ->
    _ = sweep_members_cache(),
    ok = schedule_sweep(),
    {noreply, State};
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
    true = ets:insert(?SHARED_SUBSCRIBER, {{Group, Topic}, SubPid}),
    true = invalidate_members({Group, Topic}).

maybe_insert_round_robin_count({Group, _Topic} = GroupTopic) ->
    strategy(Group) =:= round_robin_per_group andalso
        ets:insert(?SHARED_SUBS_ROUND_ROBIN_COUNTER, {GroupTopic, 0}).

maybe_delete_round_robin_count({Group, _Topic} = GroupTopic) ->
    strategy(Group) =:= round_robin_per_group andalso
        ets:delete(?SHARED_SUBS_ROUND_ROBIN_COUNTER, GroupTopic).

handle_unsubscribe(Group, Topic, SubPid) ->
    GroupTopic = {Group, Topic},
    mria:dirty_delete_object(?SHARED_SUBSCRIPTION, record(Group, Topic, SubPid)),
    true = ets:delete_object(?SHARED_SUBSCRIBER, {GroupTopic, SubPid}),
    true = invalidate_members(GroupTopic),
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

handle_purge_node(Node) ->
    Records = mnesia:dirty_select(
        ?SHARED_SUBSCRIPTION,
        ets:fun2ms(
            fun(Record = #?SHARED_SUBSCRIPTION{subpid = SubPid}) when node(SubPid) == Node ->
                Record
            end
        )
    ),
    lists:foreach(
        fun(Record) -> mria:dirty_delete_object(?SHARED_SUBSCRIPTION, Record) end,
        Records
    ),
    true = ets:delete_all_objects(?MEMBERS_CACHE).

update_stats() ->
    emqx_stats:setstat(
        'subscriptions.shared.count',
        'subscriptions.shared.max',
        ets:info(?SHARED_SUBSCRIPTION, size)
    ).

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
