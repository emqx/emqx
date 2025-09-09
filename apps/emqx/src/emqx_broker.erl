%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_router.hrl").
-include("emqx_external_trace.hrl").

-include("logger.hrl").
-include("emqx_instr.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([start_link/2, create_tabs/0, init_config/0]).

%% PubSub
-export([
    subscribe/1,
    subscribe/2,
    subscribe/3
]).

-export([unsubscribe/1]).

-export([
    subscriber_down/1,
    purge_node/1
]).

-export([
    publish/1,
    publish/2,
    safe_publish/1,
    safe_publish/2
]).

-export([dispatch/2]).

%% PubSub Infos
-export([
    subscriptions/1,
    subscriptions_via_topic/1,
    subscribers/1,
    subscribed/2
]).

%% Folds
-export([
    foldl_topics/2
]).

-export([
    get_subopts/2,
    set_subopts/2
]).

-export([topics/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(BROKER, ?MODULE).

%% Guards
-define(IS_SUBID(Id), (is_binary(Id) orelse is_atom(Id))).

-define(PT_FLAG_ASYNC_SHARD_DISPATCH, '$emqx_broker__async_fanout_shard_dispatch').

-define(cast_or_eval(PICK, Msg, Expr),
    case PICK of
        __X_Pid when __X_Pid =:= self() ->
            _ = Expr,
            ok;
        __X_Pid ->
            cast(__X_Pid, Msg)
    end
).

-type publish_opts() :: #{
    %% Whether to return a disinguishing value `{blocked, #message{}}' when a hook from
    %% `'message.publish''` returns `allow_publish => false'.  Defaults to `false'.
    hook_prohibition_as_error => boolean(),
    %% do not call message.publish hook point if true
    bypass_hook => boolean()
}.

-spec start_link(atom(), pos_integer()) -> startlink_ret().
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        []
    ).

%%------------------------------------------------------------------------------
%% Create tabs
%%------------------------------------------------------------------------------

-spec create_tabs() -> ok.
create_tabs() ->
    TabOpts = [public, {read_concurrency, true}, {write_concurrency, true}],

    %% SubOption: {TopicFilter, SubPid} -> SubOption
    %% NOTE: `foldl_topics/2` relies on it being ordered.
    ok = emqx_utils_ets:new(?SUBOPTION, [ordered_set | TabOpts]),

    %% Subscription: SubPid -> TopicFilter1, TopicFilter2, TopicFilter3, ...
    %% duplicate_bag: O(1) insert
    ok = emqx_utils_ets:new(?SUBSCRIPTION, [duplicate_bag | TabOpts]),

    %% Subscriber: Topic -> SubPid1, SubPid2, SubPid3, ..., ShardPid1, ...
    %% bag: O(n) insert
    %% However, the factor is small: high-fanout topics are _sharded_ into
    %% buckets of constant length (e.g. 1024).
    ok = emqx_utils_ets:new(?SUBSCRIBER, [bag | TabOpts]).

-spec init_config() -> ok.
init_config() ->
    %% NOTE
    %% Cache in persistent term, to avoid `emqx:get_config/2` in hot paths.
    persistent_term:put(
        ?PT_FLAG_ASYNC_SHARD_DISPATCH,
        emqx:get_config([broker, perf, async_fanout_shard_dispatch], false)
    ).

%%------------------------------------------------------------------------------
%% Subscribe API
%%------------------------------------------------------------------------------

-spec subscribe(emqx_types:topic() | emqx_types:share()) -> ok.
subscribe(Topic) when ?IS_TOPIC(Topic) ->
    subscribe(Topic, undefined).

-spec subscribe(emqx_types:topic() | emqx_types:share(), emqx_types:subid() | emqx_types:subopts()) ->
    ok.
subscribe(Topic, SubId) when ?IS_TOPIC(Topic), ?IS_SUBID(SubId) ->
    subscribe(Topic, SubId, ?DEFAULT_SUBOPTS);
subscribe(Topic, SubOpts) when ?IS_TOPIC(Topic), is_map(SubOpts) ->
    subscribe(Topic, undefined, SubOpts).

-spec subscribe(emqx_types:topic() | emqx_types:share(), emqx_types:subid(), emqx_types:subopts()) ->
    ok.
subscribe(Topic, SubId, SubOpts0) when ?IS_TOPIC(Topic), ?IS_SUBID(SubId), is_map(SubOpts0) ->
    SubOpts = maps:merge(?DEFAULT_SUBOPTS, SubOpts0),
    _ = emqx_trace:subscribe(Topic, SubId, SubOpts),
    SubPid = self(),
    case subscribed(SubPid, Topic) of
        %% New
        false ->
            ok = emqx_broker_helper:register_sub(SubPid, SubId),
            true = ets:insert(?SUBSCRIPTION, {SubPid, Topic}),
            do_subscribe(Topic, SubPid, with_subid(SubId, SubOpts));
        %% Existed
        true ->
            set_subopts(SubPid, Topic, with_subid(SubId, SubOpts)),
            %% ensure to return 'ok'
            ok
    end.

-compile({inline, [with_subid/2]}).
with_subid(undefined, SubOpts) ->
    SubOpts;
with_subid(SubId, SubOpts) ->
    maps:put(subid, SubId, SubOpts).

do_subscribe(Topic, SubPid, SubOpts) when is_binary(Topic) ->
    I = emqx_broker_helper:assign_sub_shard(Topic),
    true = ets:insert(?SUBOPTION, {{Topic, SubPid}, with_shard_idx(I, SubOpts)}),
    Sync = call(pick_sub({Topic, I}), {subscribe, Topic, SubPid, I}),
    case Sync of
        ok ->
            ok;
        Ref when is_reference(Ref) ->
            emqx_router_syncer:wait(Ref)
    end;
do_subscribe(Topic = #share{group = Group, topic = RealTopic}, SubPid, SubOpts) when
    is_binary(RealTopic)
->
    true = ets:insert(?SUBOPTION, {{Topic, SubPid}, SubOpts}),
    emqx_shared_sub:subscribe(Group, RealTopic, SubPid).

with_shard_idx(0, SubOpts) ->
    SubOpts;
with_shard_idx(I, SubOpts) ->
    maps:put(shard, I, SubOpts).

%%--------------------------------------------------------------------
%% Unsubscribe API
%%--------------------------------------------------------------------

-spec unsubscribe(emqx_types:topic() | emqx_types:share()) -> ok.
unsubscribe(Topic) when ?IS_TOPIC(Topic) ->
    SubPid = self(),
    case ets:lookup(?SUBOPTION, {Topic, SubPid}) of
        [{_, SubOpts}] ->
            _ = emqx_trace:unsubscribe(Topic, SubOpts),
            do_unsubscribe(Topic, SubPid, SubOpts);
        [] ->
            ok
    end.

-spec do_unsubscribe(emqx_types:topic() | emqx_types:share(), pid(), emqx_types:subopts()) ->
    ok.
do_unsubscribe(Topic, SubPid, SubOpts) ->
    true = ets:delete(?SUBOPTION, {Topic, SubPid}),
    true = ets:delete_object(?SUBSCRIPTION, {SubPid, Topic}),
    case Topic of
        B when is_binary(B) ->
            do_unsubscribe_regular(Topic, SubPid, SubOpts);
        #share{group = Group, topic = RealTopic} ->
            emqx_shared_sub:unsubscribe(Group, RealTopic, SubPid)
    end.

-spec do_unsubscribe_regular(emqx_types:topic(), pid(), emqx_types:subopts()) ->
    ok.
do_unsubscribe_regular(Topic, SubPid, SubOpts) ->
    I = maps:get(shard, SubOpts, 0),
    _ = emqx_broker_helper:unassign_sub_shard(Topic, I),
    case I of
        0 -> emqx_exclusive_subscription:unsubscribe(Topic, SubOpts);
        _ -> ok
    end,
    cast(pick_sub({Topic, I}), {unsubscribed, Topic, SubPid, I}).

%%--------------------------------------------------------------------
%% Publish
%%--------------------------------------------------------------------

-spec publish(emqx_types:message()) -> emqx_types:publish_result().
publish(#message{} = Msg) ->
    publish(#message{} = Msg, _Opts = #{}).

-spec publish(emqx_types:message(), publish_opts()) -> emqx_types:publish_result().
publish(#message{} = Msg, Opts) ->
    _ = emqx_trace:publish(Msg),
    emqx_message:is_sys(Msg) orelse emqx_metrics:inc('messages.publish'),
    case maps:get(bypass_hook, Opts, false) of
        true ->
            do_publish(Msg);
        false ->
            eval_hook_and_publish(Msg, Opts)
    end.

eval_hook_and_publish(Msg, Opts) ->
    case emqx_hooks:run_fold('message.publish', [], emqx_message:clean_dup(Msg)) of
        #message{headers = #{should_disconnect := true}, topic = Topic} ->
            ?TRACE("MQTT", "msg_publish_not_allowed_disconnect", #{
                message => emqx_message:to_log_map(Msg),
                topic => Topic
            }),
            disconnect;
        #message{headers = #{allow_publish := false}, topic = Topic} = Message ->
            ?TRACE("MQTT", "msg_publish_not_allowed", #{
                message => emqx_message:to_log_map(Msg),
                topic => Topic
            }),
            case maps:get(hook_prohibition_as_error, Opts, false) of
                true ->
                    {blocked, Message};
                false ->
                    []
            end;
        Msg1 = #message{} ->
            do_publish(Msg1);
        Msgs when is_list(Msgs) ->
            do_publish_many(Msgs)
    end.

do_publish_many([]) ->
    [];
do_publish_many([Msg | T]) ->
    do_publish(Msg) ++ do_publish_many(T).

do_publish(#message{topic = Topic} = Msg) ->
    PersistRes = persist_publish(Msg),
    Routes = aggre(emqx_router:match_routes(Topic)),
    Delivery = delivery(Msg),
    RouteRes = route(Routes, Delivery, PersistRes),
    do_forward_external(Delivery, RouteRes).

persist_publish(Msg) ->
    case emqx_persistent_message:persist(Msg, #{sync => noreply}) of
        noreply ->
            [persisted];
        {skipped, _} ->
            []
    end.

%% Called internally
-spec safe_publish(emqx_types:message()) -> emqx_types:publish_result().
safe_publish(Msg) ->
    safe_publish(Msg, _Opts = #{}).

-spec safe_publish(emqx_types:message(), publish_opts()) -> emqx_types:publish_result().
safe_publish(#message{} = Msg, Opts) ->
    try
        publish(Msg, Opts)
    catch
        Error:Reason:Stk ->
            ?SLOG(
                error,
                #{
                    msg => "publishing_error",
                    exception => Error,
                    reason => Reason,
                    payload => emqx_message:to_log_map(Msg),
                    stacktrace => Stk
                },
                #{topic => Msg#message.topic}
            ),
            []
    end.

-compile({inline, [delivery/1]}).
delivery(Msg) -> #delivery{sender = self(), message = Msg}.

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

route(Routes, Delivery = #delivery{message = _Msg}, PersistRes) ->
    ?EXT_TRACE_MSG_ROUTE(
        ?EXT_TRACE_ATTR((emqx_otel_trace:msg_attrs(_Msg))#{
            'route.from' => node(),
            'route.matched_result' => emqx_utils_json:encode([
                route_result({TF, RouteTo})
             || {TF, RouteTo} <- Routes
            ]),
            'client.clientid' => _Msg#message.from
        }),
        fun(DeliveryWithTrace) -> do_route(Routes, DeliveryWithTrace, PersistRes) end,
        [Delivery]
    ).

-if(?EMQX_RELEASE_EDITION == ee).
route_result({TF, Node}) when is_atom(Node) ->
    #{node => Node, route => TF};
route_result({TF, Group}) ->
    #{group => Group, route => TF}.

-else.
-endif.

-spec do_route([emqx_types:route_entry()], emqx_types:delivery(), nil() | [persisted]) ->
    emqx_types:publish_result().
do_route([], #delivery{message = Msg}, _PersistRes = []) ->
    ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, no_subscribers]),
    ok = inc_dropped_cnt(Msg),
    ?EXT_TRACE_ADD_ATTRS(
        begin
            case Msg of
                #message{flags = #{sys := true}} ->
                    ok;
                _ ->
                    #{
                        'route.dropped.node' => node(),
                        'route.dropped.reason' => no_subscribers
                    }
            end
        end
    ),
    [];
do_route([], _Delivery, PersistRes = [_ | _]) ->
    PersistRes;
do_route(Routes, Delivery, PersistRes) ->
    lists:foldl(
        fun(Route, Acc) ->
            [do_route2(Route, Delivery) | Acc]
        end,
        PersistRes,
        Routes
    ).

do_route2({To, Node}, Delivery) when Node =:= node() ->
    {Node, To, do_dispatch(To, Delivery)};
do_route2({To, Node}, Delivery) when is_atom(Node) ->
    {Node, To, forward(Node, To, Delivery, emqx:get_config([rpc, mode]))};
do_route2({To, Group}, Delivery) when is_tuple(Group); is_binary(Group) ->
    {share, To, emqx_shared_sub:dispatch(Group, To, Delivery)}.

aggre([]) ->
    [];
aggre([#route{topic = To, dest = Node}]) when is_atom(Node) ->
    [{To, Node} || emqx_router_helper:is_routable(Node)];
aggre([#route{topic = To, dest = {Group, _Node}}]) ->
    [{To, Group}];
aggre(Routes) ->
    aggre(Routes, false, []).

aggre([#route{topic = To, dest = Node} | Rest], Dedup, Acc) when is_atom(Node) ->
    case emqx_router_helper:is_routable(Node) of
        true -> NAcc = [{To, Node} | Acc];
        false -> NAcc = Acc
    end,
    aggre(Rest, Dedup, NAcc);
aggre([#route{topic = To, dest = {Group, Node}} | Rest], Dedup, Acc) ->
    case emqx_router_helper:is_routable(Node) of
        true -> aggre(Rest, true, [{To, Group} | Acc]);
        false -> aggre(Rest, Dedup, Acc)
    end;
aggre([], false, Acc) ->
    Acc;
aggre([], true, Acc) ->
    lists:usort(Acc).

do_forward_external(Delivery, RouteRes) ->
    emqx_external_broker:forward(Delivery) ++ RouteRes.

forward(Node, To, Delivery = #delivery{message = _Msg}, RpcMode) ->
    ?EXT_TRACE_MSG_FORWARD(
        ?EXT_TRACE_ATTR((emqx_otel_trace:msg_attrs(_Msg))#{
            'forward.from' => node(),
            'forward.to' => Node,
            'client.clientid' => _Msg#message.from
        }),
        fun(NDelivery) -> do_forward(Node, To, NDelivery, RpcMode) end,
        [Delivery]
    ).

%% @doc Forward message to another node.
-spec do_forward(
    node(),
    emqx_types:topic() | emqx_types:share(),
    emqx_types:delivery(),
    RpcMode :: sync | async
) ->
    emqx_types:deliver_result().
do_forward(Node, To, Delivery, async) ->
    true = emqx_broker_proto_v1:forward_async(Node, To, Delivery),
    emqx_metrics:inc('messages.forward');
do_forward(Node, To, Delivery, sync) ->
    case emqx_broker_proto_v1:forward(Node, To, Delivery) of
        {Err, Reason} when Err =:= badrpc; Err =:= badtcp ->
            ?SLOG(
                error,
                #{
                    msg => "sync_forward_msg_to_node_failed",
                    node => Node,
                    Err => Reason
                },
                #{topic => To}
            ),
            {error, badrpc};
        Result ->
            emqx_metrics:inc('messages.forward'),
            Result
    end.

%% Handle message forwarding form remote nodes by
%% `emqx_broker_proto_v1:forward/3` or
%% `emqx_broker_proto_v1:forward_async/3`
dispatch(Topic, Delivery = #delivery{sender = _Sender, message = _Msg}) ->
    ?EXT_TRACE_MSG_HANDLE_FORWARD(
        ?EXT_TRACE_ATTR((emqx_otel_trace:msg_attrs(_Msg))#{
            %%% XXX: Pid not checked
            'forward.from' => erlang:node(_Sender),
            'forward.to' => node(),
            'client.clientid' => _Msg#message.from
        }),
        fun(NDelivery) -> do_dispatch(Topic, NDelivery) end,
        [Delivery]
    ).

%% @doc Dispatch message to local subscribers.
-spec do_dispatch(emqx_types:topic() | emqx_types:share(), emqx_types:delivery()) ->
    emqx_types:deliver_result().
do_dispatch(Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    case emqx:is_running() of
        true ->
            do_dispatch2(Topic, Delivery);
        false ->
            %% In a rare case emqx_router_helper process may delay
            %% cleanup of the routing table and the peers will
            %% dispatch messages to a node that is not fully
            %% initialized. Handle this case gracefully:
            {error, not_running}
    end.

-compile({inline, [inc_dropped_cnt/1]}).
inc_dropped_cnt(Msg) ->
    case emqx_message:is_sys(Msg) of
        true ->
            ok;
        false ->
            ok = emqx_metrics:inc('messages.dropped'),
            emqx_metrics:inc('messages.dropped.no_subscribers')
    end.

-compile({inline, [subscribers/1]}).
-spec subscribers(
    emqx_types:topic()
    | emqx_types:share()
    | {shard, emqx_types:topic() | emqx_types:share(), non_neg_integer()}
) ->
    [pid()].
subscribers(Topic) when is_binary(Topic) ->
    lookup_value(?SUBSCRIBER, Topic, []);
subscribers(Shard = {shard, _Topic, _I}) ->
    lookup_value(?SUBSCRIBER, Shard, []).

%%--------------------------------------------------------------------
%% Subscriber is down
%%--------------------------------------------------------------------

-spec subscriber_down(pid()) -> true.
subscriber_down(SubPid) ->
    lists:foreach(
        fun(Topic) ->
            case lookup_value(?SUBOPTION, {Topic, SubPid}) of
                SubOpts when is_map(SubOpts) ->
                    do_unsubscribe_down(Topic, SubPid, SubOpts);
                undefined ->
                    ok
            end
        end,
        lookup_value(?SUBSCRIPTION, SubPid, [])
    ),
    ets:delete(?SUBSCRIPTION, SubPid).

do_unsubscribe_down(Topic, SubPid, SubOpts) ->
    true = ets:delete(?SUBOPTION, {Topic, SubPid}),
    case Topic of
        B when is_binary(B) ->
            do_unsubscribe_regular(Topic, SubPid, SubOpts);
        #share{group = Group, topic = RealTopic} ->
            emqx_shared_sub:unsubscribe_down(Group, RealTopic, SubPid)
    end.

%%--------------------------------------------------------------------
%% Node Cleanup APIs
%%--------------------------------------------------------------------

-spec purge_node(node()) -> ok.
purge_node(Node) ->
    ok = emqx_router:cleanup_routes(Node),
    _ = emqx_shared_sub:purge_node(Node),
    ok.

%%--------------------------------------------------------------------
%% Management APIs
%%--------------------------------------------------------------------

-spec subscriptions(pid() | emqx_types:subid()) ->
    [{emqx_types:topic() | emqx_types:share(), emqx_types:subopts()}].
subscriptions(SubPid) when is_pid(SubPid) ->
    [
        {Topic, lookup_value(?SUBOPTION, {Topic, SubPid}, #{})}
     || Topic <- lookup_value(?SUBSCRIPTION, SubPid, [])
    ];
subscriptions(SubId) ->
    case emqx_broker_helper:lookup_subpid(SubId) of
        SubPid when is_pid(SubPid) ->
            subscriptions(SubPid);
        undefined ->
            []
    end.

-spec subscriptions_via_topic(emqx_types:topic() | emqx_types:share()) -> [emqx_types:subopts()].
subscriptions_via_topic(Topic) ->
    MatchSpec = [{{{Topic, '_'}, '_'}, [], ['$_']}],
    ets:select(?SUBOPTION, MatchSpec).

-spec subscribed(
    pid() | emqx_types:subid(), emqx_types:topic() | emqx_types:share()
) -> boolean().
subscribed(SubPid, Topic) when is_pid(SubPid) ->
    ets:member(?SUBOPTION, {Topic, SubPid});
subscribed(SubId, Topic) when ?IS_SUBID(SubId) ->
    SubPid = emqx_broker_helper:lookup_subpid(SubId),
    ets:member(?SUBOPTION, {Topic, SubPid}).

-spec get_subopts(pid(), emqx_types:topic() | emqx_types:share()) -> option(emqx_types:subopts()).
get_subopts(SubPid, Topic) when is_pid(SubPid), ?IS_TOPIC(Topic) ->
    lookup_value(?SUBOPTION, {Topic, SubPid});
get_subopts(SubId, Topic) when ?IS_SUBID(SubId) ->
    case emqx_broker_helper:lookup_subpid(SubId) of
        SubPid when is_pid(SubPid) ->
            get_subopts(SubPid, Topic);
        undefined ->
            undefined
    end.

-spec set_subopts(emqx_types:topic() | emqx_types:share(), emqx_types:subopts()) -> boolean().
set_subopts(Topic, NewOpts) when is_binary(Topic), is_map(NewOpts) ->
    set_subopts(self(), Topic, NewOpts).

%% @private
set_subopts(SubPid, Topic, NewOpts) ->
    Sub = {Topic, SubPid},
    case ets:lookup(?SUBOPTION, Sub) of
        [{_, OldOpts}] ->
            ets:insert(?SUBOPTION, {Sub, maps:merge(OldOpts, NewOpts)});
        [] ->
            false
    end.

-spec foldl_topics(fun((emqx_types:topic() | emqx_types:share(), Acc) -> Acc), Acc) ->
    Acc.
foldl_topics(FoldFun, Acc) ->
    First = ets:first(?SUBOPTION),
    foldl_topics(FoldFun, Acc, First).

foldl_topics(FoldFun, Acc, {Topic, _SubPid}) ->
    Next = ets:next(?SUBOPTION, {Topic, _GreaterThanAnyPid = []}),
    foldl_topics(FoldFun, FoldFun(Topic, Acc), Next);
foldl_topics(_FoldFun, Acc, '$end_of_table') ->
    Acc.

-spec topics() -> [emqx_types:topic() | emqx_types:share()].
topics() ->
    emqx_router:topics().

%%--------------------------------------------------------------------
%% call, cast, pick
%%--------------------------------------------------------------------

-compile({inline, [call/2, cast/2, pick_sub/1, pick_pub/1]}).

call(Broker, Req) ->
    gen_server:call(Broker, Req, infinity).

cast(Broker, Req) ->
    gen_server:cast(Broker, Req).

%% Pick a pool worker to handle subscribe request
pick_sub(TopicShard) ->
    gproc_pool:pick_worker(broker_pool, TopicShard).

%% Pick a pool worker to handle message publish
pick_pub(TopicShard) ->
    gproc_pool:pick_worker(dispatcher_pool, TopicShard).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({subscribe, Topic, SubPid, 0}, {From, _Tag}, State) ->
    Existed = ets:member(?SUBSCRIBER, Topic),
    Result = maybe_add_route(Existed, Topic, From),
    assert_ok_result(Result),
    true = ets:insert(?SUBSCRIBER, {Topic, SubPid}),
    {reply, Result, State};
handle_call({subscribe, Topic, SubPid, I}, _From, State) ->
    Existed = ets:member(?SUBSCRIBER, {shard, Topic, I}),
    Recs = [{{shard, Topic, I}, SubPid}],
    Recs1 =
        case Existed of
            false ->
                %% This will attempt to add a route per each new shard.
                %% The overhead must be negligible, but the consistency in general
                %% and race conditions safety is expected to be stronger.
                %% The main purpose is to solve the race when
                %% `{shard, Topic, N}` (where N > 0)
                %% is the first ever processed subscribe request per `Topic`.
                %% It inserts `{Topic, {shard, I}}` to `?SUBSCRIBER` tab.
                %% After that, another broker worker starts processing
                %% `{shard, Topic, 0}` sub and already observers `{shard, Topic, N}`,
                %% i.e. `ets:member(?SUBSCRIBER, Topic)` returns false,
                %% so it doesn't add the route.
                %% Even if this happens, this cast is expected to be processed eventually
                %% and the route should be added (unless the worker restarts...)
                ?cast_or_eval(
                    pick_sub({Topic, 0}),
                    {subscribed, Topic, shard, I},
                    sync_route(add, Topic, #{})
                ),
                [{{shard, Topic}, I} | Recs];
            true ->
                Recs
        end,
    true = ets:insert(?SUBSCRIBER, Recs1),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({dispatch, Topic, I, Msg}, State) ->
    ?BROKER_INSTR_TS(TDisp),
    _ = do_dispatch_chans({deliver, Topic, Msg}, subscribers({shard, Topic, I}), 0),
    ?BROKER_INSTR_OBSERVE_HIST(broker, dispatch_shard_delay_us, ?US(TDisp - Msg#message.extra)),
    ?BROKER_INSTR_OBSERVE_HIST(broker, dispatch_shard_lat_us, ?US_SINCE(TDisp)),
    {noreply, State};
handle_cast({subscribed, Topic, shard, _I}, State) ->
    %% Do not need to 'maybe add' (i.e. to check if the route exists).
    %% It was already checked that this shard is newely added.
    _ = sync_route(add, Topic, #{}),
    {noreply, State};
handle_cast({unsubscribed, Topic, shard, _I}, State) ->
    _ = maybe_delete_route(Topic),
    {noreply, State};
handle_cast({unsubscribed, Topic, SubPid, 0}, State) ->
    true = ets:delete_object(?SUBSCRIBER, {Topic, SubPid}),
    _ = maybe_delete_route(Topic),
    {noreply, State};
handle_cast({unsubscribed, Topic, SubPid, I}, State) ->
    true = ets:delete_object(?SUBSCRIBER, {{shard, Topic, I}, SubPid}),
    case ets:member(?SUBSCRIBER, {shard, Topic, I}) of
        false ->
            ets:delete_object(?SUBSCRIBER, {{shard, Topic}, I}),
            %% Do not attempt to delete any routes here,
            %% let it be handled only by the same pool worker per topic (0 shard),
            %% so that all route deletes are serialized.
            ?cast_or_eval(
                pick_sub({Topic, 0}),
                {unsubscribed, Topic, shard, I},
                maybe_delete_route(Topic)
            );
        true ->
            ok
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec do_dispatch2(emqx_types:topic() | emqx_types:share(), emqx_types:delivery()) ->
    emqx_types:deliver_result().
do_dispatch2(Topic, #delivery{message = MsgIn}) ->
    ?BROKER_INSTR_TS(T0),
    ?BROKER_INSTR_BIND(Msg, MsgIn, MsgIn#message{extra = T0}),
    AsyncDispatch = persistent_term:get(?PT_FLAG_ASYNC_SHARD_DISPATCH, false),
    Deliver = {deliver, Topic, Msg},
    Shards = lookup_value(?SUBSCRIBER, {shard, Topic}, []),
    case AsyncDispatch of
        false ->
            DispN0 = do_dispatch_shards(Topic, Deliver, Shards, 0);
        true ->
            DispN0 = do_dispatch_shards_async(Topic, Msg, Shards, 0)
    end,
    DispN = do_dispatch_chans(Deliver, subscribers(Topic), DispN0),
    ?BROKER_INSTR_OBSERVE_HIST(broker, dispatch_total_lat_us, ?US_SINCE(T0)),
    case DispN of
        0 ->
            ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, no_subscribers]),
            ok = inc_dropped_cnt(Msg),
            {error, no_subscribers};
        _ ->
            {ok, DispN}
    end.

%% Don't dispatch to share subscriber here.
%% we do it in `emqx_shared_sub.erl` with configured strategy
do_dispatch_chans(Deliver, [SubPid | Rest], N) ->
    SubPid ! Deliver,
    do_dispatch_chans(Deliver, Rest, N + 1);
do_dispatch_chans(_Deliver, [], N) ->
    N.

do_dispatch_shards(Topic, Deliver, [I | Rest], N0) ->
    N = do_dispatch_chans(Deliver, subscribers({shard, Topic, I}), N0),
    do_dispatch_shards(Topic, Deliver, Rest, N);
do_dispatch_shards(_Topic, _Deliver, [], N) ->
    N.

do_dispatch_shards_async(Topic, Msg, [I | Rest], N) ->
    %% Dispatching to sharded subscribers concurrently + asynchronously.
    %% Ordering guarantees should still hold:
    %% * Each subscriber is part of exactly one shard.
    %% * Each topic-shard is always dispatched through the same process.
    cast(pick_pub({Topic, I}), {dispatch, Topic, I, Msg}),
    %% Assuming shard is non-empty.
    do_dispatch_shards_async(Topic, Msg, Rest, N + 1);
do_dispatch_shards_async(_Topic, _Msg, [], N) ->
    N.

%%

assert_ok_result(ok) -> ok;
assert_ok_result(Ref) when is_reference(Ref) -> ok.

maybe_add_route(_Existed = false, Topic, ReplyTo) ->
    sync_route(add, Topic, #{reply => ReplyTo});
maybe_add_route(_Existed = true, _Topic, _ReplyTo) ->
    ok.

maybe_delete_route(Topic) ->
    case ets:member(?SUBSCRIBER, Topic) of
        true -> ok;
        false -> sync_route(delete, Topic, #{})
    end.

sync_route(Action, Topic, ReplyTo) ->
    EnabledOn = emqx_config:get([broker, routing, batch_sync, enable_on]),
    Res =
        case EnabledOn of
            all ->
                push_sync_route(Action, Topic, ReplyTo);
            none ->
                regular_sync_route(Action, Topic);
            Role ->
                case Role =:= mria_config:whoami() of
                    true ->
                        push_sync_route(Action, Topic, ReplyTo);
                    false ->
                        regular_sync_route(Action, Topic)
                end
        end,
    _ = external_sync_route(Action, Topic),
    Res.

external_sync_route(add, Topic) ->
    emqx_external_broker:add_route(Topic);
external_sync_route(delete, Topic) ->
    emqx_external_broker:delete_route(Topic).

push_sync_route(Action, Topic, Opts) ->
    emqx_router_syncer:push(Action, Topic, node(), Opts).

regular_sync_route(add, Topic) ->
    emqx_router:do_add_route(Topic, node());
regular_sync_route(delete, Topic) ->
    emqx_router:do_delete_route(Topic, node()).

%%

-compile({inline, [lookup_value/2, lookup_value/3]}).

lookup_value(Tab, Key) ->
    ets:lookup_element(Tab, Key, 2, undefined).

lookup_value(Tab, Key, Def) ->
    ets:lookup_element(Tab, Key, 2, Def).
