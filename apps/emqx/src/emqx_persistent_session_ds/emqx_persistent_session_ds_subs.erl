%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module encapsulates the data related to the client's
%% subscriptions. It tries to represent the subscriptions as if they
%% were a simple key-value map. In reality, however, the session has
%% to retain old the subscriptions for longer to ensure the
%% consistency of message replay.
%%
%% In addition, this module is responsible for management of routing
%% tables (durable, regular and external) and for updating the
%% subscriptions of the session's `emqx_ds_client'.
-module(emqx_persistent_session_ds_subs).

%% API:
-export([
    fold/4,
    lookup/2,
    find_by_subid/2,
    to_map/1,
    gc/1,

    on_subscribe/3,
    on_unsubscribe/2,
    on_session_restore/1,
    on_session_drop/2,

    %% Used by REST API:
    cold_get_subscription/2
]).

-ifdef(TEST).
-export([
    runtime_state_invariants/2,
    offline_state_invariants/2
]).
-endif.

-export_type([
    subscription_state_id/0, subscription/0, subopts/0, subscription_state/0, subscription_mode/0
]).

-include("session_internals.hrl").
-include("emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type subscription() :: #{
    %% Session-unique identifier of the subscription. Other objects
    %% can use it as a compact reference:
    id := emqx_persistent_session_ds:subscription_id(),
    %% Reference to the current subscription state:
    current_state := subscription_state_id(),
    %% Time when the subscription was added:
    start_time := emqx_persistent_session_ds:millisecond()
}.

-type subscription_state_id() :: integer().

-type subscription_mode() :: durable | direct.

-type subopts() :: #{
    nl => _,
    qos => _,
    rap => _,
    subid => _,
    _ => _
}.

-type subscription_state() :: #{
    parent_subscription := emqx_persistent_session_ds:subscription_id(),
    upgrade_qos := boolean(),
    %% Mode of the subscription:
    %% * `durable' subscriptions use DS machinery to serve messages.
    %% * `direct' subscriptions go through the primary broker, similarly
    %%    to in-mem sessions. Applicable to QoS0 subscriptions.
    %% It's essentially an override for the case when the active mode
    %% is different from what QoS level of the subscription implies.
    mode := subscription_mode(),
    %% SubOpts:
    subopts := subopts(),
    %% Optional field that is added when subscription state becomes
    %% outdated (note: do not use the value, as GC may delete
    %% subscription states referenced by `superseded_by'):
    superseded_by => subscription_state_id(),
    %% Optional field used by shared subscriptions:
    share_topic_filter => #share{}
}.

%%================================================================================
%% API functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Pure APIs that don't have any side effects other than modifying
%% persistent session state (i.e. `s' field).
%% --------------------------------------------------------------------------------

%% @doc Fold over subscriptions.
%%
%% If `Include' list is empty, this function folds only over
%% non-shared durable subscriptions. Adding atoms `direct' or `shared'
%% adds specified types of subscriptions to the loop.
-spec fold(
    fun((emqx_persistent_session_ds:topic_filter(), subscription(), Acc) -> Acc),
    Acc,
    emqx_persistent_session_ds_state:t(),
    Include :: [direct | shared]
) ->
    Acc.
fold(Fun, Acc0, S, Includes) ->
    OptShared = lists:member(shared, Includes),
    OptDirect = lists:member(direct, Includes),
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (ShareTopicFilter = #share{}, Sub, Acc) when OptShared ->
                Fun(ShareTopicFilter, Sub, Acc);
            (#share{}, _Sub, Acc) ->
                Acc;
            (TopicFilter, Sub, Acc) ->
                case current_subscription_mode(Sub, S) of
                    durable ->
                        Fun(TopicFilter, Sub, Acc);
                    direct when OptDirect ->
                        Fun(TopicFilter, Sub, Acc);
                    _ ->
                        Acc
                end
        end,
        Acc0,
        S
    ).

%% @doc Lookup a subscription and merge it with its current state:
-spec lookup(emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds:subscription() | undefined.
lookup(TopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S) of
        Sub = #{current_state := SStateId} ->
            SState = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
            session_subscription(Sub, SState);
        undefined ->
            undefined
    end.

%% @doc Lookup subscription by its ID:
-spec find_by_subid(
    emqx_persistent_session_ds:subscription_id(), emqx_persistent_session_ds_state:t()
) ->
    {emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds:subscription()}
    | undefined.
find_by_subid(SubId, S) ->
    %% TODO: implement generic find function for emqx_persistent_session_ds_state
    try
        emqx_persistent_session_ds_state:fold_subscriptions(
            fun(TF, Sub = #{id := I}, Acc) ->
                case I of
                    SubId ->
                        throw({found, TF, Sub});
                    _ ->
                        Acc
                end
            end,
            [],
            S
        ),
        undefined
    catch
        {found, TF, Sub} ->
            {TF, Sub}
    end.

%% @doc Convert active subscriptions to a map, for information
%% purpose:
-spec to_map(emqx_persistent_session_ds_state:t()) -> map().
to_map(S) ->
    fold(
        fun(TopicFilter, _, Acc) -> Acc#{TopicFilter => lookup(TopicFilter, S)} end,
        #{},
        S,
        [direct]
    ).

%% @doc Remove subscription states that don't have a parent, and that
%% don't have any unacked messages.
%% TODO
%% This function collects shared subs as well
%% Move to a separate module to keep symmetry?
-spec gc(emqx_persistent_session_ds_state:t()) -> emqx_persistent_session_ds_state:t().
gc(S0) ->
    %% Create a set of subscription states IDs referenced either by a
    %% subscription or a stream replay state:
    AliveSet0 = emqx_persistent_session_ds_state:fold_subscriptions(
        fun(_TopicFilter, #{current_state := SStateId}, Acc) ->
            Acc#{SStateId => true}
        end,
        #{},
        S0
    ),
    AliveSet = emqx_persistent_session_ds_state:fold_streams(
        fun(_StreamId, #srs{sub_state_id = SStateId}, Acc) ->
            Acc#{SStateId => true}
        end,
        AliveSet0,
        S0
    ),
    %% Delete subscription states that don't belong to the alive set:
    emqx_persistent_session_ds_state:fold_subscription_states(
        fun(SStateId, _, S) ->
            case maps:is_key(SStateId, AliveSet) of
                true ->
                    S;
                false ->
                    emqx_persistent_session_ds_state:del_subscription_state(SStateId, S)
            end
        end,
        S0,
        S0
    ).

-spec cold_get_subscription(emqx_persistent_session_ds:id(), emqx_types:topic()) ->
    emqx_persistent_session_ds:subscription() | undefined.
cold_get_subscription(SessionId, Topic) ->
    case emqx_persistent_session_ds_state:cold_get_subscription(SessionId, Topic) of
        [Sub = #{current_state := SStateId}] ->
            case
                emqx_persistent_session_ds_state:cold_get_subscription_state(SessionId, SStateId)
            of
                [SState] ->
                    session_subscription(Sub, SState);
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------------------
%% Effectful APIs that can affect routing table, durable storage
%% subscriptions, etc.
%%--------------------------------------------------------------------------------

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) ->
    {ok | mode_changed, subscription_mode(), emqx_persistent_session_ds:session(), subscription()}
    | {error, ?RC_QUOTA_EXCEEDED}.
on_subscribe(TopicFilter, SubOpts, Sess0 = #{s := S0, props := Props}) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    NSubscriptions = emqx_persistent_session_ds_state:n_subscriptions(S0),
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined when NSubscriptions < MaxSubscriptions ->
            %% This is a new subscription:
            {Mode, Sess, Subscription} =
                do_new_subscription(TopicFilter, SubOpts, Sess0),
            {ok, Mode, gc_(Sess), Subscription};
        undefined ->
            {error, ?RC_QUOTA_EXCEEDED};
        OldSub ->
            %% This is an existing subscription:
            {Outcome, Mode, Sess, Subscription} =
                on_update_subscription(TopicFilter, SubOpts, OldSub, Sess0),
            {Outcome, Mode, gc_(Sess), Subscription}
    end.

%% @doc Process UNSUBSCRIBE
-spec on_unsubscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds:session(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(TopicFilter, Sess0 = #{s := S0}) when is_binary(TopicFilter) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription = #{current_state := SSid} ->
            SubState = emqx_persistent_session_ds_state:get_subscription_state(SSid, S0),
            Sess = do_unsubscribe(TopicFilter, Subscription, Sess0),
            {ok, gc_(Sess), session_subscription(Subscription, SubState)}
    end.

%% @doc Called for an existing session that is being restored after being offline
-spec on_session_restore(emqx_persistent_session_ds:session()) ->
    emqx_persistent_session_ds:session().
on_session_restore(Sess0 = #{s := S0}) ->
    fold(
        fun(TopicFilter, Sub = #{current_state := SSid}, Sess1 = #{s := S}) ->
            #{mode := Mode, subopts := SubOpts} =
                emqx_persistent_session_ds_state:get_subscription_state(SSid, S),
            create_subscription_effects(Mode, TopicFilter, Sub, SubOpts, Sess1)
        end,
        Sess0,
        S0,
        [direct]
    ).

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
on_session_drop(SessId, S0) ->
    fold(
        fun(TopicFilter, _Subscription, Acc) ->
            delete_persistent_route(SessId, TopicFilter),
            Acc
        end,
        S0,
        S0,
        []
    ).

%%================================================================================
%% Internal functions
%%================================================================================

-spec gc_(emqx_persistent_session_ds:session()) -> emqx_persistent_session_ds:session().
gc_(Sess = #{s := S}) ->
    Sess#{s := gc(S)}.

-spec on_update_subscription(
    emqx_persistent_session_ds:topic_filter(),
    subopts(),
    subscription(),
    emqx_persistent_session_ds:session()
) ->
    {ok | mode_changed, subscription_mode(), emqx_persistent_session_ds:session(), subscription()}.
on_update_subscription(
    TopicFilter,
    NewSubOpts,
    #{id := SubId, current_state := CurrentSubStateId} = CurrentSub,
    Sess0 = #{id := SessionId, props := #{upgrade_qos := UpgradeQoS}, s := S0}
) ->
    CurrentSubState = emqx_persistent_session_ds_state:get_subscription_state(
        CurrentSubStateId, S0
    ),
    NewMode = subscription_mode(NewSubOpts),
    case CurrentSubState of
        #{
            subopts := NewSubOpts,
            upgrade_qos := UpgradeQoS,
            mode := NewMode
        } ->
            %% Client resubscribed with the same parameters:
            {ok, NewMode, Sess0, CurrentSub};
        #{mode := NewMode} ->
            %% Subscription mode matches, but some parameters changed.
            %% Create a new subscription state:
            NewSubState = mk_subscription_state(SubId, NewSubOpts, UpgradeQoS),
            {S, Sub} =
                update_subscription_state(
                    TopicFilter, CurrentSub, CurrentSubState, NewSubState, S0
                ),
            Sess = Sess0#{s := S},
            ?tp(persistent_session_ds_subscription_updated, #{
                topic_filter => TopicFilter,
                session => SessionId,
                mode => NewMode,
                mode_changed => false
            }),
            {ok, NewMode, Sess, Sub};
        #{mode := _OldMode} ->
            %% Everything, including the mode, is changing. Here we
            %% create a totally new subscription unrelated to the old
            %% one. Currently this process is not entirely smooth. The
            %% client may double-receive some buffered messages from
            %% the old subscription.
            Sess1 = do_unsubscribe(TopicFilter, CurrentSub, Sess0),
            {_, Sess, Sub} = do_new_subscription(TopicFilter, NewSubOpts, Sess1),
            ?tp(persistent_session_ds_subscription_updated, #{
                topic_filter => TopicFilter,
                session => SessionId,
                mode => NewMode,
                mode_changed => true
            }),
            {mode_changed, NewMode, Sess, Sub}
    end.

%% Create a new subscription with effects.
-spec do_new_subscription(
    emqx_persistent_session_ds:topic_filter(), subopts(), emqx_persistent_session_ds:session()
) ->
    {subscription_mode(), emqx_persistent_session_ds:session(), subscription()}.
do_new_subscription(
    TopicFilter, SubOpts, Sess0 = #{id := SessionId, props := #{upgrade_qos := UpgradeQoS}, s := S0}
) ->
    Mode = subscription_mode(SubOpts),
    {S, Subscription} = write_new_subscription(TopicFilter, SubOpts, UpgradeQoS, S0),
    ?tp(persistent_session_ds_subscription_added, #{
        topic_filter => TopicFilter,
        session => SessionId,
        mode => Mode
    }),
    Sess1 = Sess0#{s := S},
    Sess = create_subscription_effects(Mode, TopicFilter, Subscription, SubOpts, Sess1),
    {Mode, Sess, Subscription}.

%% Delete a subscription with effects.
-spec do_unsubscribe(
    emqx_persistent_session_ds:topic_filter(),
    subscription(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
do_unsubscribe(TopicFilter, Subscription, Sess0 = #{id := SessionId, s := S0}) ->
    Mode = current_subscription_mode(Subscription, S0),
    Sess = Sess0#{s := delete_subscription_state(TopicFilter, S0)},
    ?tp(persistent_session_ds_subscription_delete, #{
        session_id => SessionId,
        topic_filter => TopicFilter,
        mode => Mode
    }),
    delete_subscription_effects(Mode, TopicFilter, Subscription, Sess).

%%--------------------------------------------------------------------------------
%% Functions for manipulating subscription state. They don't have any
%% side effects other than modifying persistent session state (i.e. `s' field).
%%--------------------------------------------------------------------------------

-spec update_subscription_state(
    emqx_persistent_session_ds:topic_filter(),
    subscription(),
    subscription_state(),
    subscription_state(),
    emqx_persistent_session_ds_state:t()
) ->
    {emqx_persistent_session_ds_state:t(), subscription()}.
update_subscription_state(TopicFilter, Subscription0, OldSubState, NewSubState, S0) ->
    #{current_state := SStateId0} = Subscription0,
    {NewSubStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    S2 = emqx_persistent_session_ds_state:put_subscription_state(NewSubStateId, NewSubState, S1),
    S3 = emqx_persistent_session_ds_state:put_subscription_state(
        SStateId0, OldSubState#{superseded_by => NewSubStateId}, S2
    ),
    Subscription = Subscription0#{current_state := NewSubStateId},
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Subscription, S3),
    {S, Subscription}.

%% Create a new subscription record and a new subscription state and
%% write it to the persistent session state.
-spec write_new_subscription(
    emqx_persistent_session_ds:topic_filter(),
    subopts(),
    boolean(),
    emqx_persistent_session_ds_state:t()
) -> {emqx_persistent_session_ds_state:t(), subscription()}.
write_new_subscription(TopicFilter, SubOpts, UpgradeQoS, S0) ->
    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
    SState = mk_subscription_state(SubId, SubOpts, UpgradeQoS),
    Subscription = #{
        id => SubId,
        current_state => SStateId,
        start_time => emqx_persistent_session_ds:now_ms()
    },
    S3 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S2),
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Subscription, S3),
    {S, Subscription}.

-spec delete_subscription_state(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_state:t()
) ->
    emqx_persistent_session_ds_state:t().
delete_subscription_state(TopicFilter, S0) ->
    emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0).

%% @doc Merge subscription with its current state:
-spec session_subscription(subscription(), subscription_state()) ->
    emqx_persistent_session_ds:subscription().
session_subscription(Subscription, #{subopts := SubOpts, mode := Mode}) ->
    Subscription#{subopts => SubOpts, mode => Mode};
session_subscription(_Subscription, undefined) ->
    undefined.

mk_subscription_state(SubId, SubOpts, UpgradeQoS) ->
    #{
        parent_subscription => SubId,
        upgrade_qos => UpgradeQoS,
        subopts => SubOpts,
        mode => subscription_mode(SubOpts)
    }.

%%--------------------------------------------------------------------------------
%% Functions that update the external world
%%--------------------------------------------------------------------------------

%% Perform side effects necessary for transferring the "outer world"
%% into state where subscription exists:
-spec create_subscription_effects(
    subscription_mode(),
    emqx_persistent_session_ds:topic_filter(),
    subscription(),
    subopts(),
    emqx_persistent_session_ds:session()
) -> emqx_persistent_session_ds:session().
create_subscription_effects(durable, TopicFilter, Subscription, _SubOpts, Sess = #{id := SessId}) ->
    add_persistent_route(SessId, TopicFilter),
    ds_client_subscribe(TopicFilter, Subscription, Sess);
create_subscription_effects(direct, TopicFilter, _Subscription, SubOpts, Sess = #{id := SessId}) ->
    add_direct_route(SessId, TopicFilter, SubOpts),
    Sess.

%% Inverse of `create_subscription_effects'
-spec delete_subscription_effects(
    subscription_mode(),
    emqx_persistent_session_ds:topic_filter(),
    subscription(),
    emqx_persistent_session_ds:session()
) -> emqx_persistent_session_ds:session().
delete_subscription_effects(durable, TopicFilter, Subscription, Sess = #{id := SessId}) ->
    delete_persistent_route(SessId, TopicFilter),
    ds_client_unsubscribe(Subscription, Sess);
delete_subscription_effects(direct, TopicFilter, _, Sess = #{id := SessId}) ->
    delete_direct_route(SessId, TopicFilter),
    Sess.

%%--------------------------------------------------------------------------------
%% `emqx_ds_client' management
%%--------------------------------------------------------------------------------

%% A wrapper function that creates a subscription in `emqx_ds_client'
%% with given options, and updates the session state:
-spec ds_client_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    subscription(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
ds_client_subscribe(TopicFilter, #{id := SubId, start_time := StartTime}, Sess0 = #{dscli := CLI0}) ->
    SubOpts = #{
        max_unacked => emqx_config:get([durable_sessions, batch_size])
    },
    Opts = #{
        id => SubId,
        db => ?PERSISTENT_MESSAGE_DB,
        topic => emqx_ds:topic_words(TopicFilter),
        start_time => emqx_persistent_session_ds:to_ds_time(StartTime),
        ds_sub_opts => SubOpts
    },
    case emqx_ds_client:subscribe(CLI0, Opts, Sess0) of
        {ok, CLI, Sess} ->
            Sess#{dscli := CLI};
        {error, already_exists} ->
            Sess0
    end.

%% Undoes `ds_cli_subscribe':
-spec ds_client_unsubscribe(
    subscription(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
ds_client_unsubscribe(#{id := SubId}, Sess0 = #{dscli := DSClient0}) ->
    case emqx_ds_client:unsubscribe(DSClient0, SubId, Sess0) of
        {ok, DSClient, Sess} ->
            Sess#{dscli := DSClient};
        {error, not_found} ->
            Sess0
    end.

%%--------------------------------------------------------------------------------
%% Routing table management
%%--------------------------------------------------------------------------------

add_persistent_route(SessionId, Topic) ->
    ok = emqx_persistent_session_ds_router:do_add_route(Topic, SessionId),
    _ = emqx_external_broker:add_persistent_route(Topic, SessionId),
    ok.

delete_persistent_route(SessionId, Topic) ->
    ok = emqx_persistent_session_ds_router:do_delete_route(Topic, SessionId),
    _ = emqx_external_broker:delete_persistent_route(Topic, SessionId),
    ok.

add_direct_route(SessionId, Topic, SubOpts) ->
    emqx_broker:subscribe(Topic, SessionId, SubOpts).

delete_direct_route(_SessionId, Topic) ->
    emqx_broker:unsubscribe(Topic).

%% Get **current** mode of the subscription:
-spec current_subscription_mode(
    subscription(),
    emqx_persistent_session_ds_state:t()
) -> subscription_mode().
current_subscription_mode(#{current_state := SStateId}, S) ->
    #{mode := Mode} = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
    Mode.

-spec subscription_mode(subopts()) -> subscription_mode().
subscription_mode(#{qos := ?QOS_0}) ->
    direct;
subscription_mode(#{qos := _QoS12}) ->
    durable.

%%================================================================================
%% Test
%%================================================================================

-ifdef(TEST).

-spec runtime_state_invariants(emqx_persistent_session_ds_fuzzer:model_state(), #{
    s := map(), dscli := _
}) ->
    boolean().
runtime_state_invariants(ModelState = #{subs := ModelSubs}, State) ->
    state_invariants(ModelState, State) and
        assert_ds_client_subscriptions(ModelState, State) and
        assert_direct_routes(ModelState).

-spec offline_state_invariants(emqx_persistent_session_ds_fuzzer:model_state(), #{s := map()}) ->
    boolean().
offline_state_invariants(ModelState = #{subs := _ModelSubs}, State) ->
    assert_durable_routes(ModelState),
    state_invariants(ModelState, State).

-spec state_invariants(emqx_persistent_session_ds_fuzzer:model_state(), #{s := map()}) ->
    boolean().
state_invariants(#{subs := ModelSubs}, #{s := S} = Rec) ->
    #{subscriptions := Subs} = S,
    assert_one_to_one(
        "model subs",
        [element(1, emqx_topic:parse(I)) || I <- maps:keys(ModelSubs)],
        "subs stored in session state",
        maps:keys(Subs),
        #{s => S}
    ) and assert_state_sub_qos(ModelSubs, S).

%% Verify that QoS of the current subscription state matches the model QoS:
assert_state_sub_qos(ModelSubs, S) ->
    #{subscriptions := Subs, subscription_states := SStates} = S,
    maps:foreach(
        fun(ModelTFBin, #{qos := ExpectedQoS}) ->
            {TopicFilter, _} = emqx_topic:parse(ModelTFBin),
            ?defer_assert(
                begin
                    #{TopicFilter := #{current_state := SSId}} = Subs,
                    #{SSId := SubState} = SStates,
                    #{subopts := #{qos := ExpectedQoS}} = SubState
                end
            )
        end,
        ModelSubs
    ),
    true.

%% This function verifies that for each model subscription with QoS >
%% 0 there's a durable route.
assert_durable_routes(ModelState) ->
    Routes = emqx_persistent_session_ds_router:foldl_routes(
        fun({ps_route, Topic, _Dest}, Acc) ->
            [Topic | Acc]
        end,
        []
    ),
    assert_one_to_one(
        "durable model subscriptions",
        durable_model_subs(ModelState),
        "durable routes",
        Routes,
        #{model => ModelState}
    ).

assert_direct_routes(_) ->
    %% TODO
    true.

%% Verify that the DS client of the session is subscribed to all
%% durable model subs.
assert_ds_client_subscriptions(ModelState, #{dscli := DSCli}) ->
    CliSubs = maps:fold(
        fun(_SubId, #{topic := Topic}, Acc) ->
            [emqx_topic:join(Topic) | Acc]
        end,
        [],
        maps:get(subs, DSCli)
    ),
    assert_one_to_one(
        "durable subscriptions",
        durable_model_subs(ModelState),
        "DS client subscriptions",
        CliSubs,
        #{model => ModelState, client => DSCli}
    ).

%% Return list of model subscriptions that should use durable delivery
%% path. Shared group is stripped.
durable_model_subs(#{subs := ModelSubs}) ->
    maps:fold(
        fun(TopicFilterBin, SubOpts = #{qos := QoS}, Acc) ->
            {TopicFilter, _} = emqx_topic:parse(TopicFilterBin),
            case TopicFilter of
                #share{topic = TF} ->
                    [TF | Acc];
                TF when is_binary(TF), QoS >= ?QOS_1 ->
                    [TF | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        ModelSubs
    ).

%% Return list of model subscriptions with QoS = 0
direct_model_subs(#{subs := ModelSubs}) ->
    maps:fold(
        fun(TopicFilterBin, #{qos := QoS}, Acc) ->
            {TopicFilter, _} = emqx_topic:parse(TopicFilterBin),
            case QoS of
                ?QOS_0 when is_binary(TopicFilter) ->
                    [TopicFilter | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        ModelSubs
    ).

%% Helper function that sorts and then compares two lists for equality
assert_one_to_one(What1, Expected, What2, Got, Misc) ->
    Diff = snabbkaffe_diff:diff_lists(#{}, lists:sort(Expected), lists:sort(Got)),
    case Diff of
        [] ->
            true;
        _ ->
            logger:error(
                "There must be 1:1 relation between ~s and ~s. Diff:~n~s~n~nMore info: ~p",
                [What1, What2, snabbkaffe_diff:format(Diff), Misc]
            ),
            ?defer_assert(
                error(?FUNCTION_NAME)
            ),
            false
    end.

-endif.
