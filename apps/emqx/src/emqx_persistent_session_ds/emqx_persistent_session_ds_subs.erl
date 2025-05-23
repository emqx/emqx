%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module encapsulates the data related to the client's
%% subscriptions. It tries to reppresent the subscriptions as if they
%% were a simple key-value map.
%%
%% In reality, however, the session has to retain old the
%% subscriptions for longer to ensure the consistency of message
%% replay.
-module(emqx_persistent_session_ds_subs).

%% API:
-export([
    fold/4,
    on_subscribe/3,
    on_unsubscribe/3,
    on_session_replay/2,
    on_session_drop/2,
    gc/1,
    lookup/2,
    find_by_subid/2,
    to_map/1
]).

%% Management API:
-export([
    cold_get_subscription/2
]).

-ifdef(TEST).
-export([
    state_invariants/2
]).
-endif.

-export_type([subscription_state_id/0, subscription/0, subscription_state/0]).

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
    start_time := emqx_ds:time()
}.

-type subscription_state_id() :: integer().

-type subscription_mode() :: durable | direct.

-type subscription_state() :: #{
    parent_subscription := emqx_persistent_session_ds:subscription_id(),
    upgrade_qos := boolean(),
    %% Mode of the subscription:
    %% * `durable' subscriptions use DS machinery to serve messages.
    %% * `direct' subscriptions go through the primary broker, similarly
    %%    to in-mem sessions. Applicable to QoS0 subscriptions.
    %% It's essentially an override for the case when the active mode
    %% is different from what QoS level of the subscription implies.
    mode => subscription_mode(),
    %% Time when the subscription was restarted:
    %% Overrides start time of the parent subscription.
    restart_time => emqx_ds:time(),
    %% SubOpts:
    subopts := #{
        nl => _,
        qos => _,
        rap => _,
        subid => _,
        _ => _
    },
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

%% @doc Fold over subscriptions:
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
                case get_subscription_mode(Sub, S) of
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

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) ->
    {ok | upgrade, subscription_mode(), emqx_persistent_session_ds_state:t(), Subscription}
    | {error, ?RC_QUOTA_EXCEEDED}
when
    Subscription :: emqx_persistent_session_ds:subscription().
on_subscribe(TopicFilter, SubOpts, #{id := SessionId, s := S0, props := Props}) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    NSubscriptions = emqx_persistent_session_ds_state:n_subscriptions(S0),
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined when NSubscriptions < MaxSubscriptions ->
            %% This is a new subscription:
            {Mode, S, Subscription} =
                create_subscription(TopicFilter, SubOpts, SessionId, Props, S0),
            {ok, Mode, gc(S), Subscription};
        undefined ->
            {error, ?RC_QUOTA_EXCEEDED};
        Sub0 ->
            %% This is an existing subscription:
            {Outcome, Mode, S, Subscription} =
                update_subscription(TopicFilter, SubOpts, Sub0, SessionId, Props, S0),
            {Outcome, Mode, gc(S), Subscription}
    end.

create_subscription(TopicFilter, SubOpts, SessionId, #{upgrade_qos := UpgradeQoS}, S0) ->
    Mode = desired_subscription_mode(SubOpts),
    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
    SState = #{
        parent_subscription => SubId,
        upgrade_qos => UpgradeQoS,
        subopts => SubOpts
    },
    Subscription = #{
        id => SubId,
        current_state => SStateId,
        start_time => now_ms()
    },
    S3 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S2),
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Subscription, S3),
    ?tp(persistent_session_ds_subscription_added, #{
        topic_filter => TopicFilter,
        session => SessionId,
        mode => Mode
    }),
    %% Immediate side-effects:
    %% If the session fails to commit, routing might become inconsistent.
    ok = add_route(Mode, SessionId, TopicFilter, SubOpts),
    {Mode, S, session_subscription(Subscription, SState)}.

update_subscription(TopicFilter, SubOpts, Sub0, SessionId, #{upgrade_qos := UpgradeQoS}, S0) ->
    #{id := SubId, current_state := SStateId0} = Sub0,
    OldSState = emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0),
    case OldSState of
        #{parent_subscription := SubId, subopts := SubOpts} ->
            %% Client resubscribed with the same parameters:
            Mode = sstate_subscription_mode(OldSState),
            {ok, Mode, S0, session_subscription(Sub0, OldSState)};
        OldSState ->
            %% Subsription parameters changed:
            DesiredMode = desired_subscription_mode(SubOpts),
            SState0 = #{
                parent_subscription => SubId,
                upgrade_qos => UpgradeQoS,
                subopts => SubOpts
            },
            case sstate_subscription_mode(OldSState) of
                DesiredMode ->
                    %% No change in subscription mode:
                    Outcome = ok,
                    Mode = DesiredMode,
                    SState = SState0;
                durable when DesiredMode =:= direct ->
                    %% Downgrade from durable to direct is not possible when the session
                    %% is active. Subscription will remain durable while the client is
                    %% connected.
                    Outcome = ok,
                    Mode = durable,
                    SState = SState0#{mode => Mode};
                direct when DesiredMode =:= durable ->
                    %% Upgrade from direct to durable is possible.
                    %% The client however is not expecting to see any messages from the past,
                    %% so we need to make scheduler aware the subscription was restarted.
                    Outcome = upgrade,
                    Mode = durable,
                    SState = SState0#{restart_time => now_ms()}
            end,
            {Sub, S} = update_subscription_state(TopicFilter, Sub0, OldSState, SState, S0),
            ?tp(persistent_session_ds_subscription_updated, #{
                topic_filter => TopicFilter,
                session => SessionId,
                mode => Mode,
                upgrade => Outcome =:= upgrade
            }),
            %% Immediate side-effects:
            case Outcome of
                ok ->
                    ok;
                upgrade ->
                    ok = add_route(durable, SessionId, TopicFilter, SubOpts),
                    ok = delete_route(direct, SessionId, TopicFilter)
            end,
            {Outcome, Mode, S, session_subscription(Sub, SState)}
    end.

update_subscription_state(TopicFilter, Sub0, OldSState, SState, S0) ->
    #{current_state := SStateId0} = Sub0,
    {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    S2 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S1),
    S3 = emqx_persistent_session_ds_state:put_subscription_state(
        SStateId0, OldSState#{superseded_by => SStateId}, S2
    ),
    Sub = Sub0#{current_state := SStateId},
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S3),
    {Sub, S}.

%% @doc Process UNSUBSCRIBE
-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_state:t()
) ->
    {ok, emqx_persistent_session_ds_state:t(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(SessionId, TopicFilter, S0) ->
    case lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription ->
            Mode = get_subscription_mode(Subscription, S0),
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0),
            ?tp(persistent_session_ds_subscription_delete, #{
                session_id => SessionId,
                topic_filter => TopicFilter,
                mode => Mode
            }),
            %% Immediate side-effects:
            ok = delete_route(Mode, SessionId, TopicFilter),
            {ok, gc(S), Subscription}
    end.

%% @doc Restart (and potentially downgrade) subscriptions during session replay.
%% 1. Resubscribes to the broker for direct subscriptions.
%% 2. Downgrades durable QoS0 subscriptions to direct subscriptions.
%% Returns a list of events, which currently includes only subscription downgrades.
%% To be called before the stream scheduler is initialized.
-spec on_session_replay(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) ->
    {emqx_persistent_session_ds_state:t(), [Event]}
when
    Event :: {downgrade, subscription_mode(), TopicFilter, subscription()},
    TopicFilter :: emqx_persistent_session_ds:topic_filter().
on_session_replay(SessionId, S0) ->
    fold(
        fun(TopicFilter, Sub0, {SAcc, Acc}) ->
            case restart_subscription(TopicFilter, Sub0, SessionId, SAcc) of
                ok ->
                    {SAcc, Acc};
                {Outcome, S} ->
                    {S, [Outcome | Acc]}
            end
        end,
        {S0, []},
        S0,
        [direct]
    ).

restart_subscription(TopicFilter, Sub0 = #{current_state := SStateId}, SessionId, S0) ->
    OldSState = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S0),
    case OldSState of
        #{mode := durable, subopts := SubOpts} ->
            %% Downgrade from durable to direct is now possible:
            Mode = direct = desired_subscription_mode(SubOpts),
            SState = maps:remove(mode, OldSState),
            {Sub, S} = update_subscription_state(TopicFilter, Sub0, OldSState, SState, S0),
            ?tp(persistent_session_ds_subscription_downgraded, #{
                topic_filter => TopicFilter,
                session => SessionId,
                mode => Mode
            }),
            %% Immediate side-effects:
            ok = add_route(direct, SessionId, TopicFilter, SubOpts),
            ok = delete_route(durable, SessionId, TopicFilter),
            {{downgrade, Mode, TopicFilter, Sub}, S};
        #{subopts := SubOpts} ->
            case desired_subscription_mode(SubOpts) of
                direct ->
                    %% Restore the subscription in the broker:
                    ok = add_route(direct, SessionId, TopicFilter, SubOpts);
                durable ->
                    %% Nothing to do:
                    ok
            end,
            ok
    end.

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) -> ok.
on_session_drop(SessionId, S0) ->
    _ = fold(
        fun(TopicFilter, _Subscription, S) ->
            case on_unsubscribe(SessionId, TopicFilter, S) of
                {ok, S1, _} -> S1;
                _ -> S
            end
        end,
        S0,
        S0,
        [direct]
    ),
    ok.

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

%% @doc Merge subscription with its current state:
-spec session_subscription(subscription(), subscription_state()) ->
    emqx_persistent_session_ds:subscription().
session_subscription(Subscription, SState) ->
    case SState of
        #{subopts := SubOpts, restart_time := RestartTime} ->
            Subscription#{subopts => SubOpts, restart_time => RestartTime};
        #{subopts := SubOpts} ->
            Subscription#{subopts => SubOpts};
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

-spec cold_get_subscription(emqx_persistent_session_ds:id(), emqx_types:topic()) ->
    emqx_persistent_session_ds:subscription() | undefined.
cold_get_subscription(SessionId, Topic) ->
    case emqx_persistent_session_ds_state:cold_get_subscription(SessionId, Topic) of
        [Sub = #{current_state := SStateId}] ->
            case
                emqx_persistent_session_ds_state:cold_get_subscription_state(SessionId, SStateId)
            of
                [#{subopts := Subopts}] ->
                    Sub#{subopts => Subopts};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

%%================================================================================
%% Internal functions
%%================================================================================

desired_subscription_mode(#{qos := ?QOS_0}) ->
    direct;
desired_subscription_mode(#{qos := _QoS12}) ->
    durable.

get_subscription_mode(#{current_state := SStateId}, S) ->
    SState = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
    sstate_subscription_mode(SState).

sstate_subscription_mode(#{mode := Mode}) ->
    Mode;
sstate_subscription_mode(#{subopts := SubOpts}) ->
    desired_subscription_mode(SubOpts).

add_route(durable, SessionId, Topic, _SubOpts) ->
    add_persistent_route(SessionId, Topic);
add_route(direct, SessionId, Topic, SubOpts) ->
    add_direct_route(SessionId, Topic, SubOpts).

delete_route(durable, SessionId, Topic) ->
    delete_persistent_route(SessionId, Topic);
delete_route(direct, SessionId, Topic) ->
    delete_direct_route(SessionId, Topic).

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

now_ms() ->
    erlang:system_time(millisecond).

%%================================================================================
%% Test
%%================================================================================

-ifdef(TEST).

-spec state_invariants(emqx_persistent_session_ds_fuzzer:model_state(), #{s := map()}) -> boolean().
state_invariants(#{subs := ModelSubs}, #{s := S}) ->
    #{subscriptions := Subs, subscription_states := SStates} = S,
    ?defer_assert(
        ?assertEqual(
            lists:sort(maps:keys(ModelSubs)),
            lists:sort(maps:keys(Subs)),
            "There should be 1:1 relationship between model and session's subscriptions"
        )
    ),
    %% Verify that QoS of the current subscription state matches the model QoS:
    maps:foreach(
        fun(TopicFilter, #{qos := ExpectedQoS}) ->
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

-endif.
