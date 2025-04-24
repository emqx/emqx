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
    on_subscribe/3,
    on_unsubscribe/3,
    on_session_replay/2,
    on_session_drop/2,
    gc/1,
    lookup/2,
    find_by_subid/2,
    fold/4,
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

-export_type([subscription_state_id/0, subscription/0, mode/0, subscription_state/0]).

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

-type mode() :: durable | direct.

-type subscription() :: #{
    %% Session-unique identifier of the subscription. Other objects
    %% can use it as a compact reference:
    id := emqx_persistent_session_ds:subscription_id(),
    %% Subscription mode:
    %% * `durable` goes through DS / Stream Scheduler machinery.
    %% * `direct` goes through the primary broker, similarly to in-mem sessions.
    mode => mode(),
    %% Reference to the current subscription state:
    current_state := subscription_state_id(),
    %% Time when the subscription was added:
    start_time := emqx_ds:time()
}.

-type subscription_state_id() :: integer().

-type subscription_state() :: #{
    parent_subscription := emqx_persistent_session_ds:subscription_id(),
    upgrade_qos := boolean(),
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

%% @doc Process a new subscription
-spec on_subscribe(
    emqx_persistent_session_ds:topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) ->
    {ok | mode_changed, emqx_persistent_session_ds_state:t(), subscription()}
    | {error, ?RC_QUOTA_EXCEEDED}.
on_subscribe(TopicFilter, SubOpts, #{id := SessionId, s := S0, props := Props}) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% This is a new subscription:
            case emqx_persistent_session_ds_state:n_subscriptions(S0) < MaxSubscriptions of
                true ->
                    create_subscription(TopicFilter, SubOpts, SessionId, Props, S0);
                false ->
                    {error, ?RC_QUOTA_EXCEEDED}
            end;
        Sub0 ->
            update_subscription(TopicFilter, SubOpts, Sub0, SessionId, Props, S0)
    end.

create_subscription(TopicFilter, SubOpts, SessionId, #{upgrade_qos := UpgradeQoS}, S0) ->
    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
    Mode = desired_subscription_mode(SubOpts),
    SState = #{
        parent_subscription => SubId,
        upgrade_qos => UpgradeQoS,
        subopts => SubOpts
    },
    Subscription = #{
        id => SubId,
        mode => Mode,
        current_state => SStateId,
        start_time => now_ms()
    },
    ok = add_route(Mode, SessionId, TopicFilter, SubOpts),
    S3 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S2),
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Subscription, S3),
    ?tp(persistent_session_ds_subscription_added, #{
        topic_filter => TopicFilter,
        session => SessionId,
        mode => Mode
    }),
    {ok, gc(S), Subscription}.

update_subscription(TopicFilter, SubOpts, Sub0, SessionId, #{upgrade_qos := UpgradeQoS}, S0) ->
    #{id := SubId, current_state := SStateId0} = Sub0,
    SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
    Mode = subscription_mode(Sub0),
    Desired = desired_subscription_mode(SubOpts),
    case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
        SState ->
            %% Client resubscribed with the same parameters:
            Sub = ensure_subscription_mode(Sub0),
            {ok, gc(S0), Sub};
        OldSState when Mode == durable orelse Desired == durable ->
            %% Durable subscription parameters changed:
            {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            S2 = emqx_persistent_session_ds_state:put_subscription_state(SStateId, SState, S1),
            S3 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId0, OldSState#{superseded_by => SStateId}, S2
            ),
            case Desired of
                Mode ->
                    Res = ok,
                    Sub = Sub0#{current_state := SStateId};
                _Changed ->
                    %% Essentially a new subscription:
                    ok = convert_route(Mode, Desired, SessionId, TopicFilter, SubOpts),
                    Res = mode_changed,
                    Sub = Sub0#{
                        current_state := SStateId,
                        mode => Desired,
                        start_time => now_ms()
                    }
            end,
            S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S3),
            {Res, S, Sub};
        _OldSState when Mode == direct andalso Desired == direct ->
            %% Direct subscription parameters changed:
            %% Update subscription state in-place.
            S = emqx_persistent_session_ds_state:put_subscription_state(SStateId0, SState, S0),
            {ok, S, Sub0}
    end.

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
            ?tp(persistent_session_ds_subscription_delete, #{
                session_id => SessionId, topic_filter => TopicFilter
            }),
            ok = delete_route(subscription_mode(Subscription), SessionId, TopicFilter),
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0),
            {ok, gc(S), Subscription}
    end.

-spec on_session_replay(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) ->
    ok.
on_session_replay(SessionId, S) ->
    fold(
        fun(TopicFilter, Subscription, _) ->
            restore_subscription(SessionId, TopicFilter, Subscription, S)
        end,
        ok,
        S,
        _Include = [direct]
    ).

restore_subscription(SessionId, TopicFilter, Sub = #{current_state := SStateId}, S) ->
    case subscription_mode(Sub) of
        durable ->
            ok;
        direct ->
            SState = emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
            #{subopts := SubOpts} = SState,
            add_direct_route(SessionId, TopicFilter, SubOpts)
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
        _Include = [direct]
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
        Sub0 = #{current_state := SStateId} ->
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId, S) of
                #{subopts := SubOpts} ->
                    Sub = ensure_subscription_mode(Sub0),
                    Sub#{subopts => SubOpts};
                undefined ->
                    undefined
            end;
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
        _Include = [direct]
    ).

%% @doc Fold over subscriptions.
%% Includes only non-shared and durable by default.
%% To include shared subscriptions in the fold, add `shared` to `Include` list.
%% To include direct subscriptions in the fold, add `direct` to `Include` list.
fold(Fun, AccIn, S, Include) ->
    IncludeShared = lists:member(shared, Include),
    IncludeDirect = lists:member(direct, Include),
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun(TopicFilter, Sub, Acc) ->
            Mode = subscription_mode(Sub),
            case TopicFilter of
                #share{} when IncludeShared ->
                    Fun(TopicFilter, Sub, Acc);
                <<_/binary>> when Mode == direct andalso IncludeDirect ->
                    Fun(TopicFilter, Sub, Acc);
                <<_/binary>> when Mode == durable ->
                    Fun(TopicFilter, Sub, Acc);
                _ ->
                    Acc
            end
        end,
        AccIn,
        S
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

subscription_mode(Sub) ->
    maps:get(mode, Sub, durable).

ensure_subscription_mode(Sub = #{mode := _}) ->
    Sub;
ensure_subscription_mode(Sub = #{}) ->
    Sub#{mode => subscription_mode(Sub)}.

desired_subscription_mode(#{qos := ?QOS_0}) ->
    direct;
desired_subscription_mode(#{qos := _QoS12}) ->
    durable.

add_route(durable, SessionId, Topic, _SubOpts) ->
    add_persistent_route(SessionId, Topic);
add_route(direct, SessionId, Topic, SubOpts) ->
    add_direct_route(SessionId, Topic, SubOpts).

delete_route(durable, SessionId, Topic) ->
    delete_persistent_route(SessionId, Topic);
delete_route(direct, SessionId, Topic) ->
    delete_direct_route(SessionId, Topic).

convert_route(direct, durable, SessionId, Topic, _SubOpts) ->
    %% NOTE: This switch is obviously non-atomic.
    ok = add_persistent_route(SessionId, Topic),
    delete_direct_route(SessionId, Topic);
convert_route(durable, direct, SessionId, Topic, SubOpts) ->
    ok = add_direct_route(SessionId, Topic, SubOpts),
    delete_persistent_route(SessionId, Topic).

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
