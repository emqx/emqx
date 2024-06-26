%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_shared_subs).

-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("session_internals.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/4,

    on_streams_replayed/2,
    on_info/3,

    renew_streams/2,
    to_map/2
]).

-type t() :: #{
    agent := emqx_persistent_session_ds_shared_subs_agent:t()
}.
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type opts() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-define(rank_x, rank_shared).
-define(rank_y, 0).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(opts()) -> t().
new(Opts) ->
    #{
        agent => emqx_persistent_session_ds_shared_subs_agent:new(
            agent_opts(Opts)
        )
    }.

-spec open(emqx_persistent_session_ds_state:t(), opts()) ->
    {ok, emqx_persistent_session_ds_state:t(), t()}.
open(S, Opts) ->
    SharedSubscriptions = fold_shared_subs(
        fun(#share{} = TopicFilter, Sub, Acc) ->
            [{TopicFilter, to_agent_subscription(S, Sub)} | Acc]
        end,
        [],
        S
    ),
    Agent = emqx_persistent_session_ds_shared_subs_agent:open(
        SharedSubscriptions, agent_opts(Opts)
    ),
    SharedSubS = #{agent => Agent},
    {ok, S, SharedSubS}.

-spec on_subscribe(
    share_topic_filter(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) -> {ok, emqx_persistent_session_ds_state:t(), t()} | {error, emqx_types:reason_code()}.
on_subscribe(TopicFilter, SubOpts, #{s := S} = Session) ->
    Subscription = emqx_persistent_session_ds_state:get_subscription(TopicFilter, S),
    on_subscribe(Subscription, TopicFilter, SubOpts, Session).

-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_state:t(),
    t()
) ->
    {ok, emqx_persistent_session_ds_state:t(), t(), emqx_persistent_session_ds:subscription()}
    | {error, emqx_types:reason_code()}.
on_unsubscribe(SessionId, TopicFilter, S0, #{agent := Agent0} = SharedSubS0) ->
    case lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription ->
            ?tp(persistent_session_ds_subscription_delete, #{
                session_id => SessionId, topic_filter => TopicFilter
            }),
            Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_unsubscribe(
                Agent0, TopicFilter
            ),
            SharedSubS = SharedSubS0#{agent => Agent1},
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, S0),
            {ok, S, SharedSubS, Subscription}
    end.

-spec renew_streams(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
renew_streams(S0, #{agent := Agent0} = SharedSubS0) ->
    {StreamLeaseEvents, Agent1} = emqx_persistent_session_ds_shared_subs_agent:renew_streams(
        Agent0
    ),
    ?tp(info, shared_subs_new_stream_lease_events, #{stream_lease_events => StreamLeaseEvents}),
    S1 = lists:foldl(
        fun
            (#{type := lease} = Event, S) -> accept_stream(Event, S);
            (#{type := revoke} = Event, S) -> revoke_stream(Event, S)
        end,
        S0,
        StreamLeaseEvents
    ),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S1, SharedSubS1}.

-spec on_streams_replayed(
    emqx_persistent_session_ds_state:t(),
    t()
) -> {emqx_persistent_session_ds_state:t(), t()}.
on_streams_replayed(S, #{agent := Agent0} = SharedSubS0) ->
    %% TODO
    %% Is it sufficient for a report?
    Progress = fold_shared_stream_states(
        fun(TopicFilter, Stream, SRS, Acc) ->
            #srs{it_begin = BeginIt} = SRS,

            StreamProgress = #{
                topic_filter => TopicFilter,
                stream => Stream,
                iterator => BeginIt,
                use_finished => is_use_finished(S, SRS)
            },
            [StreamProgress | Acc]
        end,
        [],
        S
    ),
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_stream_progress(
        Agent0, Progress
    ),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S, SharedSubS1}.

-spec on_info(emqx_persistent_session_ds_state:t(), t(), term()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_info(S, #{agent := Agent0} = SharedSubS0, Info) ->
    Agent1 = emqx_persistent_session_ds_shared_subs_agent:on_info(Agent0, Info),
    SharedSubS1 = SharedSubS0#{agent => Agent1},
    {S, SharedSubS1}.

-spec to_map(emqx_persistent_session_ds_state:t(), t()) -> map().
to_map(_S, _SharedSubS) ->
    %% TODO
    #{}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

fold_shared_subs(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{} = TopicFilter, Sub, Acc0) -> Fun(TopicFilter, Sub, Acc0);
            (_, _Sub, Acc0) -> Acc0
        end,
        Acc,
        S
    ).

fold_shared_stream_states(Fun, Acc, S) ->
    %% TODO
    %% Optimize or cache
    TopicFilters = fold_shared_subs(
        fun
            (#share{} = TopicFilter, #{id := Id} = _Sub, Acc0) ->
                Acc0#{Id => TopicFilter};
            (_, _, Acc0) ->
                Acc0
        end,
        #{},
        S
    ),
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, Stream}, SRS, Acc0) ->
            case TopicFilters of
                #{SubId := TopicFilter} ->
                    Fun(TopicFilter, Stream, SRS, Acc0);
                _ ->
                    Acc0
            end
        end,
        Acc,
        S
    ).

on_subscribe(undefined, TopicFilter, SubOpts, #{props := Props, s := S} = Session) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:n_subscriptions(S) < MaxSubscriptions of
        true ->
            create_new_subscription(TopicFilter, SubOpts, Session);
        false ->
            {error, ?RC_QUOTA_EXCEEDED}
    end;
on_subscribe(Subscription, TopicFilter, SubOpts, Session) ->
    update_subscription(Subscription, TopicFilter, SubOpts, Session).

-dialyzer({nowarn_function, create_new_subscription/3}).
create_new_subscription(TopicFilter, SubOpts, #{
    id := SessionId, s := S0, shared_sub_s := #{agent := Agent0} = SharedSubS0, props := Props
}) ->
    case
        emqx_persistent_session_ds_shared_subs_agent:on_subscribe(
            Agent0, TopicFilter, SubOpts
        )
    of
        {ok, Agent1} ->
            #{upgrade_qos := UpgradeQoS} = Props,
            {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
            SState = #{
                parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts
            },
            S3 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S2
            ),
            Subscription = #{
                id => SubId,
                current_state => SStateId,
                start_time => now_ms()
            },
            S = emqx_persistent_session_ds_state:put_subscription(
                TopicFilter, Subscription, S3
            ),
            SharedSubS = SharedSubS0#{agent => Agent1},
            ?tp(persistent_session_ds_shared_subscription_added, #{
                topic_filter => TopicFilter, session => SessionId
            }),
            {ok, S, SharedSubS};
        {error, _} = Error ->
            Error
    end.

update_subscription(#{current_state := SStateId0, id := SubId} = Sub0, TopicFilter, SubOpts, #{
    s := S0, shared_sub_s := SharedSubS, props := Props
}) ->
    #{upgrade_qos := UpgradeQoS} = Props,
    SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
    case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
        SState ->
            %% Client resubscribed with the same parameters:
            {ok, S0, SharedSubS};
        _ ->
            %% Subsription parameters changed:
            {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            S2 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S1
            ),
            Sub = Sub0#{current_state => SStateId},
            S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, Sub, S2),
            {ok, S, SharedSubS}
    end.

lookup(TopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S) of
        Sub = #{current_state := SStateId} ->
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId, S) of
                #{subopts := SubOpts} ->
                    Sub#{subopts => SubOpts};
                undefined ->
                    undefined
            end;
        undefined ->
            undefined
    end.

accept_stream(
    #{topic_filter := TopicFilter, stream := Stream, iterator := Iterator}, S0
) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% This should not happen.
            %% Agent should have received unsubscribe callback
            %% and should not have passed this stream as a new one
            error(new_stream_without_sub);
        #{id := SubId, current_state := SStateId} ->
            Key = {SubId, Stream},
            case emqx_persistent_session_ds_state:get_stream(Key, S0) of
                undefined ->
                    NewSRS =
                        #srs{
                            rank_x = ?rank_x,
                            rank_y = ?rank_y,
                            it_begin = Iterator,
                            it_end = Iterator,
                            sub_state_id = SStateId
                        },
                    S1 = emqx_persistent_session_ds_state:put_stream(Key, NewSRS, S0),
                    S1;
                _SRS ->
                    S0
            end
    end.

revoke_stream(
    #{topic_filter := TopicFilter, stream := Stream}, S0
) ->
    case emqx_persistent_session_ds_state:get_subscription(TopicFilter, S0) of
        undefined ->
            %% This should not happen.
            %% Agent should have received unsubscribe callback
            %% and should not have revoked this stream
            S0;
        #{id := SubId} ->
            Key = {SubId, Stream},
            case emqx_persistent_session_ds_state:get_stream(Key, S0) of
                undefined ->
                    S0;
                SRS0 ->
                    SRS1 = SRS0#srs{unsubscribed = true},
                    S1 = emqx_persistent_session_ds_state:put_stream(Key, SRS1, S0),
                    S1
            end
    end.

-spec to_agent_subscription(
    emqx_persistent_session_ds_state:t(), emqx_persistent_session_ds:subscription()
) ->
    emqx_persistent_session_ds_shared_subs_agent:subscription().
to_agent_subscription(_S, Subscription) ->
    %% TODO
    %% do we need anything from sub state?
    maps:with([start_time], Subscription).

-spec agent_opts(opts()) -> emqx_persistent_session_ds_shared_subs_agent:opts().
agent_opts(#{session_id := SessionId}) ->
    #{session_id => SessionId}.

-dialyzer({nowarn_function, now_ms/0}).
now_ms() ->
    erlang:system_time(millisecond).

is_use_finished(S, #srs{unsubscribed = Unsubscribed} = SRS) ->
    Unsubscribed andalso emqx_persistent_session_ds_stream_scheduler:is_fully_acked(SRS, S).
