%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module
%% * handles creation and management of _shared_ subscriptions for the session;
%% * provides streams to the session's DS client;
%% * reports progress of stream replay to the leader
%%
%% The logic is quite straightforward; most of the parts resemble the logic of the
%% `emqx_persistent_session_ds_subs` (subscribe/unsubscribe)
%% but some data is sent or received from the `emqx_ds_shared_sub_agent`
%% which communicates with remote shared subscription leaders.

-module(emqx_persistent_session_ds_shared_subs).

-export([
    new/0,
    open/1,

    on_subscribe/3,
    on_unsubscribe/2,
    on_disconnect/2,

    on_stream_replay/3,
    on_streams_gc/2,
    on_info/3,

    to_map/2
]).

%% Management API:
-export([
    cold_get_subscription/2
]).

-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("session_internals.hrl").
-include("emqx_session.hrl").
-include("emqx_ds_shared_sub_borrower.hrl").

-include_lib("snabbkaffe/include/trace.hrl").

%%--------------------------------------------------------------------
%% Type declarations
%%--------------------------------------------------------------------

-type progress() ::
    #{
        iterator := emqx_ds:iterator()
    }.

-type agent_stream_progress() :: #{
    stream := emqx_ds:stream(),
    progress := progress(),
    use_finished := boolean()
}.

-record(borrower_entry, {
    borrower_id :: emqx_ds_shared_sub_proto:borrower_id(),
    topic_filter :: emqx_types:share(),
    borrower :: emqx_ds_shared_sub_borrower:t()
}).

-type borrower_entry() :: #borrower_entry{}.

-type t() :: #{
    borrowers := #{
        emqx_persistent_session_ds:subscription_id() => borrower_entry()
    }
}.

-type stream_lease() :: #{
    type := lease,
    subscription_id := emqx_persistent_session_ds:subscription_id(),
    share_topic_filter := emqx_types:share(),
    stream := emqx_ds:stream(),
    progress := emqx_persistent_session_ds_shared_subs:progress()
}.

-type stream_revoke() :: #{
    type := revoke,
    subscription_id := emqx_persistent_session_ds:subscription_id(),
    share_topic_filter := emqx_types:share(),
    stream := emqx_ds:stream()
}.

-type stream_lease_event() :: stream_lease() | stream_revoke().

-export_type([
    progress/0,
    agent_stream_progress/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #{
        borrowers => #{}
    }.

-spec open(emqx_persistent_session_ds_state:t()) ->
    {ok, emqx_persistent_session_ds_state:t(), t()}.
open(S0) ->
    SessionId = emqx_persistent_session_ds_state:get_id(S0),
    ToOpen = fold_shared_subs(
        fun(#share{} = ShareTopicFilter, Subscription, ToOpenAcc) ->
            [{ShareTopicFilter, Subscription} | ToOpenAcc]
        end,
        [],
        S0
    ),
    SharedSubS = lists:foldl(
        fun({ShareTopicFilter, #{id := SubscriptionId}}, SharedSubSAcc) ->
            ?tp(debug, ds_shared_sub_agent_open, #{
                subscription_id => SubscriptionId,
                topic_filter => ShareTopicFilter
            }),
            add_borrower(SessionId, SharedSubSAcc, SubscriptionId, ShareTopicFilter)
        end,
        new(),
        ToOpen
    ),
    S = terminate_streams(S0),
    {ok, S, SharedSubS}.

%%--------------------------------------------------------------------
%% on_subscribe

-spec on_subscribe(
    emqx_types:share(),
    emqx_types:subopts(),
    emqx_persistent_session_ds:session()
) -> {ok, emqx_persistent_session_ds:session()} | {error, emqx_types:reason_code()}.
on_subscribe(#share{} = ShareTopicFilter, SubOpts, #{s := S} = Session) ->
    Subscription = emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S),
    on_subscribe(Subscription, ShareTopicFilter, SubOpts, Session).

%%--------------------------------------------------------------------
%% on_subscribe internal functions

on_subscribe(undefined, ShareTopicFilter, SubOpts, #{props := Props, s := S} = Session) ->
    #{max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:n_subscriptions(S) < MaxSubscriptions of
        true ->
            create_new_subscription(ShareTopicFilter, SubOpts, Session);
        false ->
            {error, ?RC_QUOTA_EXCEEDED}
    end;
on_subscribe(Subscription, ShareTopicFilter, SubOpts, Session) ->
    update_subscription(Subscription, ShareTopicFilter, SubOpts, Session).

create_new_subscription(ShareTopicFilter, SubOpts, Session0) ->
    #{
        s := S0,
        props := #{upgrade_qos := UpgradeQoS}
    } = Session0,
    #share{group = Group, topic = Topic} = ShareTopicFilter,
    case emqx_ds_shared_sub:declare(Group, Topic, #{}) of
        {ok, _} ->
            %% NOTE
            %% Module that manages durable shared subscription / durable queue state is
            %% also responsible for propagating corresponding routing updates.
            {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
            SState = #{
                parent_subscription => SubId,
                upgrade_qos => UpgradeQoS,
                subopts => SubOpts,
                share_topic_filter => ShareTopicFilter,
                mode => durable
            },
            S3 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S2
            ),
            Subscription = #{
                id => SubId,
                current_state => SStateId,
                start_time => emqx_persistent_session_ds:now_ms()
            },
            S = emqx_persistent_session_ds_state:put_subscription(
                ShareTopicFilter, Subscription, S3
            ),
            ?tp(debug, ds_shared_subs_on_subscribe_new, #{
                share_topic_filter => ShareTopicFilter,
                subscription_id => SubId
            }),
            Session1 = Session0#{s := S},
            %% Perform side effects:
            Session = ds_client_subscribe(
                ShareTopicFilter,
                SubId,
                borrower_subscribe(SubId, ShareTopicFilter, SubOpts, Session1)
            ),
            {ok, Session};
        {error, Class, Reason} ->
            ?tp(warning, ds_shared_sub_agent_queue_declare_failed, #{
                group => Group,
                topic => Topic,
                class => Class,
                reason => Reason
            }),
            {error, ?RC_UNSPECIFIED_ERROR}
    end.

update_subscription(
    #{current_state := SStateId0, id := SubId} = Sub0,
    ShareTopicFilter,
    SubOpts,
    Session
) ->
    #{s := S0, props := Props} = Session,
    #{upgrade_qos := UpgradeQoS} = Props,
    SState = #{
        parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts, mode => durable
    },
    case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
        SState ->
            %% Client resubscribed with the same parameters:
            {ok, Session};
        _ ->
            %% Subsription parameters changed:
            {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            S2 = emqx_persistent_session_ds_state:put_subscription_state(
                SStateId, SState, S1
            ),
            Sub = Sub0#{current_state => SStateId},
            S = emqx_persistent_session_ds_state:put_subscription(ShareTopicFilter, Sub, S2),
            {ok, Session#{s := S}}
    end.

%%--------------------------------------------------------------------------------
%% Side effects
%%--------------------------------------------------------------------------------

-spec ds_client_subscribe(
    emqx_types:share(),
    emqx_persistent_session_ds:subscription_id(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
ds_client_subscribe(#share{topic = TopicFilter}, SubId, Sess0 = #{dscli := CLI0}) ->
    SubOpts = #{
        max_unacked => emqx_config:get([durable_sessions, batch_size])
    },
    Opts = #{
        id => SubId,
        db => ?PERSISTENT_MESSAGE_DB,
        topic => emqx_ds:topic_words(TopicFilter),
        stream_discovery => false,
        ds_sub_opts => SubOpts
    },
    case emqx_ds_client:subscribe(CLI0, Opts, Sess0) of
        {ok, CLI, Sess} ->
            Sess#{dscli := CLI};
        {error, already_exists} ->
            Sess0
    end.

-spec ds_client_attach_iterator(
    emqx_persistent_session_ds:subscription_id(),
    emqx_ds:stream(),
    emqx_ds:iterator(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
ds_client_attach_iterator(SubId, Stream, It, Session0 = #{dscli := DSCli0}) ->
    {DSCli, Session} = emqx_ds_client:attach_iterator(SubId, Stream, It, DSCli0, Session0),
    Session#{dscli := DSCli}.

-spec ds_client_unsubscribe(
    emqx_persistent_session_ds:subscription_id(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
ds_client_unsubscribe(SubId, Session0 = #{dscli := DSCli0}) ->
    {ok, DSCli, Session} = emqx_ds_client:unsubscribe(DSCli0, SubId, Session0),
    Session#{dscli := DSCli}.

-spec borrower_subscribe(
    emqx_persistent_session_ds:subscription_id(),
    emqx_types:share(),
    emqx_persistent_session_ds_subs:subopts(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
borrower_subscribe(
    SubId, ShareTopicFilter, _SubOpts, Session = #{id := SessionId, shared_sub_s := SharedSubS0}
) ->
    SharedSubS = add_borrower(SessionId, SharedSubS0, SubId, ShareTopicFilter),
    Session#{shared_sub_s := SharedSubS}.

-spec agent_unsubscribe(
    emqx_persistent_session_ds:subscription_id(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
agent_unsubscribe(SubId, Session = #{id := SessionId, shared_sub_s := SharedSubS0}) ->
    {[], SharedSubS} = with_borrower(
        SessionId,
        SharedSubS0,
        SubId,
        fun(_BorrowerId, Borrower) ->
            emqx_ds_shared_sub_borrower:on_unsubscribe(Borrower)
        end
    ),
    Session#{shared_sub_s := SharedSubS}.

%%--------------------------------------------------------------------
%% on_unsubscribe

-spec on_unsubscribe(
    emqx_types:share(),
    emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds:session(), t(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(ShareTopicFilter, Session0) ->
    #{id := SessionId, s := S0, shared_sub_s := SharedSubS0} = Session0,
    case lookup(ShareTopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        #{id := SubId} = Subscription ->
            ?tp(debug, ds_shared_subs_on_unsubscribe, #{
                subscription_id => SubId,
                session_id => SessionId,
                share_topic_filter => ShareTopicFilter
            }),
            {S1, SharedSubS} = on_streams_gc(S0, SharedSubS0),
            S = emqx_persistent_session_ds_state:del_subscription(ShareTopicFilter, S1),
            Session1 = Session0#{s := S, shared_sub_s := SharedSubS},
            %% Perform side effects:
            Session = ds_client_unsubscribe(SubId, agent_unsubscribe(SubId, Session1)),
            {ok, Session, Subscription}
    end.

%%--------------------------------------------------------------------
%% on_streams_replay

-spec on_stream_replay(
    emqx_persistent_session_ds_state:t(),
    t(),
    emqx_persistent_session_ds:stream_key()
) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_stream_replay(S, SharedS, StreamKey) ->
    report_progress(S, SharedS, [StreamKey], _NeedUnacked = false).

%%--------------------------------------------------------------------
%% on_streams_replay gc

-spec on_streams_gc(emqx_persistent_session_ds_state:t(), t()) ->
    {emqx_persistent_session_ds_state:t(), t()}.
on_streams_gc(S, SharedS) ->
    report_progress(S, SharedS, all, _NeedUnacked = false).

%%--------------------------------------------------------------------
%% on_streams_replay/on_streams_gc internal functions

report_progress(
    S, SharedSubsS0, StreamKeySelector, NeedUnacked
) ->
    SessionId = emqx_persistent_session_ds_state:get_id(S),
    StreamProgresses = stream_progresses(S, SharedSubsS0, StreamKeySelector, NeedUnacked),
    SharedSubsS = maps:fold(
        fun(SubscriptionId, Progresses, Acc0) ->
            {[], Acc} =
                with_borrower(
                    SessionId,
                    Acc0,
                    SubscriptionId,
                    fun(_BorrowerId, Borrower) ->
                        emqx_ds_shared_sub_borrower:on_stream_progress(Borrower, Progresses)
                    end
                ),
            Acc
        end,
        SharedSubsS0,
        StreamProgresses
    ),
    {S, SharedSubsS}.

stream_progresses(S, SharedS, StreamKeySelector, NeedUnacked) ->
    StreamStates = select_stream_states(S, SharedS, StreamKeySelector),
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    lists:foldl(
        fun({{SubId, Stream}, SRS}, ProgressesAcc) ->
            ?tp(debug, ds_shared_subs_stream_progress, #{
                stream => Stream,
                srs => SRS,
                fully_acked => is_stream_fully_acked(CommQos1, CommQos2, SRS),
                comm_qos1 => CommQos1,
                comm_qos2 => CommQos2
            }),
            case
                is_stream_started(CommQos1, CommQos2, SRS) and
                    (NeedUnacked or is_stream_fully_acked(CommQos1, CommQos2, SRS))
            of
                true ->
                    StreamProgress = stream_progress(CommQos1, CommQos2, Stream, SRS),
                    maps:update_with(
                        SubId,
                        fun(Progresses) -> [StreamProgress | Progresses] end,
                        [StreamProgress],
                        ProgressesAcc
                    );
                false ->
                    ProgressesAcc
            end
        end,
        #{},
        StreamStates
    ).

stream_progress(
    CommQos1,
    CommQos2,
    Stream,
    #srs{
        it_end = EndIt,
        it_begin = BeginIt
    } = SRS
) ->
    {FullyAcked, Iterator} =
        case is_stream_fully_acked(CommQos1, CommQos2, SRS) of
            true -> {true, EndIt};
            false -> {false, BeginIt}
        end,
    #{
        stream => Stream,
        progress => #{
            iterator => Iterator
        },
        use_finished => is_use_finished(SRS),
        fully_acked => FullyAcked
    }.

select_stream_states(S, SharedSubsS, all) ->
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream} = Key, SRS, Acc0) ->
            case has_subscription(SharedSubsS, SubId) of
                true ->
                    [{Key, SRS} | Acc0];
                false ->
                    Acc0
            end
        end,
        [],
        S
    );
select_stream_states(S, SharedSubsS, StreamKeys) ->
    lists:filtermap(
        fun({SubId, _} = Key) ->
            case has_subscription(SharedSubsS, SubId) of
                true ->
                    case emqx_persistent_session_ds_state:get_stream(Key, S) of
                        undefined -> false;
                        SRS -> {true, {Key, SRS}}
                    end;
                false ->
                    false
            end
        end,
        StreamKeys
    ).

on_disconnect(S0, #{borrowers := Borrowers} = SharedSubS0) ->
    S = terminate_streams(S0),
    Progresses = stream_progresses(S, SharedSubS0, all, _NeedUnacked = true),
    SharedSubS = lists:foldl(
        fun(SubscriptionId, Acc) ->
            Progress = maps:get(SubscriptionId, Progresses, []),
            disconnect_borrower(Acc, SubscriptionId, Progress)
        end,
        SharedSubS0,
        maps:keys(Borrowers)
    ),
    {S, SharedSubS}.

%% Mark all shared SRSs as unsubscribed.
-spec terminate_streams(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
terminate_streams(S0) ->
    fold_shared_stream_states(
        fun(_ShareTopicFilter, SubId, Stream, SRS0, S) ->
            SRS = SRS0#srs{unsubscribed = true},
            emqx_persistent_session_ds_state:put_stream({SubId, Stream}, SRS, S)
        end,
        S0,
        S0
    ).

-spec terminate_stream(
    emqx_persistent_session_ds:subscription_id(),
    emqx_ds:stream(),
    emqx_persistent_session_ds_state:t()
) -> emqx_persistent_session_ds_state:t().
terminate_stream(SubId, Stream, S) ->
    maybe
        SRS0 = emqx_persistent_session_ds_state:get_stream({SubId, Stream}, S),
        #srs{} ?= SRS0,
        SRS = SRS0#srs{unsubscribed = true},
        emqx_persistent_session_ds_state:put_stream({SubId, Stream}, SRS, S)
    else
        undefined ->
            S
    end.

%%--------------------------------------------------------------------
%% on_info

-spec on_info(
    #shared_sub_message{},
    emqx_persistent_session_ds:session(),
    emqx_types:clientinfo()
) ->
    emqx_persistent_session_ds:session().
on_info(
    ?shared_sub_message(
        SubscriptionId,
        #message_to_borrower{borrower_id = BorrowerId, message = Message}
    ),
    Session0,
    _ClientInfo
) ->
    #{id := SessionId, shared_sub_s := SharedSubS0} = Session0,
    ?tp(debug, ds_shared_sub_message_to_borrower, #{
        session_id => SessionId,
        subscription_id => SubscriptionId,
        borrower_id => BorrowerId,
        message => Message
    }),
    {StreamLeaseEvents, SharedSubS} = with_borrower(
        SessionId,
        SharedSubS0,
        SubscriptionId,
        fun(KnownBorrowerId, Borrower) ->
            %% We may have recreated invalidated Borrower, resulting in a new BorrowerId.
            %% Ignore the messages to the old Borrower.
            case KnownBorrowerId of
                BorrowerId ->
                    emqx_ds_shared_sub_borrower:on_info(Borrower, Message);
                _ ->
                    {ok, [], Borrower}
            end
        end
    ),
    Session = Session0#{shared_sub_s := SharedSubS},
    handle_events(Session, StreamLeaseEvents).

-spec handle_events(emqx_persistent_session_ds:session(), [stream_lease_event()]) ->
    emqx_persistent_session_ds:session().
handle_events(Session, []) ->
    Session;
handle_events(Session0, StreamLeaseEvents) ->
    ?tp(debug, ds_shared_subs_new_stream_lease_events, #{
        stream_lease_events => StreamLeaseEvents
    }),
    Session =
        #{s := S0, shared_sub_s := SharedSubS0} =
        lists:foldl(
            fun
                (#{type := lease} = Event, Acc) ->
                    handle_lease_stream(Event, Acc);
                (#{type := revoke} = Event, Acc) ->
                    handle_revoke_stream(Event, Acc)
            end,
            Session0,
            StreamLeaseEvents
        ),
    {S, SharedSubS} = on_streams_gc(S0, SharedSubS0),
    Session#{s := S, shared_sub_s := SharedSubS}.

-spec handle_lease_stream(stream_lease_event(), emqx_persistent_session_ds:session()) ->
    emqx_persistent_session_ds:session().
handle_lease_stream(
    #{share_topic_filter := ShareTopicFilter} = Event,
    Session = #{s := S}
) ->
    case emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S) of
        undefined ->
            %% This should not happen
            Session;
        Sub ->
            add_stream_to_session(Event, Sub, Session)
    end.

-spec add_stream_to_session(
    stream_lease(),
    emqx_persistent_session_ds_subs:subscription(),
    emqx_persistent_session_ds:session()
) ->
    emqx_persistent_session_ds:session().
add_stream_to_session(
    #{stream := Stream, progress := #{iterator := Iterator}} = _Event,
    #{id := SubId, current_state := SStateId} = _Sub,
    Session = #{s := S0}
) ->
    Key = {SubId, Stream},
    NeedCreateStream =
        case emqx_persistent_session_ds_state:get_stream(Key, S0) of
            undefined ->
                true;
            #srs{unsubscribed = true} ->
                true;
            _SRS ->
                false
        end,
    {ok, {Shard, Generation}} = emqx_ds:slab_of_stream(?PERSISTENT_MESSAGE_DB, Stream),
    case NeedCreateStream of
        true ->
            NewSRS =
                #srs{
                    rank_x = Shard,
                    rank_y = Generation,
                    it_begin = Iterator,
                    it_end = Iterator,
                    sub_state_id = SStateId
                },
            ?tp(debug, ds_shared_subs_add_stream_to_session, #{
                stream => Stream,
                sub_id => SubId
            }),
            S = emqx_persistent_session_ds_state:put_stream(Key, NewSRS, S0),
            ds_client_attach_iterator(SubId, Stream, Iterator, Session#{s := S});
        false ->
            Session
    end.

-spec handle_revoke_stream(
    stream_revoke(), emqx_persistent_session_ds:session()
) -> emqx_persistent_session_ds:session().
handle_revoke_stream(
    #{subscription_id := SubId, stream := Stream},
    Session0 = #{s := S0, dscli := DSCli0, buffer := Buf0}
) ->
    S = terminate_stream(SubId, Stream, S0),
    Buf = emqx_persistent_session_ds_buffer:drop_stream({SubId, Stream}, Buf0),
    {DSCli, Session} = emqx_ds_client:detach_iterator(
        SubId,
        Stream,
        DSCli0,
        Session0#{buffer := Buf, s := S}
    ),
    Session#{dscli := DSCli}.

%%--------------------------------------------------------------------
%% to_map

-spec to_map(emqx_persistent_session_ds_state:t(), t()) -> map().
to_map(S, _SharedSubS) ->
    fold_shared_subs(
        fun(ShareTopicFilter, _, Acc) -> Acc#{ShareTopicFilter => lookup(ShareTopicFilter, S)} end,
        #{},
        S
    ).

%%--------------------------------------------------------------------
%% cold_get_subscription

-spec cold_get_subscription(emqx_persistent_session_ds:id(), emqx_types:share()) ->
    emqx_persistent_session_ds:subscription() | undefined.
cold_get_subscription(SessionId, ShareTopicFilter) ->
    maybe
        [Sub = #{current_state := SStateId}] ?=
            emqx_persistent_session_ds_state:cold_get_subscription(SessionId, ShareTopicFilter),
        [#{subopts := Subopts}] ?=
            emqx_persistent_session_ds_state:cold_get_subscription_state(SessionId, SStateId),
        Sub#{subopts => Subopts}
    else
        _ -> undefined
    end.

%%--------------------------------------------------------------------
%% Generic helpers
%%--------------------------------------------------------------------

-spec lookup(emqx_types:share(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds:subscription() | undefined.
lookup(ShareTopicFilter, S) ->
    maybe
        Sub = emqx_persistent_session_ds_state:get_subscription(ShareTopicFilter, S),
        #{current_state := SStateId} ?= Sub,
        #{subopts := SubOpts} ?=
            emqx_persistent_session_ds_state:get_subscription_state(SStateId, S),
        Sub#{subopts => SubOpts}
    else
        _ -> undefined
    end.

fold_shared_subs(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{} = ShareTopicFilter, Sub, Acc0) -> Fun(ShareTopicFilter, Sub, Acc0);
            (_, _Sub, Acc0) -> Acc0
        end,
        Acc,
        S
    ).

fold_shared_stream_states(Fun, Acc, S) ->
    ShareTopicFilters = fold_shared_subs(
        fun
            (#share{} = ShareTopicFilter, #{id := Id} = _Sub, Acc0) ->
                Acc0#{Id => ShareTopicFilter};
            (_, _, Acc0) ->
                Acc0
        end,
        #{},
        S
    ),
    emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, Stream}, SRS, Acc0) ->
            case ShareTopicFilters of
                #{SubId := ShareTopicFilter} ->
                    Fun(ShareTopicFilter, SubId, Stream, SRS, Acc0);
                _ ->
                    Acc0
            end
        end,
        Acc,
        S
    ).

is_use_finished(#srs{unsubscribed = Unsubscribed}) ->
    Unsubscribed.

is_stream_started(CommQos1, CommQos2, #srs{first_seqno_qos1 = Q1, first_seqno_qos2 = Q2}) ->
    (CommQos1 >= Q1) or (CommQos2 >= Q2).

is_stream_fully_acked(_, _, #srs{
    first_seqno_qos1 = Q1, last_seqno_qos1 = Q1, first_seqno_qos2 = Q2, last_seqno_qos2 = Q2
}) ->
    %% Streams where the last chunk doesn't contain any QoS1 and 2
    %% messages are considered fully acked:
    true;
is_stream_fully_acked(Comm1, Comm2, #srs{last_seqno_qos1 = S1, last_seqno_qos2 = S2}) ->
    (Comm1 >= S1) andalso (Comm2 >= S2).

-spec add_borrower(
    emqx_persistent_session_ds:id(),
    t(),
    emqx_persistent_session_ds:subscription_id(),
    emqx_types:share()
) -> t().
add_borrower(
    SessionId,
    #{borrowers := Borrowers0} = SharedSubS,
    SubscriptionId,
    ShareTopicFilter
) ->
    ?tp(debug, ds_shared_sub_agent_add_borrower, #{
        session_id => SessionId,
        share_topic_filter => ShareTopicFilter
    }),
    BorrowerId = make_borrower_id(SessionId, SubscriptionId),
    Borrower = emqx_ds_shared_sub_borrower:new(#{
        session_id => SessionId,
        share_topic_filter => ShareTopicFilter,
        id => BorrowerId,
        send_after => send_to_borrower_after(BorrowerId)
    }),
    BorrowerEntry = #borrower_entry{
        borrower_id = BorrowerId,
        topic_filter = ShareTopicFilter,
        borrower = Borrower
    },
    Borrowers = Borrowers0#{
        SubscriptionId => BorrowerEntry
    },
    SharedSubS#{borrowers := Borrowers}.

-spec disconnect_borrower(t(), emqx_persistent_session_ds:subscription_id(), progress()) -> t().
disconnect_borrower(SharedSubS, SubscriptionId, Progress) ->
    case SharedSubS of
        #{
            borrowers := #{
                SubscriptionId := #borrower_entry{
                    borrower = Borrower, borrower_id = BorrowerId
                }
            } = Borrowers
        } ->
            ok = destroy_borrower_id(BorrowerId),
            %% The whole session is shutting down, no need to handle the result.
            _ = emqx_ds_shared_sub_borrower:on_disconnect(Borrower, Progress),
            SharedSubS#{borrowers => maps:remove(SubscriptionId, Borrowers)};
        _ ->
            SharedSubS
    end.

-spec has_subscription(t(), emqx_persistent_session_ds:subscription_id()) -> boolean().
has_subscription(#{borrowers := Borrowers}, SubscriptionId) ->
    maps:is_key(SubscriptionId, Borrowers).

-spec with_borrower(
    emqx_persistent_session_ds:id(),
    t(),
    emqx_persistent_session_ds:subscription_id(),
    Fun
) ->
    {[stream_lease_event()], t()}
when
    Fun :: fun((BorrowerId, Borrower) -> {ok, Events, Borrower} | {stop, Events} | {reset, Events}),
    Events :: [emqx_ds_shared_sub_borrower:to_agent_events()],
    BorrowerId :: emqx_ds_shared_sub_proto:borrower_id(),
    Borrower :: emqx_ds_shared_sub_borrower:t().
with_borrower(SessionId, SharedSubS0, SubscriptionId, Fun) ->
    #{borrowers := Borrowers} = SharedSubS0,
    case Borrowers of
        #{SubscriptionId := Entry0} ->
            #borrower_entry{
                topic_filter = ShareTopicFilter,
                borrower = Borrower0,
                borrower_id = BorrowerId
            } = Entry0,
            case Fun(BorrowerId, Borrower0) of
                {ok, RawEvents, Borrower} ->
                    Entry = Entry0#borrower_entry{borrower = Borrower},
                    SharedSubS = SharedSubS0#{borrowers := Borrowers#{SubscriptionId := Entry}};
                {stop, RawEvents} ->
                    SharedSubS = SharedSubS0#{borrowers := maps:remove(SubscriptionId, Borrowers)};
                {reset, RawEvents} ->
                    ok = destroy_borrower_id(BorrowerId),
                    SharedSubS = add_borrower(
                        SessionId, SharedSubS0, SubscriptionId, ShareTopicFilter
                    )
            end,
            Events = enrich_events(RawEvents, SubscriptionId, ShareTopicFilter),
            {Events, SharedSubS};
        _ ->
            ?tp(warning, ds_shared_sub_borrower_not_found, #{
                session_id => SessionId,
                subscription_id => SubscriptionId
            }),
            {[], SharedSubS0}
    end.

-spec enrich_events(
    [emqx_ds_shared_sub_borrower:to_agent_events()],
    emqx_persistent_session_ds:subscription_id(),
    emqx_types:share()
) -> [stream_lease_event()].
enrich_events(Events, SubscriptionId, ShareTopicFilter) ->
    [
        Event#{subscription_id => SubscriptionId, share_topic_filter => ShareTopicFilter}
     || Event <- Events
    ].

make_borrower_id(Id, SubscriptionId) ->
    emqx_ds_shared_sub_proto:borrower_id(Id, SubscriptionId, alias()).

destroy_borrower_id(BorrowerId) ->
    Alias = emqx_ds_shared_sub_proto:borrower_pidref(BorrowerId),
    _ = unalias(Alias),
    ok.

send_to_borrower_after(BorrowerId) ->
    SubscriptionId = emqx_ds_shared_sub_proto:borrower_subscription_id(BorrowerId),
    fun(Time, Msg) ->
        send_after(
            Time,
            SubscriptionId,
            self(),
            #message_to_borrower{
                borrower_id = BorrowerId,
                message = Msg
            }
        )
    end.

-spec send_after(
    non_neg_integer(),
    emqx_persistent_session_ds:subscription_id(),
    pid() | reference(),
    term()
) -> reference().
send_after(Time, SubscriptionId, Dest, Msg) ->
    erlang:send_after(Time, Dest, ?session_message(?shared_sub_message(SubscriptionId, Msg))).
