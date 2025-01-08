%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module aggregates shared subscription handlers (borrowers)
%% for a session.

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_config.hrl").

-export([
    new/1,
    open/2,
    pre_subscribe/3,
    has_subscription/2,
    has_subscriptions/1,

    on_subscribe/4,
    on_unsubscribe/2,
    on_stream_progress/2,
    on_info/3,
    on_disconnect/2
]).

-export([
    send_to_borrower/2
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

-type subscription() :: emqx_persistent_session_ds_shared_subs_agent:subscription().
-type share_topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type subscription_id() :: emqx_persistent_session_ds_shared_subs_agent:subscription_id().

-type options() :: #{
    session_id := emqx_persistent_session_ds:id()
}.

-record(borrower_entry, {
    borrower_id :: emqx_ds_shared_sub_proto:borrower_id(),
    topic_filter :: share_topic_filter(),
    borrower :: emqx_ds_shared_sub_borrower:t()
}).

-type borrower_entry() :: #borrower_entry{}.

-type t() :: #{
    borrowers := #{
        subscription_id() => borrower_entry()
    },
    session_id := emqx_persistent_session_ds:id()
}.

-record(message_to_borrower, {
    borrower_id :: emqx_ds_shared_sub_proto:borrower_id(),
    message :: term()
}).

-export_type([
    t/0,
    options/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> t().
new(Opts) ->
    init_state(Opts).

-spec open([{share_topic_filter(), subscription()}], options()) -> t().
open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{id := SubscriptionId}}, State) ->
            ?tp(debug, ds_shared_sub_agent_open, #{
                subscription_id => SubscriptionId,
                topic_filter => ShareTopicFilter
            }),
            add_borrower(State, SubscriptionId, ShareTopicFilter)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

-spec pre_subscribe(t(), share_topic_filter(), emqx_types:subopts()) ->
    ok | {error, emqx_types:reason_code()}.
pre_subscribe(_State, #share{group = Group, topic = Topic}, _SubOpts) ->
    case ?dq_config(enable) of
        true ->
            %% TODO: Weird to have side effects in function with this name.
            TS = emqx_message:timestamp_now(),
            case emqx_ds_shared_sub_queue:declare(Group, Topic, TS, _StartTime = TS) of
                {ok, _} ->
                    ok;
                exists ->
                    ok;
                {error, Class, Reason} ->
                    ?tp(warning, ds_shared_sub_agent_queue_declare_failed, #{
                        group => Group,
                        topic => Topic,
                        class => Class,
                        reason => Reason
                    }),
                    {error, ?RC_UNSPECIFIED_ERROR}
            end;
        false ->
            {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}
    end.

-spec has_subscription(t(), subscription_id()) -> boolean().
has_subscription(#{borrowers := Borrowers}, SubscriptionId) ->
    maps:is_key(SubscriptionId, Borrowers).

-spec has_subscriptions(t()) -> boolean().
has_subscriptions(#{borrowers := Borrowers}) ->
    maps:size(Borrowers) > 0.

-spec on_subscribe(t(), subscription_id(), share_topic_filter(), emqx_types:subopts()) -> t().
on_subscribe(State0, SubscriptionId, ShareTopicFilter, _SubOpts) ->
    ?tp(debug, ds_shared_sub_agent_on_subscribe, #{
        share_topic_filter => ShareTopicFilter
    }),
    add_borrower(State0, SubscriptionId, ShareTopicFilter).

-spec on_unsubscribe(t(), subscription_id()) -> t().
on_unsubscribe(State0, SubscriptionId) ->
    {[], State} = with_borrower(State0, SubscriptionId, fun(_BorrowerId, Borrower) ->
        emqx_ds_shared_sub_borrower:on_unsubscribe(Borrower)
    end),
    State.

-spec on_stream_progress(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_stream_progress(State, StreamProgresses) when map_size(StreamProgresses) == 0 ->
    State;
on_stream_progress(State, StreamProgresses) ->
    maps:fold(
        fun(SubscriptionId, Progresses, StateAcc0) ->
            {[], StateAcc1} = with_borrower(StateAcc0, SubscriptionId, fun(
                _BorrowerId, Borrower
            ) ->
                emqx_ds_shared_sub_borrower:on_stream_progress(Borrower, Progresses)
            end),
            StateAcc1
        end,
        State,
        StreamProgresses
    ).

-spec on_disconnect(t(), #{
    subscription_id() => [emqx_persistent_session_ds_shared_subs:agent_stream_progress()]
}) -> t().
on_disconnect(#{borrowers := Borrowers} = State, StreamProgresses) ->
    ok = lists:foreach(
        fun(SubscriptionId) ->
            Progress = maps:get(SubscriptionId, StreamProgresses, []),
            disconnect_borrower(State, SubscriptionId, Progress)
        end,
        maps:keys(Borrowers)
    ),
    State#{borrowers => #{}}.

-spec on_info(t(), subscription_id(), term()) ->
    {[emqx_persistent_session_ds_shared_subs_agent:event()], t()}.
on_info(State, SubscriptionId, #message_to_borrower{
    borrower_id = BorrowerId, message = Message
}) ->
    ?tp(debug, ds_shared_sub_message_to_borrower, #{
        subscription_id => SubscriptionId,
        message => Message
    }),
    with_borrower(State, SubscriptionId, fun(KnownBorrowerId, Borrower) ->
        %% We may have recreated invalidated Borrower, resulting in a new BorrowerId.
        %% Ignore the messages to the old Borrower.
        case KnownBorrowerId of
            BorrowerId ->
                emqx_ds_shared_sub_borrower:on_info(Borrower, Message);
            _ ->
                {ok, [], Borrower}
        end
    end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_state(Opts) ->
    SessionId = maps:get(session_id, Opts),
    #{
        session_id => SessionId,
        borrowers => #{}
    }.

disconnect_borrower(State, SubscriptionId, Progress) ->
    case State of
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
            State#{borrowers => maps:remove(SubscriptionId, Borrowers)};
        _ ->
            State
    end.

add_borrower(
    #{session_id := SessionId, borrowers := Borrowers0} = State0,
    SubscriptionId,
    ShareTopicFilter
) ->
    ?tp(debug, ds_shared_sub_agent_add_borrower, #{
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
    Borrowers1 = Borrowers0#{
        SubscriptionId => BorrowerEntry
    },
    State1 = State0#{borrowers => Borrowers1},
    State1.

make_borrower_id(Id, SubscriptionId) ->
    emqx_ds_shared_sub_proto:borrower_id(Id, SubscriptionId, alias()).

destroy_borrower_id(BorrowerId) ->
    Alias = emqx_ds_shared_sub_proto:borrower_pidref(BorrowerId),
    _ = unalias(Alias),
    ok.

send_to_borrower_after(BorrowerId) ->
    SubscriptionId = emqx_ds_shared_sub_proto:borrower_subscription_id(BorrowerId),
    fun(Time, Msg) ->
        emqx_persistent_session_ds_shared_subs_agent:send_after(
            Time,
            SubscriptionId,
            self(),
            #message_to_borrower{
                borrower_id = BorrowerId,
                message = Msg
            }
        )
    end.

send_to_borrower(BorrowerId, Msg) ->
    SubscriptionId = emqx_ds_shared_sub_proto:borrower_subscription_id(BorrowerId),
    emqx_persistent_session_ds_shared_subs_agent:send(
        emqx_ds_shared_sub_proto:borrower_pidref(BorrowerId),
        SubscriptionId,
        #message_to_borrower{
            borrower_id = BorrowerId,
            message = Msg
        }
    ).

with_borrower(State0, SubscriptionId, Fun) ->
    case State0 of
        #{
            borrowers := #{
                SubscriptionId := #borrower_entry{
                    topic_filter = ShareTopicFilter,
                    borrower = Borrower0,
                    borrower_id = BorrowerId
                } = Entry0
            } = Borrowers
        } ->
            {Events0, State1} =
                case Fun(BorrowerId, Borrower0) of
                    {ok, Events, Borrower1} ->
                        Entry1 = Entry0#borrower_entry{
                            borrower = Borrower1
                        },
                        {Events, State0#{borrowers => Borrowers#{SubscriptionId => Entry1}}};
                    {stop, Events} ->
                        ok = destroy_borrower_id(BorrowerId),
                        {Events, State0#{borrowers => maps:remove(SubscriptionId, Borrowers)}};
                    {reset, Events} ->
                        ok = destroy_borrower_id(BorrowerId),
                        {Events, add_borrower(State0, SubscriptionId, ShareTopicFilter)}
                end,
            Events1 = enrich_events(Events0, SubscriptionId, ShareTopicFilter),
            {Events1, State1};
        #{session_id := SessionId} ->
            ?tp(warning, ds_shared_sub_agent_borrower_not_found, #{
                session_id => SessionId,
                subscription_id => SubscriptionId
            }),
            {[], State0}
    end.

enrich_events(Events, SubscriptionId, ShareTopicFilter) ->
    [
        Event#{subscription_id => SubscriptionId, share_topic_filter => ShareTopicFilter}
     || Event <- Events
    ].
