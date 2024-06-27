%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_shared_subs_agent).

-include("shared_subs_agent.hrl").
-include("emqx_session.hrl").
-include("session_internals.hrl").

-type session_id() :: emqx_persistent_session_ds:id().

-type subscription() :: #{
    start_time := emqx_ds:time()
}.

-type t() :: term().
-type topic_filter() :: emqx_persistent_session_ds:share_topic_filter().

-type opts() :: #{
    session_id := session_id()
}.

%% TODO
%% This records go through network, we better shrink them
%% * use integer keys
%% * somehow avoid passing stream and topic_filter — they both are part of the iterator
-type stream_lease() :: #{
    type => lease,
    %% Used as "external" subscription_id
    topic_filter := topic_filter(),
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator()
}.

-type stream_revoke() :: #{
    type => revoke,
    topic_filter := topic_filter(),
    stream := emqx_ds:stream()
}.

-type stream_lease_event() :: stream_lease() | stream_revoke().

-type stream_progress() :: #{
    topic_filter := topic_filter(),
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator(),
    use_finished := boolean()
}.

-export_type([
    t/0,
    subscription/0,
    session_id/0,
    stream_lease/0,
    opts/0
]).

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/2,
    on_stream_progress/2,
    on_info/2,
    on_disconnect/2,

    renew_streams/1
]).

-export([
    send/2,
    send_after/3
]).

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

-callback new(opts()) -> t().
-callback open([{topic_filter(), subscription()}], opts()) -> t().
-callback on_subscribe(t(), topic_filter(), emqx_types:subopts()) ->
    {ok, t()} | {error, term()}.
-callback on_unsubscribe(t(), topic_filter()) -> t().
-callback on_disconnect(t(), [stream_progress()]) -> t().
-callback renew_streams(t()) -> {[stream_lease_event()], t()}.
-callback on_stream_progress(t(), [stream_progress()]) -> t().
-callback on_info(t(), term()) -> t().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(opts()) -> t().
new(Opts) ->
    ?shared_subs_agent:new(Opts).

-spec open([{topic_filter(), subscription()}], opts()) -> t().
open(Topics, Opts) ->
    ?shared_subs_agent:open(Topics, Opts).

-spec on_subscribe(t(), topic_filter(), emqx_types:subopts()) ->
    {ok, t()} | {error, emqx_types:reason_code()}.
on_subscribe(Agent, TopicFilter, SubOpts) ->
    ?shared_subs_agent:on_subscribe(Agent, TopicFilter, SubOpts).

-spec on_unsubscribe(t(), topic_filter()) -> t().
on_unsubscribe(Agent, TopicFilter) ->
    ?shared_subs_agent:on_unsubscribe(Agent, TopicFilter).

-spec on_disconnect(t(), [stream_progress()]) -> t().
on_disconnect(Agent, StreamProgresses) ->
    ?shared_subs_agent:on_disconnect(Agent, StreamProgresses).

-spec renew_streams(t()) -> {[stream_lease_event()], t()}.
renew_streams(Agent) ->
    ?shared_subs_agent:renew_streams(Agent).

-spec on_stream_progress(t(), [stream_progress()]) -> t().
on_stream_progress(Agent, StreamProgress) ->
    ?shared_subs_agent:on_stream_progress(Agent, StreamProgress).

-spec on_info(t(), term()) -> t().
on_info(Agent, Info) ->
    ?shared_subs_agent:on_info(Agent, Info).

-spec send(pid(), term()) -> term().
send(Dest, Msg) ->
    erlang:send(Dest, ?session_message(?shared_sub_message(Msg))).

-spec send_after(non_neg_integer(), pid(), term()) -> reference().
send_after(Time, Dest, Msg) ->
    erlang:send_after(Time, Dest, ?session_message(?shared_sub_message(Msg))).
