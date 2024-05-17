%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_shared_subs_agent).

-type session_id() :: emqx_persistent_session_ds:id().

-type subscription() :: #{
    start_time := emqx_ds:time()
}.

-type t() :: term().
-type topic_filter() :: emqx_persistent_session_ds:share_topic_filter().

-type opts() :: #{
    session_id := session_id(),
    send_funs := #{
        send := fun((pid(), term()) -> term()),
        send_after := fun((non_neg_integer(), pid(), term()) -> reference())
    }
}.

%% TODO
%% This records goe through network, we better shrink them
%% * use integer keys
%% * somehow avoid passing stream and topic_filter — they both are part of the iterator
-type stream_lease() :: #{
    %% Used as "external" subscription_id
    topic_filter := topic_filter(),
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator()
}.

-type stream_revoke() :: #{
    topic_filter := topic_filter(),
    stream := emqx_ds:stream()
}.

-type stream_progress() :: #{
    topic_filter := topic_filter(),
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator()
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
    on_session_drop/1,
    on_stream_progress/2,
    on_info/2,

    renew_streams/1
]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(opts()) -> t().
new(_Opts) ->
    undefined.

-spec open([{topic_filter(), subscription()}], opts()) -> t().
open(_Topics, _Opts) ->
    undefined.

-spec on_subscribe(t(), topic_filter(), emqx_types:subopts()) ->
    {ok, t()} | {error, term()}.
on_subscribe(Agent, _TopicFilter, _SubOpts) ->
    % {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}
    {ok, Agent}.

-spec on_unsubscribe(t(), topic_filter()) -> t().
on_unsubscribe(Agent, _TopicFilter) ->
    Agent.

-spec on_session_drop(t()) -> t().
on_session_drop(Agent) ->
    Agent.

-spec renew_streams(t()) -> {[stream_lease()], [stream_revoke()], t()}.
renew_streams(Agent) ->
    {[], [], Agent}.

-spec on_stream_progress(t(), [stream_progress()]) -> t().
on_stream_progress(Agent, _StreamProgress) ->
    Agent.

-spec on_info(t(), term()) -> t().
on_info(Agent, _Info) ->
    Agent.
