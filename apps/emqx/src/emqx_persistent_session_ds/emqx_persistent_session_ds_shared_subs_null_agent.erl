%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_shared_subs_null_agent).

-include("emqx_mqtt.hrl").

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

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

new(_Opts) ->
    undefined.

open(_Topics, _Opts) ->
    undefined.

pre_subscribe(_Agent, _TopicFilter, _SubOpts) ->
    {error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}.

has_subscription(_Agent, _SubscriptionId) ->
    false.

has_subscriptions(_Agent) ->
    false.

on_subscribe(Agent, _SubscriptionId, _TopicFilter, _SubOpts) ->
    Agent.

on_unsubscribe(Agent, _SubscriptionId) ->
    Agent.

on_disconnect(Agent, _) ->
    Agent.

on_stream_progress(Agent, _StreamProgress) ->
    Agent.

on_info(Agent, _SubscriptionId, _Info) ->
    {[], Agent}.
