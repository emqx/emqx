%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_hooks).
-moduledoc """
Hook handlers for v2 topic-metrics. Reads ETS / atomics directly —
no gen_server call in the hot path.
""".

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("emqx_topic_metrics.hrl").

-export([
    enable/0,
    disable/0
]).

-export([
    on_message_publish/1,
    on_message_delivered/2,
    on_message_dropped/3
]).

%% Indexes into the per-collection counters_ref. Must match the
%% order of ?METRICS in emqx_topic_metrics.hrl.
-define(IDX_IN, 1).
-define(IDX_OUT, 2).
-define(IDX_DROPPED, 3).
-define(IDX_BYTES_IN, 4).
-define(IDX_BYTES_OUT, 5).

%%--------------------------------------------------------------------
%% Hook lifecycle
%%--------------------------------------------------------------------

-spec enable() -> ok.
enable() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_TOPIC_METRICS),
    emqx_hooks:put('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_TOPIC_METRICS),
    emqx_hooks:put('message.dropped', {?MODULE, on_message_dropped, []}, ?HP_TOPIC_METRICS),
    ok.

-spec disable() -> ok.
disable() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('message.dropped', {?MODULE, on_message_dropped}),
    ok.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_message_publish(#message{topic = Topic} = Msg) ->
    inc_matches(Topic, Msg, [{?IDX_IN, 1}, {?IDX_BYTES_IN, msg_size(Msg)}]),
    ok.

on_message_delivered(_ClientInfo, #message{topic = Topic} = Msg) ->
    %% Publisher namespace is the same for the delivered hook (it's
    %% the publisher of the original message, not the subscriber).
    %% This keeps "messages.out" symmetric with "messages.in" — both
    %% count namespace-owned activity from the publisher's side.
    inc_matches(Topic, Msg, [{?IDX_OUT, 1}, {?IDX_BYTES_OUT, msg_size(Msg)}]),
    ok.

on_message_dropped(#message{topic = Topic} = Msg, _ByWhom, _Reason) ->
    inc_matches(Topic, Msg, [{?IDX_DROPPED, 1}]),
    ok.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

%% The publisher's namespace is read up-front and passed into the
%% registry, which only returns records whose owner-namespace is
%% either global or matches the publisher. That keeps the post-filter
%% out of the hot path and avoids pulling counter refs we'd just drop.
%%
%% `Updates' is a list of `{Idx, Delta}' pairs applied to every
%% matched collection's counter_ref. Two pairs in the publish/deliver
%% paths (message count + bytes), one in the dropped path.
inc_matches(Topic, Msg, Updates) ->
    PublisherNs = emqx_broker:get_namespace(Msg),
    case emqx_topic_metrics_registry:matches_with_record(Topic, PublisherNs) of
        [] -> ok;
        Matches -> inc_each(Matches, Updates)
    end.

inc_each(Matches, Updates) ->
    lists:foreach(
        fun({_Name, CRef}) ->
            lists:foreach(
                fun({Idx, Delta}) -> counters:add(CRef, Idx, Delta) end,
                Updates
            )
        end,
        Matches
    ).

%% Approximate message size on the wire: just the topic + the
%% payload. We deliberately do NOT walk headers / MQTT properties
%% / user-props — that walk is variable-cost (per message) on the
%% hot path, and the topic+payload pair is what operators usually
%% mean by "message size" for capacity planning. If a customer ever
%% needs the exact wire-byte count, that's a different metric and
%% can be added without changing this one.
msg_size(#message{topic = Topic, payload = Payload}) ->
    iolist_size(Topic) + iolist_size(Payload).
