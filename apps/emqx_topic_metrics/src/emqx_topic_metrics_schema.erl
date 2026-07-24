%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_schema).
-moduledoc """
Field validators for the v2 named-collection topic-metrics feature.

v2 deliberately does NOT have a HOCON root — the cluster-wide set of
collections is stored in a `mria` `disc_copies` table managed by
`emqx_topic_metrics_registry`. This module exists only to give the
REST handler and the registry one shared place for input validation.
""".

-include("emqx_topic_metrics.hrl").

-export([validate_name/1, validate_topic_filter/1]).

-spec validate_name(binary()) -> ok | {error, term()}.
validate_name(Name) when is_binary(Name) ->
    case re:run(Name, ?NAME_REGEX, [{capture, none}]) of
        match -> ok;
        nomatch -> {error, #{cause => bad_name, name => Name}}
    end;
validate_name(_) ->
    {error, #{cause => bad_name}}.

-spec validate_topic_filter(binary()) -> ok | {error, term()}.
validate_topic_filter(Filter) when is_binary(Filter) ->
    %% emqx_topic:validate/1 returns `true' on success and throws on
    %% failure (including the empty-binary case). We don't use a
    %% `try ... of true -> ok' form because dialyzer infers the
    %% return type as `true' and warns about an "impossible" `false'
    %% fallthrough; the unconditional `catch' covers every error
    %% path.
    try
        true = emqx_topic:validate({filter, Filter}),
        case is_shared_filter(Filter) of
            false -> ok;
            true -> {error, #{cause => shared_topic_filter_not_allowed, filter => Filter}}
        end
    catch
        _:Reason -> {error, #{cause => bad_topic_filter, reason => Reason}}
    end;
validate_topic_filter(_) ->
    {error, #{cause => bad_topic_filter}}.

%% Shared-subscription filters (`$share/<group>/<topic>' and the
%% `$queue/<topic>' alias) are valid *subscription* filters but never
%% match a published `#message.topic' (the share prefix lives on the
%% subscription, not the message). A collection stored with such a
%% filter would silently count nothing, so we reject it at creation.
%% Deliveries to shared subscribers are still counted under the plain
%% topic filter via the `message.delivered' hook.
is_shared_filter(<<"$share/", _/binary>>) -> true;
is_shared_filter(<<"$queue/", _/binary>>) -> true;
is_shared_filter(_) -> false.
