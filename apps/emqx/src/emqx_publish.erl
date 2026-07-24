%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_publish).

-export([run_pre_authz_hook/4]).

-export_type([
    pre_authz_context/0,
    pre_authz_overrides/0,
    pre_authz_result/0,
    prepared/0
]).

-type pre_authz_context() :: #{
    authz_context := emqx_authz_context:t(),
    action := emqx_types:pubsub(),
    topic := emqx_types:topic()
}.
-type pre_authz_overrides() :: #{
    topic => emqx_types:topic(),
    retain => boolean(),
    headers => map()
}.
-type pre_authz_result() :: {ok, pre_authz_overrides()} | {error, term()}.
-type prepared() :: #{
    action := emqx_types:pubsub(),
    topic := emqx_types:topic(),
    headers := map()
}.

-define(OVERRIDE_KEYS, [headers, retain, topic]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec run_pre_authz_hook(
    PacketOrFrame :: term(),
    emqx_authz_context:t(),
    emqx_types:pubsub(),
    emqx_types:topic()
) -> {ok, prepared()} | {error, term()}.
run_pre_authz_hook(PacketOrFrame, AuthzContext, Action, Topic) ->
    Context = #{authz_context => AuthzContext, action => Action, topic => Topic},
    case emqx_hooks:run_fold_strict(
        'client.publish_pre_authz', [PacketOrFrame, Context], {ok, #{}}
    ) of
        {ok, {ok, Overrides}} ->
            Prepared = #{topic => Topic, action => Action, headers => #{}},
            apply_overrides(?OVERRIDE_KEYS, Overrides, Prepared);
        {ok, {error, Reason}} ->
            {error, Reason};
        {ok, InvalidResult} ->
            {error, {invalid_publish_pre_authz_result, InvalidResult}};
        {error, Reason} ->
            {error, {publish_pre_authz_hook_failed, Reason}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

apply_overrides([], _Overrides, Prepared) ->
    {ok, Prepared};
apply_overrides([topic | Rest], #{topic := TopicOverride} = Overrides, Prepared) ->
    apply_overrides(Rest, Overrides, Prepared#{topic => TopicOverride});
apply_overrides([headers | Rest], #{headers := HeadersOverride} = Overrides, Prepared) ->
    apply_overrides(Rest, Overrides, Prepared#{headers => HeadersOverride});
apply_overrides([retain | Rest], #{retain := RetainOverride} = Overrides, #{action := Action} = Prepared) ->
    apply_overrides(Rest, Overrides, Prepared#{action := Action#{retain => RetainOverride}});
apply_overrides([_Key | Rest], Overrides, Prepared) ->
    apply_overrides(Rest, Overrides, Prepared).
