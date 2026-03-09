%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov).

-moduledoc """
UNS Governance hook callbacks for publish authorization and payload checks.
""".

-export([hook/0, unhook/0, on_client_authorize/4, on_message_publish/1]).

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_unsgov.hrl").

-define(AUTHZ_HOOK, {?MODULE, on_client_authorize, []}).
-define(PUBLISH_HOOK, {?MODULE, on_message_publish, []}).
-define(PDICT_KEY, emqx_unsgov_model_id).

hook() ->
    ok = emqx_hooks:put('client.authorize', ?AUTHZ_HOOK, ?HP_AUTHZ + 1),
    ok = emqx_hooks:put('message.publish', ?PUBLISH_HOOK, ?HP_SCHEMA_VALIDATION + 1).

unhook() ->
    ok = emqx_hooks:del('client.authorize', ?AUTHZ_HOOK),
    emqx_hooks:del('message.publish', ?PUBLISH_HOOK).

on_client_authorize(_ClientInfo, #{action_type := publish}, Topic, Result) ->
    maybe
        ok ?= check_not_exempt(Topic),
        {allow, ModelId} ?= emqx_unsgov_store:validate_topic(Topic),
        ?LOG(debug, #{msg => "acl_allow", topic => Topic, model_id => ModelId}),
        erlang:put(?PDICT_KEY, ModelId),
        {ok, Result}
    else
        exempt ->
            ?LOG(debug, #{msg => "acl_exempt", topic => Topic}),
            emqx_unsgov_metrics:record_exempt(),
            {ok, Result};
        {deny, DeniedBy, Reason} ->
            ?LOG(debug, #{msg => "acl_deny", topic => Topic, model_id => DeniedBy, reason => Reason}),
            handle_authz_deny(DeniedBy, Topic, Reason, Result)
    end;
on_client_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

on_message_publish(Message) ->
    case erlang:erase(?PDICT_KEY) of
        undefined ->
            {ok, Message};
        ModelId ->
            handle_message_publish(ModelId, Message)
    end.

handle_message_publish(ModelId, Message) ->
    Topic = emqx_message:topic(Message),
    Payload = emqx_message:payload(Message),
    case emqx_unsgov_store:validate_message(ModelId, Topic, Payload) of
        {allow, _} ->
            ?LOG(debug, #{msg => "publish_allow", topic => Topic, model_id => ModelId}),
            emqx_unsgov_metrics:record_allowed(ModelId),
            {ok, Message};
        {deny, DenyModelId, Reason} ->
            ?LOG(debug, #{
                msg => "publish_deny", topic => Topic, model_id => DenyModelId, reason => Reason
            }),
            emqx_unsgov_metrics:record_drop(Topic, Reason, Reason),
            emqx_unsgov_metrics:record_drop_model(DenyModelId, Reason, Reason),
            {stop, emqx_message:set_headers(#{allow_publish => false}, Message)}
    end.

handle_authz_deny(ModelId, Topic, Reason, Result) ->
    case emqx_unsgov_config:on_mismatch() of
        deny ->
            emqx_unsgov_metrics:record_drop(Topic, Reason, Reason),
            emqx_unsgov_metrics:record_drop_model(ModelId, Reason, Reason),
            {stop, #{result => deny, from => emqx_unsgov, reason => Reason}};
        _ ->
            {ok, Result}
    end.

check_not_exempt(Topic) ->
    case emqx_unsgov_config:is_exempt_topic(Topic) of
        false -> ok;
        true -> exempt
    end.
