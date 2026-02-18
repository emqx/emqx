%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate).

-export([hook/0, unhook/0, on_client_authorize/4, on_message_publish/1]).

-include_lib("emqx_plugin_helper/include/emqx_hooks.hrl").

-define(AUTHZ_HOOK, {?MODULE, on_client_authorize, []}).
-define(PUBLISH_HOOK, {?MODULE, on_message_publish, []}).

hook() ->
    ok = emqx_hooks:put('client.authorize', ?AUTHZ_HOOK, ?HP_AUTHZ + 1),
    ok = emqx_hooks:put('message.publish', ?PUBLISH_HOOK, ?HP_SCHEMA_VALIDATION + 1).

unhook() ->
    ok = emqx_hooks:del('client.authorize', ?AUTHZ_HOOK),
    emqx_hooks:del('message.publish', ?PUBLISH_HOOK).

on_client_authorize(_ClientInfo, #{action_type := publish}, Topic, Result) ->
    case emqx_uns_gate_config:enabled() of
        false ->
            {ok, Result};
        true ->
            TopicBin = emqx_utils_conv:bin(Topic),
            case emqx_uns_gate_config:is_exempt_topic(TopicBin) of
                true ->
                    emqx_uns_gate_metrics:record_exempt(),
                    {ok, Result};
                false ->
                    case emqx_uns_gate_store:validate_topic(TopicBin) of
                        allow ->
                            {ok, Result};
                        {deny, Reason} ->
                            case emqx_uns_gate_config:on_mismatch() of
                                ignore ->
                                    {ok, Result};
                                deny ->
                                    emqx_uns_gate_metrics:record_drop(TopicBin, Reason, Reason),
                                    {stop, #{
                                        result => deny,
                                        from => emqx_uns_gate,
                                        reason => Reason
                                    }}
                            end
                    end
            end
    end;
on_client_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

on_message_publish(Message) ->
    Topic = emqx_message:topic(Message),
    Payload = emqx_message:payload(Message),
    case emqx_uns_gate_config:enabled() of
        false ->
            {ok, Message};
        true ->
            case emqx_uns_gate_config:is_exempt_topic(Topic) of
                true ->
                    {ok, Message};
                false ->
                    case emqx_uns_gate_store:validate_message(Topic, Payload) of
                        allow ->
                            emqx_uns_gate_metrics:record_allowed(),
                            {ok, Message};
                        {deny, Reason} ->
                            emqx_uns_gate_metrics:record_drop(Topic, Reason, Reason),
                            {stop, emqx_message:set_headers(#{allow_publish => false}, Message)}
                    end
            end
    end.
