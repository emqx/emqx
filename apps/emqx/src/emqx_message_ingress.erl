%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_message_ingress).

-include("emqx.hrl").
-include("emqx_access_control.hrl").

-export([
    ingress/2,
    ingress_and_authorize/2,
    authorize/2,
    finalize/2,
    finalize_and_publish/2
]).

-export_type([context/0]).

-type context() :: #{authz_ctx := emqx_authz_context:t()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec ingress(
    emqx_types:clientinfo(),
    emqx_types:message()
) -> {ok, emqx_types:message()} | {error, term()}.
ingress(ClientInfo, Msg) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    Ctx = #{authz_ctx => AuthzContext},
    run_hooks(Ctx, Msg).

-spec ingress_and_authorize(
    emqx_types:clientinfo(),
    emqx_types:message()
) -> {allow, emqx_types:message()} | deny | {error, term()}.
ingress_and_authorize(ClientInfo, Msg) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    Ctx = #{authz_ctx => AuthzContext},
    case run_hooks(Ctx, Msg) of
        {ok, NMsg} -> do_authorize(AuthzContext, NMsg);
        {error, _} = Error -> Error
    end.

-spec authorize(
    emqx_types:clientinfo(),
    emqx_types:message()
) -> {allow, emqx_types:message()} | deny.
authorize(ClientInfo, Msg) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    do_authorize(AuthzContext, Msg).

do_authorize(AuthzContext, Msg = #message{topic = Topic, qos = QoS}) ->
    Action = ?AUTHZ_PUBLISH(QoS, emqx_message:get_flag(retain, Msg)),
    case emqx_access_control:authorize(AuthzContext, Action, Topic) of
        allow -> {allow, Msg};
        deny -> deny
    end.

-spec finalize(emqx_types:clientinfo(), emqx_types:message()) -> emqx_types:message().
finalize(ClientInfo, Msg) ->
    emqx_mountpoint:mount(maps:get(mountpoint, ClientInfo, undefined), Msg).

-spec finalize_and_publish(
    emqx_types:clientinfo(), emqx_types:message()
) -> emqx_types:publish_result().
finalize_and_publish(ClientInfo, Msg) ->
    emqx_broker:publish(finalize(ClientInfo, Msg)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

run_hooks(Ctx, Msg) ->
    case emqx_hooks:run_fold_strict('message.ingress', [Ctx], Msg) of
        {ok, {error, Reason}} ->
            {error, Reason};
        {ok, NMsg} ->
            {ok, NMsg};
        {error, Reason} ->
            {error, {message_ingress_hook_failed, Reason}}
    end.
