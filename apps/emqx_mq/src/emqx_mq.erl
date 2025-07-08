%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq).

-include_lib("emqx/include/emqx_hooks.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish/1,
    on_client_authorize/4,
    on_session_subscribed/3,
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_HIGHEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

on_message_publish(Msg) ->
    {ok, Msg}.

on_delivery_completed(_Msg, #{session_birth_time := _SessionBirthTime, clientid := _ClientId}) ->
    ok.

on_session_subscribed(
    #{clientid := _ClientId},
    <<"$test/", Topic/binary>>,
    _SubOpts
) ->
    erlang:send_after(1000, self(), {test_message, Topic}),
    ok;
on_session_subscribed(
    #{clientid := _ClientId},
    _Topic,
    _SubOpts
) ->
    ok.

on_client_authorize(
    #{clientid := _ClientId},
    #{action_type := _ActionType, qos := _QoS},
    _Topic,
    _DefaultResult
) ->
    ignore.

on_message_nack(
    #{clientid := _ClientId},
    Delivers
) ->
    {ok, Delivers}.

on_client_handle_info(
    #{clientid := ClientId},
    {test_message, Topic},
    #{deliver := Delivers} = Acc
) ->
    Msg = emqx_message:make(ClientId, Topic, <<"test message">>),
    Deliver = {deliver, Topic, Msg},
    erlang:send_after(1000, self(), {test_message, Topic}),
    {ok, Acc#{deliver => [Deliver | Delivers]}};
on_client_handle_info(
    #{clientid := _ClientId},
    _Info,
    Acc
) ->
    {ok, Acc}.
