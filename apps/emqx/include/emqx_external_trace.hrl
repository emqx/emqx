%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-ifndef(EMQX_EXT_TRACE_HRL).
-define(EMQX_EXT_TRACE_HRL, true).

%% --------------------------------------------------------------------
%% Macros

-define(EXT_TRACE_START, '$ext_trace_start').
-define(EXT_TRACE_STOP, '$ext_trace_stop').

-define(EXT_TRACE__RULE_INTERNAL_CLIENTID, '$emqx_rule_internal_clientid').
-define(EXT_TRACE__ACTION_INTERNAL_CLIENTID, '$emqx_action_internal_clientid').

-define(EMQX_EXTERNAL_MODULE, emqx_external_trace).
-define(PROVIDER, {?EMQX_EXTERNAL_MODULE, trace_provider}).

-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-if(?EMQX_RELEASE_EDITION == ee).

-define(get_provider(), persistent_term:get(?PROVIDER, undefined)).

-define(no_provider(Any), Any).
-define(no_provider(ProcessFun, ProcessFunArgs), erlang:apply(ProcessFun, ProcessFunArgs)).

-define(apply_provider(M, F, A), erlang:apply(M, F, A)).

-define(with_provider_call(
    Callback,
    CallbackArgs,
    IfNoProvider
),
    fun() ->
        case ?get_provider() of
            undefined ->
                ?no_provider(IfNoProvider);
            Provider ->
                ?apply_provider(Provider, Callback, CallbackArgs)
        end
    end()
).

-define(with_provider_action(
    Callback,
    Attrs,
    Action,
    Any
),
    fun() ->
        case ?get_provider() of
            undefined ->
                ?no_provider(Any);
            Provider ->
                ?apply_provider(Provider, Callback, [Attrs, Action, Any])
        end
    end()
).

-define(with_provider_apply_process_fun(
    Callback,
    Attrs,
    ProcessFun,
    ProcessFunArgs
),
    fun() ->
        case ?get_provider() of
            undefined ->
                ?no_provider(ProcessFun, ProcessFunArgs);
            Provider ->
                ?apply_provider(Provider, Callback, [Attrs, ProcessFun, ProcessFunArgs])
        end
    end()
).

-define(EXT_TRACE_ADD_ATTRS(Attrs),
    ?with_provider_call(add_span_attrs, [Attrs], ok)
).

-define(EXT_TRACE_ADD_ATTRS(Attrs, Ctx),
    ?with_provider_call(add_span_attrs, [Attrs, Ctx], ok)
).

-define(EXT_TRACE_SET_STATUS_OK(),
    ?with_provider_call(set_status_ok, [], ok)
).

-define(EXT_TRACE_SET_STATUS_ERROR(),
    ?with_provider_call(set_status_error, [], ok)
).

-define(EXT_TRACE_SET_STATUS_ERROR(Msg),
    ?with_provider_call(set_status_error, [Msg], ok)
).

-define(EXT_TRACE_ATTR(_Expr_), begin
    _Expr_
end).

-define(EXT_TRACE_CLIENT_CONNECT(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_connect, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_DISCONNECT(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_disconnect, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_SUBSCRIBE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_subscribe, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_UNSUBSCRIBE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_unsubscribe, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_authn, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN_BACKEND(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_authn_backend, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_authz, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ_BACKEND(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_authz_backend, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_DISCONNECT(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(broker_disconnect, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_SUBSCRIBE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(broker_subscribe, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_UNSUBSCRIBE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(broker_unsubscribe, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBLISH(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_publish, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBACK(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_puback, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREC(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_pubrec, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREL(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_pubrel, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBCOMP(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(client_pubcomp, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_MSG_ROUTE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(msg_route, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_MSG_FORWARD(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(msg_forward, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_MSG_HANDLE_FORWARD(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(msg_handle_forward, Attrs, Fun, FunArgs)
).

%% Start broker.publish trace, ends by `?EXT_TRACE_OUTGOING_STOP`
-define(EXT_TRACE_BROKER_PUBLISH(Attrs, Delivers),
    ?with_provider_action(broker_publish, Attrs, ?EXT_TRACE_START, Delivers)
).

-define(EXT_TRACE_OUTGOING_START(Attrs, Packet),
    ?with_provider_action(outgoing, Attrs, ?EXT_TRACE_START, Packet)
).

-define(EXT_TRACE_OUTGOING_STOP(Attrs, Packets),
    ?with_provider_action(outgoing, Attrs, ?EXT_TRACE_STOP, Packets)
).

-define(EXT_TRACE_APPLY_RULE(Attrs, Fun, FunArgs),
    ?with_provider_apply_process_fun(apply_rule, Attrs, Fun, FunArgs)
).

-define(EXT_TRACE_HANDLE_ACTION_START(Attrs, Envs),
    ?with_provider_action(handle_action, Attrs, ?EXT_TRACE_START, Envs)
).

-define(EXT_TRACE_HANDLE_ACTION_STOP(Attrs, RequestContext),
    ?with_provider_action(handle_action, Attrs, ?EXT_TRACE_STOP, RequestContext)
).

-define(EXT_TRACE_WITH_ACTION_METADATA(Envs, RequestContext),
    ?with_provider_call(with_action_metadata, [Envs, RequestContext], RequestContext)
).

-else.

-define(EXT_TRACE_ADD_ATTRS(_Attrs), ok).
-define(EXT_TRACE_ADD_ATTRS(_Attrs, _Ctx), ok).
-define(EXT_TRACE_SET_STATUS_OK(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(_), ok).

-define(EXT_TRACE_ATTR(_Expr_),
    ok
).

-define(EXT_TRACE_CLIENT_CONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_DISCONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_SUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_UNSUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHN_BACKEND(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_AUTHZ_BACKEND(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_DISCONNECT(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_SUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_UNSUBSCRIBE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBLISH(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBACK(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREC(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBREL(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_CLIENT_PUBCOMP(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_ROUTE(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_FORWARD(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_MSG_HANDLE_FORWARD(_Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_BROKER_PUBLISH(_Attrs, Delivers),
    Delivers
).

-define(EXT_TRACE_OUTGOING_START(_Attrs, Packet),
    Packet
).

-define(EXT_TRACE_OUTGOING_STOP(_Attrs, Packets),
    ok
).

-define(EXT_TRACE_APPLY_RULE(Attrs, Fun, FunArgs),
    erlang:apply(Fun, FunArgs)
).

-define(EXT_TRACE_HANDLE_ACTION_START(_Attrs, Envs),
    Envs
).

-define(EXT_TRACE_HANDLE_ACTION_STOP(_Attrs, Envs),
    ok
).

-define(EXT_TRACE_WITH_ACTION_METADATA(_Envs, RequestContext),
    RequestContext
).

%% EMQX_RELEASE_EDITION check end
-endif.

%% EMQX_EXT_TRACE_HRL check end
-endif.
