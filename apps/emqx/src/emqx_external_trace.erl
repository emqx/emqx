%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_external_trace).

-include("emqx_external_trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%% Legacy
-type channel_info() :: #{atom() => _}.
-export_type([channel_info/0]).

%% --------------------------------------------------------------------
%% Trace in Rich mode callbacks

%% Client Connect/Disconnect
-callback client_connect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_disconnect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_subscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_unsubscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_authn(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_authz(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback broker_disconnect(Any, InitAttrs, fun((Any) -> Res)) -> Res when
    Any :: any(),
    InitAttrs :: attrs(),
    Res :: any().

-callback broker_subscribe(Any, InitAttrs, fun((Any) -> Res)) -> Res when
    Any :: any(),
    InitAttrs :: attrs(),
    Res :: any().

-callback broker_unsubscribe(Any, InitAttrs, fun((Any) -> Res)) -> Res when
    Any :: any(),
    InitAttrs :: attrs(),
    Res :: any().

%% Message Processing Spans
%% PUBLISH(form Publisher) -> ROUTE -> FORWARD(optional) -> DELIVER(to Subscribers)
-callback client_publish(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_puback(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_pubrec(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_pubrel(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback client_pubcomp(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback msg_route(Delivery, InitAttrs, fun((Delivery) -> Res)) -> Res when
    InitAttrs :: attrs(),
    Delivery :: emqx_types:delivery(),
    Res :: term().

%% @doc Trace message forwarding
%% The span `message.forward` always starts in the publisher process and ends in the subscriber process.
%% They are logically two unrelated processes. So the SpanCtx always need to be propagated.
-callback msg_forward(Delivery, InitAttrs, fun((Delivery) -> Res)) -> Res when
    InitAttrs :: attrs(),
    Delivery :: emqx_types:delivery(),
    Res :: term().

-callback msg_handle_forward(Delivery, InitAttrs, fun((Delivery) -> Res)) -> Res when
    InitAttrs :: attrs(),
    Delivery :: emqx_types:delivery(),
    Res :: term().

-callback broker_publish(list(Deliver), Attrs) -> list(Deliver) when
    Deliver :: emqx_types:deliver(),
    Attrs :: attrs().

-callback outgoing(TraceAction, Packet, Attrs) -> Res when
    TraceAction :: ?EXT_TRACE_START | ?EXT_TRACE_STOP,
    Packet :: emqx_types:packet(),
    Attrs :: attrs(),
    Res :: term().

%% --------------------------------------------------------------------
%% Span enrichments APIs

-callback add_span_attrs(Attrs) -> ok when
    Attrs :: attrs().

-callback add_span_attrs(Attrs, Ctx) -> ok when
    Attrs :: attrs(),
    Ctx :: map() | undefined.

-callback set_status_ok() -> ok.

-callback set_status_error() -> ok.

-callback set_status_error(unicode:unicode_binary()) -> ok.

-optional_callbacks(
    [
        add_span_attrs/1,
        add_span_attrs/2,
        set_status_ok/0,
        set_status_error/0,
        set_status_error/1,
        client_authn/3,
        client_authz/3
    ]
).

-export([
    provider/0,
    register_provider/1,
    unregister_provider/1
]).

%%--------------------------------------------------------------------
%% provider API
%%--------------------------------------------------------------------

-spec register_provider(module()) -> ok | {error, term()}.
register_provider(Module) when is_atom(Module) ->
    case is_valid_provider(Module) of
        true ->
            persistent_term:put(?PROVIDER, Module);
        false ->
            {error, invalid_provider}
    end.

-spec unregister_provider(module()) -> ok | {error, term()}.
unregister_provider(Module) ->
    case persistent_term:get(?PROVIDER, undefined) of
        Module ->
            persistent_term:erase(?PROVIDER),
            ok;
        _ ->
            {error, not_registered}
    end.

-spec provider() -> module() | undefined.
provider() ->
    persistent_term:get(?PROVIDER, undefined).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% TODO:
%% 1. Add more checks for the provider module and functions
%% 2. Add more checks for the trace functions
is_valid_provider(Module) ->
    lists:all(
        fun({F, A}) -> erlang:function_exported(Module, F, A) end,
        ?MODULE:behaviour_info(callbacks) -- ?MODULE:behaviour_info(optional_callbacks)
    ).
