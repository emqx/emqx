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

%% --------------------------------------------------------------------
%% Trace in Rich mode callbacks

%% Client Connect/Disconnect
-callback trace_client_connect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback trace_client_authn(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback trace_client_disconnect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback trace_client_subscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback trace_client_authz(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback trace_client_unsubscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

%% --------------------------------------------------------------------
%% Span enrichments APIs

-callback add_span_attrs(Attrs) -> ok when
    Attrs :: attrs() | attrs_meta().

-callback add_span_event(EventName, Attrs) -> ok when
    EventName :: event_name(),
    Attrs :: attrs() | attrs_meta().

%% --------------------------------------------------------------------
%% Legacy mode callbacks

-callback trace_process_publish(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().

-callback start_trace_send(Delivers, Attrs) -> Delivers when
    Delivers :: [emqx_types:deliver()],
    Attrs :: attrs().

-callback end_trace_send(Packet | [Packet]) -> ok when
    Packet :: emqx_types:packet().

-optional_callbacks(
    [
        add_span_attrs/1,
        add_span_event/2,
        trace_client_authn/3,
        trace_client_authz/3
    ]
).

%% --------------------------------------------------------------------

-export([
    trace_client_connect/3,
    trace_client_disconnect/3,
    trace_client_subscribe/3,
    trace_client_unsubscribe/3
]).

-export([
    trace_client_authn/3,
    trace_client_authz/3
]).

-export([
    add_span_attrs/1,
    add_span_event/2
]).

-export([
    provider/0,
    register_provider/1,
    unregister_provider/1,
    trace_process_publish/3,
    start_trace_send/2,
    end_trace_send/1
]).

-define(PROVIDER, {?MODULE, trace_provider}).

-define(with_provider(IfRegistered, IfNotRegistered),
    case persistent_term:get(?PROVIDER, undefined) of
        undefined ->
            IfNotRegistered;
        Provider ->
            Provider:IfRegistered
    end
).

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
%% Trace in Rich mode API
%%--------------------------------------------------------------------

%% @doc Start a trace event for Client CONNECT
-spec trace_client_connect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_connect(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start a trace event for Client DISCONNECT
-spec trace_client_disconnect(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_disconnect(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start a trace event for Client SUBSCRIBE
-spec trace_client_subscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_subscribe(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start a trace event for Client UNSUBSCRIBE
-spec trace_client_unsubscribe(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_unsubscribe(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start a sub-span for Client AUTHN
-spec trace_client_authn(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_authn(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start a sub-span for Client AUTHZ
-spec trace_client_authz(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_client_authz(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% --------------------------------------------------------------------
%% Span enrichments APIs
%% --------------------------------------------------------------------

%% @doc Enrich trace attributes
-spec add_span_attrs(AttrsOrMeta) -> ok when
    AttrsOrMeta :: attrs() | attrs_meta().
add_span_attrs(AttrsOrMeta) ->
    _ = catch ?with_provider(?FUNCTION_NAME(AttrsOrMeta), ok),
    ok.

%% @doc Add trace event
-spec add_span_event(EventName, AttrsOrMeta) -> ok when
    EventName :: event_name(),
    AttrsOrMeta :: attrs() | attrs_meta().
add_span_event(EventName, AttrsOrMeta) ->
    _ = catch ?with_provider(?FUNCTION_NAME(EventName, AttrsOrMeta), ok),
    ok.

%%--------------------------------------------------------------------
%% Legacy trace API
%%--------------------------------------------------------------------

%% @doc Trace message processing from publisher
-spec trace_process_publish(Packet, InitAttrs, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    InitAttrs :: attrs(),
    Res :: term().
trace_process_publish(Packet, InitAttrs, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, InitAttrs, ProcessFun), ProcessFun(Packet)).

%% @doc Start Trace message delivery to subscriber
-spec start_trace_send(Delivers, Attrs) -> Delivers when
    Delivers :: [emqx_types:deliver()],
    Attrs :: attrs().
start_trace_send(Delivers, Attrs) ->
    ?with_provider(?FUNCTION_NAME(Delivers, Attrs), Delivers).

%% @doc End Trace message delivery
-spec end_trace_send(Packet | [Packet]) -> ok when
    Packet :: emqx_types:packet().
end_trace_send(Packets) ->
    ?with_provider(?FUNCTION_NAME(Packets), ok).

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
