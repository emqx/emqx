%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Legacy
-type channel_info() :: #{atom() => _}.
-export_type([channel_info/0]).

%% e2e traces
-type init_attrs() :: attrs().
-type attrs() :: #{atom() => _}.
-type t_fun() :: function().
-type t_args() :: list().
-type t_res() :: any().

-export_type([
    init_attrs/0,
    attrs/0,
    t_fun/0,
    t_args/0,
    t_res/0
]).

%% --------------------------------------------------------------------
%% Trace in Rich mode callbacks

%% Client Connect/Disconnect
-callback client_connect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_disconnect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_subscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_unsubscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authn(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authn_backend(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authz(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authz_backend(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_disconnect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_subscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_unsubscribe(init_attrs(), t_fun(), t_args()) -> t_res().

%% Message Processing Spans
%% PUBLISH(form Publisher) -> ROUTE -> FORWARD(optional) -> DELIVER(to Subscribers)
-callback client_publish(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_puback(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubrec(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubrel(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubcomp(init_attrs(), t_fun(), t_args()) -> t_res().

-callback msg_route(init_attrs(), t_fun(), t_args()) -> t_res().

%% @doc Trace message forwarding
%% The span `message.forward' always starts in the publisher process and ends in the subscriber process.
%% They are logically two unrelated processes. So the SpanCtx always need to be propagated.
-callback msg_forward(init_attrs(), t_fun(), t_args()) -> t_res().

-callback msg_handle_forward(init_attrs(), t_fun(), t_args()) -> t_res().

%% for broker_publish and outgoing, the process_fun is not needed
%% They Process Ctx in deliver/packet
-callback broker_publish(
    attrs(),
    TraceAction :: ?EXT_TRACE_START,
    list(Deliver)
) -> list(Deliver) when
    Deliver :: emqx_types:deliver().

-callback outgoing(
    attrs(),
    TraceAction :: ?EXT_TRACE_START | ?EXT_TRACE_STOP,
    Packet :: emqx_types:packet()
) -> Res :: t_res().

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
