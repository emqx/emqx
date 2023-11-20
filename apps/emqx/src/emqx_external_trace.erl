%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-callback trace_process_publish(Packet, Channel, fun((Packet, Channel) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    Channel :: emqx_channel:channel(),
    Res :: term().

-callback start_trace_send(list(emqx_types:deliver()), emqx_channel:channel()) ->
    list(emqx_types:deliver()).

-callback end_trace_send(emqx_types:packet() | [emqx_types:packet()]) -> ok.

-callback event(EventName :: term(), Attributes :: term()) -> ok.

-export([
    register_provider/1,
    unregister_provider/1,
    trace_process_publish/3,
    start_trace_send/2,
    end_trace_send/1,
    event/1,
    event/2
]).

-define(PROVIDER, {?MODULE, trace_provider}).

-define(with_provider(IfRegisitered, IfNotRegisired),
    case persistent_term:get(?PROVIDER, undefined) of
        undefined ->
            IfNotRegisired;
        Provider ->
            Provider:IfRegisitered
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

%%--------------------------------------------------------------------
%% trace API
%%--------------------------------------------------------------------

-spec trace_process_publish(Packet, Channel, fun((Packet, Channel) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    Channel :: emqx_channel:channel(),
    Res :: term().
trace_process_publish(Packet, Channel, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, Channel, ProcessFun), ProcessFun(Packet, Channel)).

-spec start_trace_send(list(emqx_types:deliver()), emqx_channel:channel()) ->
    list(emqx_types:deliver()).
start_trace_send(Delivers, Channel) ->
    ?with_provider(?FUNCTION_NAME(Delivers, Channel), Delivers).

-spec end_trace_send(emqx_types:packet() | [emqx_types:packet()]) -> ok.
end_trace_send(Packets) ->
    ?with_provider(?FUNCTION_NAME(Packets), ok).

event(Name) ->
    event(Name, #{}).

-spec event(term(), term()) -> ok.
event(Name, Attributes) ->
    ?with_provider(?FUNCTION_NAME(Name, Attributes), ok).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_valid_provider(Module) ->
    lists:all(
        fun({F, A}) -> erlang:function_exported(Module, F, A) end,
        ?MODULE:behaviour_info(callbacks)
    ).
