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

-callback trace_process_publish(Packet, ChannelInfo, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    ChannelInfo :: channel_info(),
    Res :: term().

-callback start_trace_send(list(emqx_types:deliver()), channel_info()) ->
    list(emqx_types:deliver()).

-callback end_trace_send(emqx_types:packet() | [emqx_types:packet()]) -> ok.

-type channel_info() :: #{atom() => _}.

-export([
    provider/0,
    register_provider/1,
    unregister_provider/1,
    trace_process_publish/3,
    start_trace_send/2,
    end_trace_send/1
]).

-export_type([channel_info/0]).

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
%% trace API
%%--------------------------------------------------------------------

-spec trace_process_publish(Packet, ChannelInfo, fun((Packet) -> Res)) -> Res when
    Packet :: emqx_types:packet(),
    ChannelInfo :: channel_info(),
    Res :: term().
trace_process_publish(Packet, ChannelInfo, ProcessFun) ->
    ?with_provider(?FUNCTION_NAME(Packet, ChannelInfo, ProcessFun), ProcessFun(Packet)).

-spec start_trace_send(list(emqx_types:deliver()), channel_info()) ->
    list(emqx_types:deliver()).
start_trace_send(Delivers, ChannelInfo) ->
    ?with_provider(?FUNCTION_NAME(Delivers, ChannelInfo), Delivers).

-spec end_trace_send(emqx_types:packet() | [emqx_types:packet()]) -> ok.
end_trace_send(Packets) ->
    ?with_provider(?FUNCTION_NAME(Packets), ok).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_valid_provider(Module) ->
    lists:all(
        fun({F, A}) -> erlang:function_exported(Module, F, A) end,
        ?MODULE:behaviour_info(callbacks)
    ).
