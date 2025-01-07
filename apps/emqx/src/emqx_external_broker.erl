%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_external_broker).

-callback forward(emqx_types:delivery()) ->
    emqx_types:publish_result().

-callback add_route(emqx_types:topic()) -> ok.
-callback delete_route(emqx_types:topic()) -> ok.

-callback add_shared_route(emqx_types:topic(), emqx_types:group()) -> ok.
-callback delete_shared_route(emqx_types:topic(), emqx_types:group()) -> ok.

-callback add_persistent_route(emqx_types:topic(), emqx_persistent_session_ds:id()) -> ok.
-callback delete_persistent_route(emqx_types:topic(), emqx_persistent_session_ds:id()) -> ok.

-type dest() :: term().

-export([
    %% Registration
    provider/0,
    register_provider/1,
    unregister_provider/1,
    %% Forwarding
    forward/1,
    %% Routing updates
    add_route/1,
    delete_route/1,
    add_shared_route/2,
    delete_shared_route/2,
    add_persistent_route/2,
    delete_persistent_route/2,
    add_persistent_shared_route/3,
    delete_persistent_shared_route/3
]).

-export_type([dest/0]).

-include("logger.hrl").

-define(PROVIDER, {?MODULE, external_broker}).

-define(safe_with_provider(IfRegistered, IfNotRegistered),
    case persistent_term:get(?PROVIDER, undefined) of
        undefined ->
            IfNotRegistered;
        Provider ->
            try
                Provider:IfRegistered
            catch
                Err:Reason:St ->
                    ?SLOG_THROTTLE(error, #{
                        msg => external_broker_crashed,
                        provider => Provider,
                        callback => ?FUNCTION_NAME,
                        stacktrace => St,
                        error => Err,
                        reason => Reason
                    }),
                    {error, Reason}
            end
    end
).

%% TODO: provider API copied from emqx_external_traces,
%% but it can be moved to a common module.

%%--------------------------------------------------------------------
%% Provider API
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
%% Broker API
%%--------------------------------------------------------------------

forward(Delivery) ->
    ?safe_with_provider(?FUNCTION_NAME(Delivery), []).

add_route(Topic) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic), ok).

delete_route(Topic) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic), ok).

add_shared_route(Topic, Group) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, Group), ok).

delete_shared_route(Topic, Group) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, Group), ok).

add_persistent_route(Topic, ID) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, ID), ok).

delete_persistent_route(Topic, ID) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, ID), ok).

add_persistent_shared_route(Topic, Group, ID) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, Group, ID), ok).

delete_persistent_shared_route(Topic, Group, ID) ->
    ?safe_with_provider(?FUNCTION_NAME(Topic, Group, ID), ok).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_valid_provider(Module) ->
    lists:all(
        fun({F, A}) -> erlang:function_exported(Module, F, A) end,
        ?MODULE:behaviour_info(callbacks)
    ).
