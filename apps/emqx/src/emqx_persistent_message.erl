%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_message).

-behaviour(emqx_config_handler).

-include("emqx.hrl").

-export([init/0]).
-export([is_persistence_enabled/0, force_ds/0]).

%% Config handler
-export([add_handler/0, pre_config_update/3]).

%% Message persistence
-export([
    persist/1
]).

-define(PERSISTENT_MESSAGE_DB, emqx_persistent_message).

-define(WHEN_ENABLED(DO),
    case is_persistence_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

%%--------------------------------------------------------------------

init() ->
    ?WHEN_ENABLED(begin
        Backend = storage_backend(),
        ok = emqx_ds:open_db(?PERSISTENT_MESSAGE_DB, Backend),
        ok = emqx_persistent_session_ds_router:init_tables(),
        ok = emqx_persistent_session_ds:create_tables(),
        ok
    end).

-spec is_persistence_enabled() -> boolean().
is_persistence_enabled() ->
    emqx_config:get([session_persistence, enable]).

-spec storage_backend() -> emqx_ds:create_db_opts().
storage_backend() ->
    storage_backend([durable_storage, messages]).

%% Dev-only option: force all messages to go through
%% `emqx_persistent_session_ds':
-spec force_ds() -> boolean().
force_ds() ->
    emqx_config:get([session_persistence, force_persistence]).

storage_backend(Path) ->
    ConfigTree = #{'_config_handler' := {Module, Function}} = emqx_config:get(Path),
    apply(Module, Function, [ConfigTree]).

%%--------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    emqx_config_handler:add_handler([session_persistence], ?MODULE).

pre_config_update([session_persistence], #{<<"enable">> := New}, #{<<"enable">> := Old}) when
    New =/= Old
->
    {error, "Hot update of session_persistence.enable parameter is currently not supported"};
pre_config_update(_Root, _NewConf, _OldConf) ->
    ok.

%%--------------------------------------------------------------------

-spec persist(emqx_types:message()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
persist(Msg) ->
    ?WHEN_ENABLED(
        case needs_persistence(Msg) andalso has_subscribers(Msg) of
            true ->
                store_message(Msg);
            false ->
                {skipped, needs_no_persistence}
        end
    ).

needs_persistence(Msg) ->
    not (emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg)).

-spec store_message(emqx_types:message()) -> emqx_ds:store_batch_result().
store_message(Msg) ->
    emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, [Msg], #{sync => false}).

has_subscribers(#message{topic = Topic}) ->
    emqx_persistent_session_ds_router:has_any_route(Topic).

%%
