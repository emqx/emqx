%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_message).

-behaviour(emqx_config_handler).

-include("emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([init/0]).
-export([is_persistence_enabled/0, is_persistence_enabled/1, force_ds/1, get_db_config/0]).

%% Config handler
-export([add_handler/0, pre_config_update/3]).

%% Message persistence
-export([
    persist/1
]).

-include("emqx_persistent_message.hrl").

%%--------------------------------------------------------------------

init() ->
    %% Note: currently persistence can't be enabled or disabled in the
    %% runtime. If persistence is enabled for any of the zones, we
    %% consider durability feature to be on:
    Zones = maps:keys(emqx_config:get([zones])),
    IsEnabled = lists:any(fun is_persistence_enabled/1, Zones),
    persistent_term:put(?PERSISTENCE_ENABLED, IsEnabled),
    ?WITH_DURABILITY_ENABLED(begin
        ?SLOG(notice, #{msg => "Session durability is enabled"}),
        ok = emqx_ds:open_db(?PERSISTENT_MESSAGE_DB, get_db_config()),
        ok = emqx_persistent_session_ds_router:init_tables(),
        ok = emqx_persistent_session_ds:create_tables()
    end).

-spec is_persistence_enabled() -> boolean().
is_persistence_enabled() ->
    persistent_term:get(?PERSISTENCE_ENABLED, false).

-spec is_persistence_enabled(emqx_types:zone()) -> boolean().
is_persistence_enabled(Zone) ->
    emqx_config:get_zone_conf(Zone, [durable_sessions, enable]).

-spec get_db_config() -> emqx_ds:create_db_opts().
get_db_config() ->
    emqx_ds_schema:db_config([durable_storage, messages]).

%% Dev-only option: force all messages to go through
%% `emqx_persistent_session_ds':
-spec force_ds(emqx_types:zone()) -> boolean().
force_ds(Zone) ->
    emqx_config:get_zone_conf(Zone, [durable_sessions, force_persistence]).

%%--------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    emqx_config_handler:add_handler([durable_sessions], ?MODULE).

pre_config_update([durable_sessions], #{<<"enable">> := New}, #{<<"enable">> := Old}) when
    New =/= Old
->
    {error, "Hot update of durable_sessions.enable parameter is currently not supported"};
pre_config_update(_Root, _NewConf, _OldConf) ->
    ok.

%%--------------------------------------------------------------------

-spec persist(emqx_types:message()) ->
    emqx_ds:store_batch_result() | {skipped, needs_no_persistence}.
persist(Msg) ->
    ?WITH_DURABILITY_ENABLED(
        case needs_persistence(Msg) andalso has_subscribers(Msg) of
            true ->
                store_message(Msg);
            false ->
                {skipped, needs_no_persistence}
        end
    ).

needs_persistence(Msg) ->
    not emqx_message:get_flag(dup, Msg).

-spec store_message(emqx_types:message()) -> emqx_ds:store_batch_result().
store_message(Msg) ->
    emqx_metrics:inc('messages.persisted'),
    emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, [Msg], #{sync => false}).

has_subscribers(#message{topic = Topic}) ->
    emqx_persistent_session_ds_router:has_any_route(Topic).

%%
