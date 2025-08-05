%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_message).

-behaviour(emqx_config_handler).
-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, init/1, handle_continue/2, handle_call/3, handle_cast/2]).
-export([is_persistence_enabled/0, is_persistence_enabled/1, force_ds/1, get_db_config/0]).

%% Config handler
-export([add_handler/0, pre_config_update/3]).

%% Message persistence
-export([
    persist/1
]).

-include("emqx_persistent_message.hrl").

%%--------------------------------------------------------------------

-spec is_persistence_enabled() -> boolean().
is_persistence_enabled() ->
    persistent_term:get(?PERSISTENCE_ENABLED, false).

-spec is_persistence_enabled(emqx_types:zone()) -> boolean().
is_persistence_enabled(Zone) ->
    emqx_config:get_zone_conf(Zone, [durable_sessions, enable]).

-spec get_db_config() -> emqx_ds:create_db_opts().
get_db_config() ->
    emqx_ds_schema:db_config_messages().

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

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    Zones = maps:keys(emqx_config:get([zones])),
    IsEnabled = lists:any(fun is_persistence_enabled/1, Zones),
    persistent_term:put(?PERSISTENCE_ENABLED, IsEnabled),
    case is_persistence_enabled() of
        true ->
            %% TODO: currently initialization is asynchronous. This is
            %% done to work around a deadlock that happens when Raft
            %% backend is used with `n_sites' > 0. During the initial
            %% forming of the cluster, the node should be able to
            %% proceed to the autocluster stage, which happens _after_
            %% full start of EMQX application. As a side effect, some
            %% durable features may become available _after_ listeners
            %% are enabled. This may lead to transient errors.
            %% Unfortunately, the real solution involves deep rework
            %% of emqx_machine and autocluster.
            {ok, undefined, {continue, real_init}};
        false ->
            ignore
    end.

handle_continue(real_init, State) ->
    %% Note: currently persistence can't be enabled or disabled in the
    %% runtime. If persistence is enabled for any of the zones, we
    %% consider durability feature to be on:
    ?SLOG(notice, #{msg => "Session durability is enabled"}),
    ok = emqx_ds:open_db(?PERSISTENT_MESSAGE_DB, get_db_config()),
    ok = emqx_persistent_session_ds_router:init_tables(),
    ok = emqx_persistent_session_ds:create_tables(),
    ok = emqx_persistent_session_ds_gc_timer:init(),
    ok = emqx_durable_will:init(),
    ok = emqx_ds:wait_db(?PERSISTENT_MESSAGE_DB, all, infinity),
    %% FIXME:
    ok = emqx_ds:wait_db(sessions, all, infinity),
    emqx_persistent_session_ds_sup:on_dbs_up(),
    {stop, normal, State}.

handle_call(_, _, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_, State) ->
    {noreply, State}.
