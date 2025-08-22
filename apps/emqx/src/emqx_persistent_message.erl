%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_message).

-behaviour(emqx_config_handler).
-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, init/1, terminate/2, handle_continue/2, handle_call/3, handle_cast/2]).
-export([
    is_persistence_enabled/0,
    wait_readiness/1,
    is_persistence_enabled/1,
    force_ds/1,
    get_db_config/0
]).

%% Config handler
-export([add_handler/0, pre_config_update/3]).

%% Message persistence
-export([
    persist/2,
    store_batch/2
]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("emqx_persistent_message.hrl").

-type persist_opts() :: #{sync => boolean() | noreply}.

-define(optvar_ready, emqx_persistent_message_ready).

%%--------------------------------------------------------------------

-doc """
This function returns when EMQX has finished initializing databases
needed for durable sessions.

This is a temporary solution that is meant to work around a problem
related to the EMQX startup sequence: before cluster discovery begins,
EMQX for some reason waits for the full startup of the applications.
It cannot occur if the DS databases are configured to wait for a
certain number of replicas.

As a temporary solution, we let EMQX start without waiting for durable
storages. Once EMQX startup sequence is fixed and split into
reasonable stages, this function should be removed, and creation of
durable storages should become a prerequisite for start of EMQX
business applications.
""".
-spec wait_readiness(timeout()) -> ok | disabled | timeout.
wait_readiness(Timeout) ->
    case is_persistence_enabled() of
        true ->
            case optvar:read(?optvar_ready, Timeout) of
                {ok, _} ->
                    ok;
                Err ->
                    Err
            end;
        false ->
            disabled
    end.

-spec is_persistence_enabled() -> boolean().
is_persistence_enabled() ->
    persistent_term:get(?PERSISTENCE_ENABLED, false).

-spec is_persistence_enabled(emqx_types:zone()) -> boolean().
is_persistence_enabled(Zone) ->
    emqx_config:get_zone_conf(Zone, [durable_sessions, enable]).

-spec get_db_config() -> emqx_ds:create_db_opts().
get_db_config() ->
    Opts = emqx_ds_schema:db_config_messages(),
    Opts#{
        store_ttv => true,
        payload_type => ?ds_pt_mqtt
    }.

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

-doc """
Check if a message has durable subscribers and save message to `messages` DB if so.
""".
-spec persist(emqx_types:message(), persist_opts()) ->
    noreply | {skipped, needs_no_persistence} | reference().
persist(Msg, Opts) ->
    ?WITH_DURABILITY_ENABLED(
        case needs_persistence(Msg) andalso has_subscribers(Msg) of
            true ->
                store_batch([Msg], Opts);
            false ->
                {skipped, needs_no_persistence}
        end
    ).

needs_persistence(Msg) ->
    not emqx_message:get_flag(dup, Msg).

has_subscribers(#message{topic = Topic}) ->
    emqx_persistent_session_ds_router:has_any_route(Topic).

-doc """
Unconditionally save messages to the DS DB.

WARNING: this function assumes (without checking) that all messages originate from the same publisher.
Shard is decided by client ID of the *first* message.
""".
-spec store_batch
    ([emqx_types:message(), ...], #{sync := true}) -> ok | emqx_ds:error(_);
    ([emqx_types:message(), ...], #{sync := false}) -> reference();
    ([emqx_types:message(), ...], #{sync := noreply}) -> noreply.
store_batch([#message{from = From} | _] = Msgs, Opts) ->
    Shard = emqx_ds:shard_of(?PERSISTENT_MESSAGE_DB, From),
    {Count, TTVs} = count_and_convert(Msgs),
    emqx_metrics:inc('messages.persisted', Count),
    Sync = maps:get(sync, Opts, true),
    MaybeRef = emqx_ds:dirty_append(
        #{
            db => ?PERSISTENT_MESSAGE_DB,
            shard => Shard,
            reply => Sync =/= noreply
        },
        TTVs
    ),
    case Sync of
        true ->
            receive
                ?ds_tx_commit_reply(MaybeRef, Reply) ->
                    case emqx_ds:dirty_append_outcome(MaybeRef, Reply) of
                        {ok, _Serial} ->
                            ok;
                        {error, _, _} = Err ->
                            Err
                    end
            after 5_000 ->
                ?err_rec(commit_timeout)
            end;
        false ->
            MaybeRef;
        noreply ->
            noreply
    end.

count_and_convert([Msg]) ->
    {1, [emqx_ds_payload_transform:message_to_ttv(Msg)]};
count_and_convert(L) ->
    count_and_convert(L, 0, []).

count_and_convert([], Count, Acc) ->
    {Count, lists:reverse(Acc)};
count_and_convert([Msg | Rest], Count, Acc0) ->
    Acc = [emqx_ds_payload_transform:message_to_ttv(Msg) | Acc0],
    count_and_convert(Rest, Count + 1, Acc).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    process_flag(trap_exit, true),
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
    optvar:set(?optvar_ready, true),
    {noreply, State}.

terminate(_, _) ->
    optvar:unset(?optvar_ready).

handle_call(_, _, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_, State) ->
    {noreply, State}.
