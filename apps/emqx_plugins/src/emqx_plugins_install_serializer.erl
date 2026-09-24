%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Coordinates plugin installations through the oldest core node.
%% An occupied lock is reported immediately so the caller can retry later.
%% Coordination requires an available server; there is no local fallback.
%% The original installation process owns the lock, including remote requests.
%% The process which holds the lock can take it again, so the steps which
%% compose one installation share a single critical section.
%% Server restarts and network partitions retain the existing limitations.
-module(emqx_plugins_install_serializer).

-behaviour(gen_server).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, run/2, lock_status/0, child_spec/0]).
-export([acquire_lock/1, release_lock/1]).
-export([supports_install_lock/0, install_in_progress/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?MODULE).

%% The lock this process took, so that it can be taken again by the same
%% process without calling the coordinator a second time.
-define(HELD_KEY, {?MODULE, held_lock}).

%% The applications installed on this node right now, see `install_in_progress/1'.
-define(LOCAL_INSTALLS, {?MODULE, local_installs}).

-type lock() :: reference().
-type state() :: #{owner := {pid(), lock()} | undefined}.

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

-spec start_link() -> {ok, pid()} | {error, term()} | ignore.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Mark the version of `emqx_plugins' which takes the cluster wide
%% installation lock.
%%
%% A node which may install a package probes this on every other target node
%% (`emqx_plugins:node_supports_install_lock/1'): an older node has neither this
%% module nor this function, and the probe fails there.  Answering is all it
%% does, so it is safe to call on any node, including a replicant, whose local
%% server is not started.
-spec supports_install_lock() -> true.
supports_install_lock() ->
    true.

%% @doc Run the callback while holding the cluster installation lock.
-spec run(name_vsn(), fun(() -> T)) -> T | {error, map()}.
run(NameVsn, Fun) ->
    case erlang:get(?HELD_KEY) of
        undefined ->
            case acquire(NameVsn) of
                {ok, Coordinator, Lock} ->
                    erlang:put(?HELD_KEY, {Coordinator, Lock}),
                    ok = mark_install_started(NameVsn),
                    try
                        Fun()
                    after
                        erlang:erase(?HELD_KEY),
                        mark_install_finished(NameVsn),
                        release(Coordinator, Lock)
                    end;
                {error, _} = Error ->
                    Error
            end;
        {_Coordinator, _Lock} ->
            %% This process already holds the lock: the callers take it once
            %% around the whole sequence which replaces an installation, and the
            %% steps inside that sequence take it again.
            Fun()
    end.

%% @doc Whether this node runs an installation of `NameVsn' right now.
%%
%% The installation lock is cluster wide, but the files it protects are local
%% to each node: an installation running on another node can not touch this
%% node's install directory, while one running here can be between the
%% publication and the validation of its tree, when the tree is already
%% complete but can still be rolled back.  A caller which has to read the
%% installation state while the lock is not available uses this to tell the two
%% apart (`emqx_plugins:with_installed_or_refused/3').
%%
%% Installations of the same application are not told apart: replacing a
%% version also removes the other versions of that application.
-spec install_in_progress(name_vsn()) -> boolean().
install_in_progress(NameVsn) ->
    maps:is_key(install_app(NameVsn), local_installs()).

mark_install_started(NameVsn) ->
    update_local_installs(fun(Installs) ->
        maps:update_with(install_app(NameVsn), fun(Count) -> Count + 1 end, 1, Installs)
    end).

mark_install_finished(NameVsn) ->
    update_local_installs(fun(Installs) ->
        case maps:get(install_app(NameVsn), Installs, 0) of
            0 -> Installs;
            1 -> maps:remove(install_app(NameVsn), Installs);
            Count -> Installs#{install_app(NameVsn) => Count - 1}
        end
    end).

local_installs() ->
    persistent_term:get(?LOCAL_INSTALLS, #{}).

update_local_installs(Fun) ->
    persistent_term:put(?LOCAL_INSTALLS, Fun(local_installs())),
    ok.

install_app(NameVsn) ->
    case emqx_plugins_utils:split_name_vsn(NameVsn) of
        {AppName, _Vsn} -> AppName;
        error -> NameVsn
    end.

%% @doc Acquire this node's lock for the original installation process.
-spec acquire_lock(pid()) -> {ok, reference()} | {error, term()}.
acquire_lock(Holder) ->
    ok = ensure_started(),
    gen_server:call(?SERVER, {acquire, Holder}, infinity).

%% @doc Release the lock identified by the token returned by acquire_lock/1.
-spec release_lock(reference()) -> ok.
release_lock(Lock) ->
    gen_server:call(?SERVER, {release, Lock}, infinity).

%% @doc Report this node's holder; the waiting list is always empty.
-spec lock_status() -> #{owner => pid() | undefined, waiting => [pid()]}.
lock_status() ->
    case whereis(?SERVER) of
        undefined -> #{owner => undefined, waiting => []};
        _ -> gen_server:call(?SERVER, lock_status)
    end.

init([]) ->
    %% The lock is handed out by the coordinator, and that is always a core node
    %% (`mria_membership:coordinator/0' selects among core members only), so the
    %% server of a replicant node is never asked for a lock.
    case mria_rlog:role() of
        replicant ->
            ignore;
        _Core ->
            {ok, #{owner => undefined}}
    end.

-spec handle_call(term(), gen_server:from(), state()) -> {reply, term(), state()}.
handle_call(acquire, {Pid, _}, State) ->
    handle_acquire(Pid, State);
handle_call({acquire, Pid}, _From, State) when is_pid(Pid) ->
    handle_acquire(Pid, State);
handle_call({release, Lock}, _From, State) ->
    {reply, ok, release_owner(Lock, State)};
handle_call(lock_status, _From, State = #{owner := Owner}) ->
    {reply, #{owner => owner_pid(Owner), waiting => []}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_acquire(Pid, State = #{owner := undefined}) ->
    Lock = erlang:monitor(process, Pid),
    {reply, {ok, Lock}, State#{owner := {Pid, Lock}}};
handle_acquire(_Pid, State) ->
    {reply, {error, installation_in_progress}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({release, Lock}, State) ->
    {noreply, release_owner(Lock, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', Lock, process, Pid, _Reason}, State = #{owner := {Pid, Lock}}) ->
    {noreply, State#{owner := undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

release_owner(Lock, State = #{owner := {_Pid, Lock}}) ->
    _ = erlang:demonitor(Lock, [flush]),
    State#{owner := undefined};
release_owner(Lock, State) ->
    ?SLOG(warning, #{
        msg => "unexpected_plugin_install_lock_release",
        lock => Lock,
        owner => maps:get(owner, State, undefined)
    }),
    State.

%% A supervisor started before this child was introduced can add it on demand.
ensure_started() ->
    case whereis(?SERVER) of
        undefined ->
            case whereis(emqx_plugins_sup) of
                undefined ->
                    ok;
                _Sup ->
                    _ = supervisor:start_child(emqx_plugins_sup, child_spec()),
                    ok
            end;
        _Pid ->
            ok
    end.

-spec coordinator() -> node() | undefined.
coordinator() ->
    try
        mria_membership:coordinator()
    catch
        _Class:_Reason ->
            case whereis(mria_membership) of
                undefined ->
                    case nodes() of
                        [] -> node();
                        _Peers -> undefined
                    end;
                _Pid ->
                    undefined
            end
    end.

acquire(NameVsn) ->
    case coordinator() of
        undefined ->
            lock_error(NameVsn, coordinator_unknown);
        Coordinator ->
            try acquire_on(Coordinator, self()) of
                {ok, Lock} -> {ok, Coordinator, Lock};
                {error, Reason} -> lock_error(NameVsn, Reason)
            catch
                Class:Reason:Stacktrace ->
                    lock_error(NameVsn, Reason, #{
                        coordinator => Coordinator,
                        class => Class,
                        stacktrace => Stacktrace
                    })
            end
    end.

acquire_on(Coordinator, Holder) when Coordinator =:= node() ->
    acquire_lock(Holder);
acquire_on(Coordinator, Holder) ->
    emqx_plugins_proto_v6:acquire_install_lock(Coordinator, Holder).

release(Coordinator, Lock) ->
    try
        case Coordinator =:= node() of
            true -> release_lock(Lock);
            false -> emqx_plugins_proto_v6:release_install_lock(Coordinator, Lock)
        end
    catch
        Class:Reason ->
            ?SLOG(warning, #{
                msg => "failed_to_release_plugin_install_lock",
                coordinator => Coordinator,
                class => Class,
                reason => Reason
            })
    end.

lock_error(NameVsn, Reason) ->
    lock_error(NameVsn, Reason, #{}).

lock_error(NameVsn, Reason, Extra) ->
    ?SLOG(warning, Extra#{
        msg => "failed_to_acquire_plugin_install_lock",
        name_vsn => NameVsn,
        reason => Reason
    }),
    {error, #{
        msg => "failed_to_acquire_plugin_install_lock",
        name_vsn => NameVsn,
        reason => Reason
    }}.

owner_pid(undefined) -> undefined;
owner_pid({Pid, _Lock}) -> Pid.
