%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Serializes the installations of one plugin on one node.
%%
%% Installations of the same plugin can publish and roll back the same tree, so
%% they are ordered against each other; installations of different plugins write
%% different directories and do not block.  The lock belongs to the process which
%% runs the installation and is reentrant for it, so the steps of one
%% installation share a critical section.  An occupied lock is refused, it is not
%% queued.
-module(emqx_plugins_install_serializer).

-behaviour(gen_server).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, run/2, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SERVER, ?MODULE).

%% The locks this process holds and the server instance which handed each of
%% them out, so that it can take one of them again without calling the server
%% twice.  The server instance is recorded because a restarted server knows
%% nothing about the locks the previous one handed out.
-define(HELD_LOCKS, {?MODULE, held_locks}).

-type lock() :: reference().
-type state() :: #{locks := #{binary() => {pid(), lock()}}}.

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Run the callback while holding the installation lock of NameVsn's
%% plugin, or report that another process holds it.
-spec run(name_vsn(), fun(() -> T)) -> T | {error, map()}.
run(NameVsn, Fun) ->
    Plugin = plugin_name(NameVsn),
    Server = whereis(?SERVER),
    case held_lock(Plugin) of
        {ok, {Server, _Lock}} ->
            %% The process which runs an installation takes the lock once
            %% around the whole sequence, and its steps take it again.
            Fun();
        _StaleOrNotHeld ->
            acquire_and_run(NameVsn, Plugin, Fun)
    end.

acquire_and_run(NameVsn, Plugin, Fun) ->
    case acquire(NameVsn, Plugin) of
        {ok, Server, Lock} ->
            put_held_lock(Plugin, Server, Lock),
            try
                Fun()
            after
                erase_held_lock(Plugin),
                release(Plugin, Lock)
            end;
        {error, _} = Error ->
            Error
    end.

init([]) ->
    {ok, #{locks => #{}}}.

-spec handle_call(term(), gen_server:from(), state()) -> {reply, term(), state()}.
handle_call({acquire, Plugin, Holder}, _From, State = #{locks := Locks}) ->
    case maps:is_key(Plugin, Locks) of
        true ->
            {reply, {error, busy_reason(Plugin)}, State};
        false ->
            Lock = erlang:monitor(process, Holder),
            {reply, {ok, self(), Lock}, State#{locks := Locks#{Plugin => {Holder, Lock}}}}
    end;
handle_call({release, Plugin, Lock}, _From, State = #{locks := Locks}) ->
    case Locks of
        #{Plugin := {_Holder, Lock}} ->
            _ = erlang:demonitor(Lock, [flush]),
            {reply, ok, State#{locks := maps:remove(Plugin, Locks)}};
        _NotHeld ->
            ?SLOG(warning, #{
                msg => "unexpected_plugin_install_lock_release",
                plugin => Plugin,
                lock => Lock
            }),
            {reply, ok, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', Lock, process, _Pid, _Reason}, State = #{locks := Locks}) ->
    {noreply, State#{locks := maps:filter(fun(_Plugin, {_P, L}) -> L =/= Lock end, Locks)}};
handle_info(_Info, State) ->
    {noreply, State}.

acquire(NameVsn, Plugin) ->
    try gen_server:call(?SERVER, {acquire, Plugin, self()}, infinity) of
        {ok, Server, Lock} ->
            {ok, Server, Lock};
        {error, Reason} when is_map(Reason) ->
            {error, Reason#{name_vsn => emqx_plugins_utils:bin(NameVsn)}};
        Other ->
            Other
    catch
        %% The server is not running, so the installation can not be ordered
        %% against the others right now: reported as an occupied lock, which the
        %% callers retry.
        exit:{noproc, _} -> busy_error(NameVsn, Plugin)
    end.

release(Plugin, Lock) ->
    try
        gen_server:call(?SERVER, {release, Plugin, Lock}, infinity)
    catch
        %% The server died, so the lock it held is gone as well; the caller's
        %% result must not be replaced by this failure.
        exit:{noproc, _} -> ok
    end.

busy_reason(Plugin) ->
    #{
        msg => "plugin_installation_in_progress",
        plugin => Plugin,
        reason => installation_in_progress
    }.

busy_error(NameVsn, Plugin) ->
    {error, (busy_reason(Plugin))#{name_vsn => emqx_plugins_utils:bin(NameVsn)}}.

%% `parse_name_vsn/1' in emqx_plugins_utils creates an atom and the identifier may come from a
%% request, so the textual split is used here: every version of a plugin shares
%% one lock, which is what keeps two versions of it from installing at the same
%% time.
plugin_name(NameVsn) ->
    case emqx_plugins_utils:split_name_vsn(NameVsn) of
        {Plugin, _Vsn} -> Plugin;
        error -> emqx_plugins_utils:bin(NameVsn)
    end.

%% The lock the process holds, bound to the server instance which handed it
%% out: a restarted server knows nothing of it, and the lock has to be taken
%% again from the new instance.
held_lock(Plugin) ->
    maps:find(Plugin, held_locks()).

put_held_lock(Plugin, Server, Lock) ->
    _ = erlang:put(?HELD_LOCKS, (held_locks())#{Plugin => {Server, Lock}}),
    ok.

erase_held_lock(Plugin) ->
    _ = erlang:put(?HELD_LOCKS, maps:remove(Plugin, held_locks())),
    ok.

held_locks() ->
    case erlang:get(?HELD_LOCKS) of
        undefined -> #{};
        Held -> Held
    end.
