%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Serializes plugin installations across the cluster.
%%
%% Every node runs one instance of this server, but only the instance on the
%% cluster's coordinator hands out the lock: the coordinator is the oldest core
%% member which `mria' sees as up (`mria_membership:coordinator/0'), the node
%% the rest of EMQX delegates cluster wide work to as well.  The other nodes
%% take the lock from that instance with a `gen_server:call/3', so at most one
%% plugin installation runs in the whole cluster at a time.  The lock is not
%% keyed on the plugin: an installation is serialized against every other
%% installation, not only against the installations of the same plugin.
%%
%% The lock is fail closed: when the coordinator can not be determined, or its
%% instance can not be reached, `run/2' returns an error instead of running the
%% installation without a lock.  A node where `mria' is not running and which
%% has no peers is a cluster of one and coordinates with itself, so a single
%% node installation, and the unit tests which do not start the rest of EMQX,
%% need no cluster.
%%
%% The instance is started by `emqx_plugins_sup', and also on demand by the
%% first caller (`ensure_started/0'): a node whose new code was loaded by a hot
%% upgrade still runs the supervisor with the child specs it started with, so
%% without this the first installation after the upgrade would be refused.
%%
%% A granted lock is identified by the monitor reference of its holder, and
%% only that reference releases it: a caller can not release the lock of
%% another caller, and a release for a lock which is already gone (the holder
%% outlived the instance which granted it) is ignored.
%%
%% The lock is not re-entrant: a holder which takes it again waits for itself
%% forever, since the caller waits for an answer with no timeout.  Nothing in
%% the installation path (see `emqx_plugins_fs') takes it twice.
%%
%% The holder is monitored, so a holder which dies releases the lock.  One
%% window is not closed here: when the instance which hands out the lock dies,
%% its state is lost, and an installation which is still running can overlap
%% with one which starts after the instance restarted.  The same holds while
%% the connection to the holder is down: it loses the monitor (a remote
%% `monitor/2' reports `noconnection') and grants the lock again although the
%% holder may still be running.  Closing either needs a lock which survives the
%% instance and the partition, which is out of scope.
-module(emqx_plugins_install_serializer).

-behaviour(gen_server).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/0, run/2, lock_status/0, child_spec/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(SERVER, ?MODULE).

-type lock() :: reference().
-type lock_server() :: ?SERVER | {?SERVER, node()}.
-type waiter() :: {gen_server:from(), lock()}.
-type state() :: #{
    owner := {pid(), lock()} | undefined,
    waiters := queue:queue(waiter())
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Run `Fun' while holding the cluster wide plugin installation lock.
%%
%% `NameVsn' names the plugin the caller is installing; it is used for logging
%% only.
-spec run(name_vsn(), fun(() -> T)) -> T | {error, map()}.
run(NameVsn, Fun) ->
    case acquire(NameVsn) of
        {ok, Server, Lock} ->
            try
                Fun()
            after
                release(Server, Lock)
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc What this node's instance holds: the holder of the lock and the callers
%% which are queued for it, in the order they will be granted.
-spec lock_status() -> #{owner => pid() | undefined, waiting => [pid()]}.
lock_status() ->
    case whereis(?SERVER) of
        undefined ->
            #{owner => undefined, waiting => []};
        _Pid ->
            gen_server:call(?SERVER, lock_status)
    end.

%%--------------------------------------------------------------------
%% gen_server
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{owner => undefined, waiters => queue:new()}}.

-spec handle_call(term(), gen_server:from(), state()) ->
    {reply, {ok, lock()} | {error, term()} | map(), state()} | {noreply, state()}.
handle_call(acquire, {Pid, _Tag}, State = #{owner := undefined}) ->
    %% The lock is free: the caller owns it until it releases or dies.  Its
    %% monitor reference is the lock, so the `DOWN' below releases exactly the
    %% lock which was granted.
    Lock = erlang:monitor(process, Pid),
    {reply, {ok, Lock}, State#{owner := {Pid, Lock}}};
handle_call(acquire, From = {Pid, _Tag}, State = #{waiters := Waiters}) ->
    %% Held: the caller is queued, and it is not answered until it is granted
    %% the lock.  It is monitored from the start, so a caller which gives up
    %% while it waits is dropped instead of being granted the lock later.
    Lock = erlang:monitor(process, Pid),
    {noreply, State#{waiters := queue:in({From, Lock}, Waiters)}};
handle_call(lock_status, _From, State = #{owner := Owner, waiters := Waiters}) ->
    {reply,
        #{
            owner => owner_pid(Owner),
            waiting => [Pid || {{Pid, _Tag}, _Lock} <- queue:to_list(Waiters)]
        },
        State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({release, Lock}, State = #{owner := {_Pid, Lock}}) ->
    _ = erlang:demonitor(Lock, [flush]),
    {noreply, grant_next(State#{owner := undefined})};
handle_cast({release, Lock}, State) ->
    %% A release for a lock which is not the current one: the instance restarted
    %% while the holder was running, and the caller is releasing a lock state
    %% which is already gone.
    ?SLOG(warning, #{
        msg => "unexpected_plugin_install_lock_release",
        lock => Lock,
        owner => maps:get(owner, State, undefined)
    }),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', Lock, process, Pid, _Reason}, State = #{owner := {Pid, Lock}}) ->
    %% The holder died without releasing: the lock is free again.
    {noreply, grant_next(State#{owner := undefined})};
handle_info({'DOWN', Lock, process, _Pid, _Reason}, State = #{waiters := Waiters}) ->
    %% A caller which was still waiting died: it is dropped from the queue.
    {noreply, State#{waiters := queue:filter(fun({_From, L}) -> L =/= Lock end, Waiters)}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% The coordinator hands out the lock, every other node takes it from there.
%%
%% Only the coordinator's instance is consulted, so all nodes contend for the
%% same lock.  The instances on the other nodes are idle.
-spec lock_server(name_vsn()) -> {ok, lock_server()} | {error, map()}.
lock_server(NameVsn) ->
    case coordinator() of
        undefined ->
            lock_error(NameVsn, coordinator_unknown);
        Coordinator when Coordinator =:= node() ->
            ok = ensure_started(),
            case whereis(?SERVER) of
                undefined ->
                    lock_error(NameVsn, serializer_not_running);
                _Pid ->
                    {ok, ?SERVER}
            end;
        Coordinator ->
            {ok, {?SERVER, Coordinator}}
    end.

%% Start this node's instance if it is not running.  The instance is normally a
%% child of `emqx_plugins_sup'; a supervisor which was started before this
%% server existed (a hot upgrade) does not have it in its child specs, and
%% adding it here lets the supervisor take it over from then on.
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

%% The node whose instance hands out the lock: the oldest core member which
%% `mria' sees as up.  It is the same node the rest of EMQX delegates cluster
%% wide work to (`mria_membership:coordinator/0').
%%
%% A node where `mria' is not running at all (the plugins application is used
%% without the rest of EMQX, as in the unit tests) and which has no peers is a
%% cluster of one, and coordinates with itself.  Everywhere else the coordinator
%% can not be determined without guessing, and the lock fails closed rather than
%% falling back to a lock which would order this node only.
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
    case lock_server(NameVsn) of
        {ok, Server} ->
            try gen_server:call(Server, acquire, infinity) of
                {ok, Lock} ->
                    {ok, Server, Lock};
                Other ->
                    lock_error(NameVsn, {unexpected_reply, Other})
            catch
                Class:Reason:Stacktrace ->
                    lock_error(NameVsn, Reason, #{
                        server => Server,
                        class => Class,
                        stacktrace => Stacktrace
                    })
            end;
        {error, _} = Error ->
            Error
    end.

release(Server, Lock) ->
    gen_server:cast(Server, {release, Lock}).

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

grant_next(State = #{owner := undefined, waiters := Waiters}) ->
    case queue:out(Waiters) of
        {empty, _} ->
            State;
        {{value, {From, Lock}}, Rest} ->
            %% The waiter is granted the same lock (monitor reference) it was
            %% queued with: if it died in the meantime, the `DOWN' which is
            %% already on its way releases the lock again.
            gen_server:reply(From, {ok, Lock}),
            State#{waiters := Rest, owner := {element(1, From), Lock}}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% The lock is not keyed on the plugin: two installations of different plugins
%% are serialized as well, so at most one installation runs in the cluster.
%% (This is also what the cluster wide test case in `emqx_plugins_SUITE' pins.)
serializes_callers_of_different_plugins_test() ->
    with_serializer(fun() ->
        assert_serialized("plugin-a-1.0.0", "plugin-b-2.0.0")
    end).

serializes_callers_of_the_same_plugin_test() ->
    with_serializer(fun() ->
        assert_serialized("plugin-a-1.0.0", "plugin-a-1.0.0")
    end).

%% A holder which dies without releasing the lock does not block the next
%% caller: the server monitors it.
releases_the_lock_when_the_holder_dies_test() ->
    with_serializer(fun() ->
        Parent = self(),
        Holder = hold_lock(Parent, "plugin-a-1.0.0"),
        receive
            {entered, Holder} -> ok
        after 5000 ->
            error(holder_did_not_get_the_lock)
        end,
        exit(Holder, kill),
        ?assertEqual(ok, run("plugin-b-2.0.0", fun() -> ok end))
    end).

%% A caller which gives up while it waits is dropped from the queue, so the
%% lock is not handed to a process which is already gone.
drops_a_waiter_which_dies_test() ->
    with_serializer(fun() ->
        Parent = self(),
        Holder = hold_lock(Parent, "plugin-a-1.0.0"),
        receive
            {entered, Holder} -> ok
        after 5000 ->
            error(holder_did_not_get_the_lock)
        end,
        Waiter = spawn(fun() ->
            run("plugin-b-2.0.0", fun() -> Parent ! waiter_entered end)
        end),
        ok = wait_until_queued(Waiter),
        exit(Waiter, kill),
        Holder ! release,
        receive
            {result, Holder, ok} -> ok
        after 5000 ->
            error(holder_did_not_finish)
        end,
        %% The dead waiter must not have taken the lock with it.
        ?assertEqual(ok, run("plugin-c-3.0.0", fun() -> ok end))
    end).

%% A release which does not carry the lock of the current holder is ignored:
%% the lock stays with its holder.
ignores_a_release_for_another_lock_test() ->
    with_serializer(fun() ->
        Parent = self(),
        Holder = hold_lock(Parent, "plugin-a-1.0.0"),
        receive
            {entered, Holder} -> ok
        after 5000 ->
            error(holder_did_not_get_the_lock)
        end,
        Second = spawn(fun() ->
            Result = run("plugin-b-2.0.0", fun() ->
                Parent ! {entered, self()},
                ok
            end),
            Parent ! {result, self(), Result}
        end),
        ok = wait_until_queued(Second),
        %% A lock reference which was never granted (as a holder which outlived
        %% the instance would send) releases nothing.
        gen_server:cast(?SERVER, {release, make_ref()}),
        _ = lock_status(),
        receive
            {entered, Second} -> ?assert(false)
        after 200 -> ok
        end,
        Holder ! release,
        receive
            {result, Holder, ok} -> ok
        after 5000 ->
            error(first_caller_did_not_finish)
        end,
        receive
            {result, Second, ok} -> ok
        after 5000 ->
            error(second_caller_did_not_finish)
        end
    end).

%% The lock is released on every exit path of the function it guards.
releases_the_lock_when_the_fun_raises_test() ->
    with_serializer(fun() ->
        ?assertThrow(boom, run("plugin-a-1.0.0", fun() -> throw(boom) end)),
        ?assertEqual(ok, run("plugin-a-1.0.0", fun() -> ok end))
    end).

returns_the_result_of_the_fun_test() ->
    with_serializer(fun() ->
        ?assertEqual({ok, 42}, run("plugin-a-1.0.0", fun() -> {ok, 42} end))
    end).

%% Fail closed: without the server (and without peers, which is the case in the
%% unit tests) the function is not run at all.
fails_closed_when_the_serializer_is_not_running_test() ->
    ok = stop_serializer(),
    Ran = make_ref(),
    ?assertMatch(
        {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
        run("plugin-a-1.0.0", fun() ->
            self() ! Ran,
            ok
        end)
    ),
    receive
        Ran -> ?assert(false)
    after 0 -> ok
    end.

%% Fail closed as well when the coordinator is another node which can not be
%% reached: the installation does not fall back to a lock which would order
%% this node only.
fails_closed_when_the_coordinator_can_not_be_reached_test() ->
    with_serializer(fun() ->
        ok = meck:new(mria_membership, [passthrough]),
        try
            ok = meck:expect(mria_membership, coordinator, fun() -> 'no-such-node@nowhere' end),
            Ran = make_ref(),
            ?assertMatch(
                {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
                run("plugin-a-1.0.0", fun() ->
                    self() ! Ran,
                    ok
                end)
            ),
            receive
                Ran -> ?assert(false)
            after 0 -> ok
            end
        after
            ok = meck:unload(mria_membership)
        end
    end).

assert_serialized(NameVsnA, NameVsnB) ->
    Parent = self(),
    Holder = hold_lock(Parent, NameVsnA),
    receive
        {entered, Holder} -> ok
    after 5000 ->
        error(first_caller_did_not_get_the_lock)
    end,
    Second = spawn(fun() ->
        Result = run(NameVsnB, fun() ->
            Parent ! {entered, self()},
            ok
        end),
        Parent ! {result, self(), Result}
    end),
    %% The second caller is queued.  The first one holds the lock and only
    %% releases it below, so within this window the second one can not have
    %% been granted: it must not be in its function.
    ok = wait_until_queued(Second),
    receive
        {entered, Second} ->
            ?assert(false)
    after 200 ->
        ok
    end,
    Holder ! release,
    receive
        {result, Holder, ok} -> ok
    after 5000 ->
        error(first_caller_did_not_finish)
    end,
    receive
        {entered, Second} -> ok
    after 5000 ->
        error(second_caller_did_not_get_the_lock)
    end,
    receive
        {result, Second, ok} -> ok
    after 5000 ->
        error(second_caller_did_not_finish)
    end.

hold_lock(Parent, NameVsn) ->
    spawn(fun() ->
        Result = run(NameVsn, fun() ->
            Parent ! {entered, self()},
            receive
                release -> ok
            end,
            ok
        end),
        Parent ! {result, self(), Result}
    end).

%% Wait until the server has the caller in its queue.  This only synchronizes
%% the test: what the caller does is asserted through the messages it sends.
wait_until_queued(Pid) ->
    wait_until_queued(Pid, 100).

wait_until_queued(Pid, 0) ->
    error({caller_not_queued, Pid});
wait_until_queued(Pid, Attempts) ->
    case lists:member(Pid, maps:get(waiting, lock_status())) of
        true ->
            ok;
        false ->
            timer:sleep(10),
            wait_until_queued(Pid, Attempts - 1)
    end.

with_serializer(Fun) ->
    ok = stop_serializer(),
    {ok, Pid} = start_link(),
    try
        Fun()
    after
        ok = gen_server:stop(Pid)
    end.

stop_serializer() ->
    case whereis(?SERVER) of
        undefined ->
            ok;
        Pid ->
            ok = gen_server:stop(Pid)
    end.

-endif.
