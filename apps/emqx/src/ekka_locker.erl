%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_locker).

-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    start_link/1,
    start_link/2
]).

%% For test cases
-export([stop/0, stop/1]).

%% Lock APIs
-export([
    acquire/1,
    acquire/2,
    acquire/3,
    acquire/4
]).

-export([
    release/1,
    release/2,
    release/3
]).

%% For RPC call
-export([
    acquire_lock/2,
    acquire_lock/3,
    release_lock/2
]).

%% gen_server Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type resource() :: term().

-type lock_type() :: local | leader | quorum | all.

-type lock_result() :: {boolean(), [node() | {node(), any()}]}.

-type piggyback() :: mfa() | undefined.

-export_type([
    resource/0,
    lock_type/0,
    lock_result/0,
    piggyback/0
]).

-record(lock, {
    resource :: resource(),
    owner :: pid(),
    counter :: integer(),
    created :: integer()
}).

-record(lease, {expiry, timer}).

-record(state, {locks, lease, monitors}).

-define(SERVER, ?MODULE).
-define(LOG(Level, Format, Args),
    logger:Level("Ekka(Locker): " ++ Format, Args)
).

%% 15 seconds by default
-define(LEASE_TIME, 15000).
-define(MC_TIMEOUT, 30000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(?SERVER).

-spec start_link(atom()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Name) ->
    start_link(Name, ?LEASE_TIME).

-spec start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Name, LeaseTime) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, LeaseTime], []).

-spec stop() -> ok.
stop() ->
    stop(?SERVER).

-spec stop(pid() | atom()) -> ok.
stop(Name) ->
    gen_server:call(Name, stop).

-spec acquire(resource()) -> lock_result().
acquire(Resource) ->
    acquire(?SERVER, Resource).

-spec acquire(atom(), resource()) -> lock_result().
acquire(Name, Resource) when is_atom(Name) ->
    acquire(Name, Resource, local).

-spec acquire(atom(), resource(), lock_type()) -> lock_result().
acquire(Name, Resource, Type) ->
    acquire(Name, Resource, Type, undefined).

-spec acquire(atom(), resource(), lock_type(), piggyback()) -> lock_result().
acquire(Name, Resource, local, Piggyback) when is_atom(Name) ->
    acquire_lock(Name, lock_obj(Resource), Piggyback);
acquire(Name, Resource, leader, Piggyback) when is_atom(Name) ->
    Leader = mria_membership:leader(),
    case
        rpc:call(
            Leader,
            ?MODULE,
            acquire_lock,
            [Name, lock_obj(Resource), Piggyback]
        )
    of
        Err = {badrpc, _Reason} ->
            {false, [{Leader, Err}]};
        Res ->
            Res
    end;
acquire(Name, Resource, quorum, Piggyback) when is_atom(Name) ->
    Ring = mria_membership:ring(up),
    Nodes = ekka_ring:find_nodes(Resource, Ring),
    acquire_locks(Nodes, Name, lock_obj(Resource), Piggyback);
acquire(Name, Resource, all, Piggyback) when is_atom(Name) ->
    acquire_locks(
        mria_membership:nodelist(up),
        Name,
        lock_obj(Resource),
        Piggyback
    ).

acquire_locks(Nodes, Name, LockObj, Piggyback) ->
    {ResL, _BadNodes} =
        rpc:multicall(Nodes, ?MODULE, acquire_lock, [Name, LockObj, Piggyback], ?MC_TIMEOUT),
    case merge_results(ResL) of
        Res = {true, _} ->
            Res;
        Res = {false, _} ->
            _ = rpc:multicall(Nodes, ?MODULE, release_lock, [Name, LockObj], ?MC_TIMEOUT),
            Res
    end.

acquire_lock(Name, LockObj, Piggyback) ->
    {acquire_lock(Name, LockObj), [with_piggyback(node(), Piggyback)]}.

acquire_lock(Name, LockObj = #lock{resource = Resource, owner = Owner}) ->
    Pos = #lock.counter,
    %% check lock status and set the lock atomically
    try ets:update_counter(Name, Resource, [{Pos, 0}, {Pos, 1, 1, 1}], LockObj) of
        %% no lock before, lock it
        [0, 1] ->
            true;
        %% has already been locked, either by self or by others
        [1, 1] ->
            case ets:lookup(Name, Resource) of
                [#lock{owner = Owner}] -> true;
                _Other -> false
            end
    catch
        error:badarg ->
            %% While remote node is booting, this might fail because
            %% the ETS table has not been created at that moment
            true
    end.

with_piggyback(Node, undefined) ->
    Node;
with_piggyback(Node, {M, F, Args}) ->
    {Node, erlang:apply(M, F, Args)}.

lock_obj(Resource) ->
    #lock{
        resource = Resource,
        owner = self(),
        counter = 0,
        created = erlang:system_time(millisecond)
    }.

-spec release(resource()) -> lock_result().
release(Resource) ->
    release(?SERVER, Resource).

-spec release(atom(), resource()) -> lock_result().
release(Name, Resource) ->
    release(Name, Resource, local).

-spec release(atom(), resource(), lock_type()) -> lock_result().
release(Name, Resource, local) ->
    release_lock(Name, lock_obj(Resource));
release(Name, Resource, leader) ->
    Leader = mria_membership:leader(),
    case rpc:call(Leader, ?MODULE, release_lock, [Name, lock_obj(Resource)]) of
        Err = {badrpc, _Reason} ->
            {false, [{Leader, Err}]};
        Res ->
            Res
    end;
release(Name, Resource, quorum) ->
    Ring = mria_membership:ring(up),
    Nodes = ekka_ring:find_nodes(Resource, Ring),
    release_locks(Nodes, Name, lock_obj(Resource));
release(Name, Resource, all) ->
    release_locks(mria_membership:nodelist(up), Name, lock_obj(Resource)).

release_locks(Nodes, Name, LockObj) ->
    {ResL, _BadNodes} = rpc:multicall(Nodes, ?MODULE, release_lock, [Name, LockObj], ?MC_TIMEOUT),
    merge_results(ResL).

release_lock(Name, #lock{resource = Resource, owner = Owner}) ->
    Res =
        try ets:lookup(Name, Resource) of
            [Lock = #lock{owner = Owner}] ->
                ets:delete_object(Name, Lock);
            [_Lock] ->
                false;
            [] ->
                true
        catch
            error:badarg -> true
        end,
    {Res, [node()]}.

merge_results([]) ->
    {false, []};
merge_results(ResL) ->
    merge_results(ResL, [], []).
merge_results([], Succ, []) ->
    {true, lists:flatten(Succ)};
merge_results([], _, Failed) ->
    {false, lists:flatten(Failed)};
merge_results([{true, Res} | ResL], Succ, Failed) ->
    merge_results(ResL, [Res | Succ], Failed);
merge_results([{false, Res} | ResL], Succ, Failed) ->
    merge_results(ResL, Succ, [Res | Failed]);
merge_results([{badrpc, _} = Res | ResL], Succ, Failed) ->
    merge_results(ResL, Succ, [Res | Failed]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Name, LeaseTime]) ->
    Tab = ets:new(Name, [
        public,
        set,
        named_table,
        {keypos, 2},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    TRef = timer:send_interval(LeaseTime * 2, check_lease),
    Lease = #lease{expiry = LeaseTime, timer = TRef},
    {ok, #state{locks = Tab, lease = Lease, monitors = #{}}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(check_lease, State = #state{locks = Tab, lease = Lease, monitors = Monitors}) ->
    Monitors1 = lists:foldl(
        fun(#lock{resource = Resource, owner = Owner}, MonAcc) ->
            case maps:find(Owner, MonAcc) of
                {ok, ResourceSet} ->
                    case is_set_elem(Resource, ResourceSet) of
                        true ->
                            %% force kill it as it might have hung
                            _ = spawn(fun() -> force_kill_lock_owner(Owner, Resource) end),
                            MonAcc;
                        false ->
                            maps:put(Owner, set_put(Resource, ResourceSet), MonAcc)
                    end;
                error ->
                    _MRef = erlang:monitor(process, Owner),
                    maps:put(Owner, set_put(Resource, #{}), MonAcc)
            end
        end,
        Monitors,
        check_lease(Tab, Lease, erlang:system_time(millisecond))
    ),
    {noreply, State#state{monitors = Monitors1}, hibernate};
handle_info(
    {'DOWN', _MRef, process, DownPid, _Reason},
    State = #state{locks = Tab, monitors = Monitors}
) ->
    case maps:find(DownPid, Monitors) of
        {ok, ResourceSet} ->
            lists:foreach(
                fun(Resource) ->
                    case ets:lookup(Tab, Resource) of
                        [Lock = #lock{owner = OwnerPid}] when OwnerPid =:= DownPid ->
                            ets:delete_object(Tab, Lock);
                        _ ->
                            ok
                    end
                end,
                set_to_list(ResourceSet)
            ),
            {noreply, State#state{monitors = maps:remove(DownPid, Monitors)}};
        error ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{lease = Lease}) ->
    cancel_lease(Lease).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_lease(Tab, #lease{expiry = Expiry}, Now) ->
    Spec = ets:fun2ms(fun({_, _, _, _, T} = Resource) when (Now - T) > Expiry -> Resource end),
    ets:select(Tab, Spec).

cancel_lease(#lease{timer = TRef}) -> timer:cancel(TRef).

set_put(Resource, ResourceSet) when is_map(ResourceSet) ->
    ResourceSet#{Resource => nil}.

set_to_list(ResourceSet) when is_map(ResourceSet) ->
    maps:keys(ResourceSet).

is_set_elem(Resource, ResourceSet) when is_map(ResourceSet) ->
    maps:is_key(Resource, ResourceSet).

force_kill_lock_owner(Pid, Resource) ->
    logger:error("kill ~p as it has held the lock for too long, resource: ~p", [Pid, Resource]),
    Fields = [status, message_queue_len, current_stacktrace],
    Status = rpc:call(node(Pid), erlang, process_info, [Pid, Fields], 5000),
    logger:error("lock_owner_status:~n~p", [Status]),
    _ = exit(Pid, kill),
    ok.

-ifdef(TEST).
force_kill_test() ->
    Pid = spawn(fun() ->
        receive
            foo ->
                ok
        end
    end),
    ?assert(is_process_alive(Pid)),
    ok = force_kill_lock_owner(Pid, resource),
    ?assertNot(is_process_alive(Pid)).

-endif.
