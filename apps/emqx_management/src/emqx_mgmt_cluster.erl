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

-module(emqx_mgmt_cluster).

-behaviour(gen_server).

%% APIs
-export([start_link/0]).

-export([invite_async/1, invitation_status/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec invite_async(atom()) -> ok | ignore | {error, {already_started, pid()}}.
invite_async(Node) ->
    %% Proxy the invitation task to the leader node
    JoinTo = mria_membership:leader(),
    case Node =/= JoinTo of
        true ->
            gen_server:call({?MODULE, JoinTo}, {invite_async, Node, JoinTo}, infinity);
        false ->
            ignore
    end.

-spec invitation_status() -> map().
invitation_status() ->
    Leader = mria_membership:leader(),
    gen_server:call({?MODULE, Leader}, invitation_status, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #{}}.

handle_call({invite_async, Node, JoinTo}, _From, State) ->
    case maps:get(Node, State, undefined) of
        undefined ->
            Caller = self(),
            Task = spawn_link_invite_worker(Node, JoinTo, Caller),
            State1 = remove_finished_task(Node, State),
            {reply, ok, State1#{Node => Task}};
        WorkerPid ->
            {reply, {error, {already_started, WorkerPid}}, State}
    end;
handle_call(invitation_status, _From, State) ->
    {reply, state_to_invitation_status(State), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({task_done, _WorkerPid, Node, Result}, State) ->
    case maps:take(Node, State) of
        {Task, State1} ->
            History = maps:get(history, State1, #{}),
            Task1 = Task#{
                result => Result,
                finished_at => erlang:system_time(millisecond)
            },
            {noreply, State1#{history => History#{Node => Task1}}};
        error ->
            {noreply, State}
    end;
handle_info({'EXIT', WorkerPid, Reason}, State) ->
    case take_node_name_via_worker_pid(WorkerPid, State) of
        {key_value, Node, Task, State1} ->
            History = maps:get(history, State1, #{}),
            Task1 = Task#{
                result => {error, Reason},
                finished_at => erlang:system_time(millisecond)
            },
            {noreply, State1#{history => History#{Node => Task1}}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

spawn_link_invite_worker(Node, JoinTo, Caller) ->
    Pid = erlang:spawn_link(
        fun() ->
            Result =
                case emqx_mgmt_cluster_proto_v3:invite_node(Node, JoinTo, infinity) of
                    ok ->
                        ok;
                    {error, {already_in_cluster, _Node}} ->
                        ok;
                    {error, _} = E ->
                        E;
                    {badrpc, Reason} ->
                        {error, {badrpc, Reason}}
                end,
            Caller ! {task_done, self(), Node, Result}
        end
    ),
    #{worker => Pid, started_at => erlang:system_time(millisecond)}.

take_node_name_via_worker_pid(WorkerPid, Map) when is_map(Map) ->
    Key = find_node_name_via_worker_pid(WorkerPid, maps:next(maps:iterator(Map))),
    case maps:take(Key, Map) of
        error ->
            error;
        {Vaule, Map1} ->
            {key_value, Key, Vaule, Map1}
    end.

find_node_name_via_worker_pid(_WorkerPid, none) ->
    error;
find_node_name_via_worker_pid(WorkerPid, {Key, Task, I}) ->
    case maps:get(worker, Task, undefined) of
        WorkerPid ->
            Key;
        _ ->
            find_node_name_via_worker_pid(WorkerPid, maps:next(I))
    end.

remove_finished_task(Node, State = #{history := History}) ->
    State#{history => maps:remove(Node, History)};
remove_finished_task(_Node, State) ->
    State.

state_to_invitation_status(State) ->
    History = maps:get(history, State, #{}),
    {Succ, Failed} = lists:foldl(
        fun({Node, Task}, {SuccAcc, FailedAcc}) ->
            #{
                started_at := StartedAt,
                finished_at := FinishedAt,
                result := Result
            } = Task,
            Ret = #{node => Node, started_at => StartedAt, finished_at => FinishedAt},
            case is_succeed_result(Result) of
                true ->
                    {[Ret | SuccAcc], FailedAcc};
                false ->
                    {SuccAcc, [Ret#{reason => format_error_reason(Result)} | FailedAcc]}
            end
        end,
        {[], []},
        maps:to_list(History)
    ),

    InPro = maps:fold(
        fun(Node, _Task = #{started_at := StartedAt}, Acc) ->
            [#{node => Node, started_at => StartedAt} | Acc]
        end,
        [],
        maps:without([history], State)
    ),
    #{succeed => Succ, in_progress => InPro, failed => Failed}.

is_succeed_result(Result) ->
    case Result of
        ok ->
            true;
        {error, {already_in_cluster, _Node}} ->
            true;
        _ ->
            false
    end.

format_error_reason(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).
