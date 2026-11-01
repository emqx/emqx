%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_session_mgr).

-moduledoc """
Session manager for A2A orchestration.

Creates sessions from incoming requests, spawns workers for entry
tasks, and coordinates DAG dependencies — when a task completes,
unblocks downstream tasks that were waiting on it.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    dispatch/4,
    task_completed/3,
    task_failed/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_a2a.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Called by emqx_a2a hook when a request arrives
dispatch(TargetId, Request, ReplyTopic, Correlation) ->
    gen_server:cast(?MODULE, {dispatch, TargetId, Request, ReplyTopic, Correlation}).

%% Called by worker when task completes successfully
task_completed(SessionId, TaskId, Result) ->
    gen_server:cast(?MODULE, {task_completed, SessionId, TaskId, Result}).

%% Called by worker when task fails
task_failed(SessionId, TaskId, Reason) ->
    gen_server:cast(?MODULE, {task_failed, SessionId, TaskId, Reason}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% session_id => #{workers => #{task_id => pid}, ...}
    {ok, #{sessions => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({dispatch, TargetId, Request, ReplyTopic, Correlation}, State) ->
    State1 = handle_dispatch(TargetId, Request, ReplyTopic, Correlation, State),
    {noreply, State1};
handle_cast({task_completed, SessionId, TaskId, Result}, State) ->
    State1 = handle_task_completed(SessionId, TaskId, Result, State),
    {noreply, State1};
handle_cast({task_failed, SessionId, TaskId, Reason}, State) ->
    State1 = handle_task_failed(SessionId, TaskId, Reason, State),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    State1 = handle_worker_down(Pid, Reason, State),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal: dispatch
%%--------------------------------------------------------------------

handle_dispatch(TargetId, Request, ReplyTopic, Correlation, State) ->
    %% Check if it's a workflow (prefixed with "workflow-") or a single agent
    case TargetId of
        <<"workflow-", WorkflowId/binary>> ->
            dispatch_workflow(WorkflowId, Request, ReplyTopic, Correlation, State);
        AgentId ->
            dispatch_agent(AgentId, Request, ReplyTopic, Correlation, State)
    end.

dispatch_agent(AgentId, Request, ReplyTopic, Correlation, State) ->
    case emqx_a2a_store:get_agent_cfg(AgentId) of
        {ok, AgentCfg} ->
            SessionId = generate_session_id(),
            Text = maps:get(text, Request, <<>>),

            TaskId = AgentId,
            TaskSpec = #{
                id => TaskId,
                agent_id => AgentId,
                description => Text,
                needs => [],
                next => <<"output">>,
                status => running
            },

            Session = #{
                id => SessionId,
                type => agent,
                ref_id => AgentId,
                reply_topic => ReplyTopic,
                correlation => Correlation,
                tasks => #{TaskId => TaskSpec},
                status => working
            },
            ok = emqx_a2a_store:put_session(Session),

            %% Publish "submitted" ack
            publish_status(ReplyTopic, Correlation, SessionId, submitted, <<>>),

            %% Spawn worker immediately
            WorkerArgs = #{
                session_id => SessionId,
                task_id => TaskId,
                agent_cfg => AgentCfg,
                prompt => Text,
                context => [],
                reply_topic => ReplyTopic,
                correlation => Correlation
            },
            case emqx_a2a_worker_sup:start_worker(WorkerArgs) of
                {ok, Pid} ->
                    erlang:monitor(process, Pid),
                    Sessions = maps:get(sessions, State),
                    SessionState = #{
                        workers => #{TaskId => Pid},
                        session => Session
                    },
                    State#{sessions := Sessions#{SessionId => SessionState}};
                {error, Reason} ->
                    ?LOG(error, #{
                        msg => "failed_to_spawn_worker",
                        agent_id => AgentId,
                        reason => Reason
                    }),
                    publish_status(
                        ReplyTopic,
                        Correlation,
                        SessionId,
                        failed,
                        <<"Failed to start worker">>
                    ),
                    State
            end;
        {error, not_found} ->
            ?LOG(warning, #{msg => "agent_cfg_not_found", agent_id => AgentId}),
            publish_error(
                ReplyTopic,
                Correlation,
                <<"Agent config not found: ", AgentId/binary>>
            ),
            State
    end.

dispatch_workflow(WorkflowId, Request, ReplyTopic, Correlation, State) ->
    case emqx_a2a_store:get_workflow(WorkflowId) of
        {ok, WorkflowDef} ->
            SessionId = generate_session_id(),
            Text = maps:get(text, Request, <<>>),
            Variables = maps:get(variables, Request, #{}),

            %% Build task specs from workflow definition
            TaskDefs = maps:get(tasks, WorkflowDef, []),
            Tasks = build_task_specs(TaskDefs, Text, Variables),

            Session = #{
                id => SessionId,
                type => workflow,
                ref_id => WorkflowId,
                reply_topic => ReplyTopic,
                correlation => Correlation,
                tasks => Tasks,
                status => working,
                extra => #{variables => Variables}
            },
            ok = emqx_a2a_store:put_session(Session),

            publish_status(ReplyTopic, Correlation, SessionId, submitted, <<>>),

            %% Spawn workers for entry tasks (no dependencies)
            Workers = spawn_entry_tasks(SessionId, Tasks, ReplyTopic, Correlation),

            Sessions = maps:get(sessions, State),
            SessionState = #{
                workers => Workers,
                session => Session,
                results => #{}
            },
            State#{sessions := Sessions#{SessionId => SessionState}};
        {error, not_found} ->
            ?LOG(warning, #{msg => "workflow_not_found", workflow_id => WorkflowId}),
            publish_error(
                ReplyTopic,
                Correlation,
                <<"Workflow not found: ", WorkflowId/binary>>
            ),
            State
    end.

%%--------------------------------------------------------------------
%% Internal: DAG coordination
%%--------------------------------------------------------------------

handle_task_completed(SessionId, TaskId, Result, State) ->
    Sessions = maps:get(sessions, State),
    case maps:get(SessionId, Sessions, undefined) of
        undefined ->
            ?LOG(warning, #{
                msg => "orphan_task_completed",
                session_id => SessionId,
                task_id => TaskId
            }),
            State;
        SessionState ->
            #{session := Session, workers := Workers} = SessionState,
            Results = maps:get(results, SessionState, #{}),
            Results1 = Results#{TaskId => Result},
            Tasks = maps:get(tasks, Session),

            %% Update task status
            TaskSpec = maps:get(TaskId, Tasks),
            Tasks1 = Tasks#{TaskId := TaskSpec#{status := completed}},

            %% Check for downstream tasks that are now unblocked
            Session1 = Session#{tasks := Tasks1},
            {NewWorkers, Session2} = spawn_unblocked_tasks(
                SessionId, Session1, Results1, Workers
            ),

            %% Check if all tasks are done
            AllDone = maps:fold(
                fun(_K, #{status := S}, Acc) -> Acc andalso (S =:= completed) end,
                true,
                maps:get(tasks, Session2)
            ),

            Session3 =
                case AllDone of
                    true ->
                        %% Publish final result (from terminal task)
                        TerminalResult = find_terminal_result(
                            maps:get(tasks, Session2), Results1
                        ),
                        publish_status(
                            maps:get(reply_topic, Session),
                            maps:get(correlation, Session),
                            SessionId,
                            completed,
                            TerminalResult
                        ),
                        Session2#{status := completed};
                    false ->
                        Session2
                end,

            ok = emqx_a2a_store:update_session(Session3),

            case maps:get(status, Session3) of
                S when S =:= completed; S =:= failed ->
                    %% Clean up from in-memory state; ETS copy persists for queries
                    State#{sessions := maps:remove(SessionId, Sessions)};
                _ ->
                    SessionState1 = SessionState#{
                        session := Session3,
                        workers := NewWorkers,
                        results := Results1
                    },
                    State#{sessions := Sessions#{SessionId := SessionState1}}
            end
    end.

handle_task_failed(SessionId, TaskId, Reason, State) ->
    Sessions = maps:get(sessions, State),
    case maps:get(SessionId, Sessions, undefined) of
        undefined ->
            State;
        SessionState ->
            #{session := Session} = SessionState,
            Tasks = maps:get(tasks, Session),
            TaskSpec = maps:get(TaskId, Tasks),
            Tasks1 = Tasks#{TaskId := TaskSpec#{status := failed}},
            Session1 = Session#{tasks := Tasks1, status := failed},
            ok = emqx_a2a_store:update_session(Session1),

            ReasonBin = iolist_to_binary(io_lib:format("~p", [Reason])),
            publish_status(
                maps:get(reply_topic, Session),
                maps:get(correlation, Session),
                SessionId,
                failed,
                <<"Task ", TaskId/binary, " failed: ", ReasonBin/binary>>
            ),

            %% Clean up from in-memory state
            State#{sessions := maps:remove(SessionId, Sessions)}
    end.

handle_worker_down(Pid, Reason, State) when Reason =/= normal ->
    Sessions = maps:get(sessions, State),
    %% Find which session/task this worker belongs to
    maps:fold(
        fun(SessionId, #{workers := Workers} = _SS, AccState) ->
            case find_task_by_pid(Pid, Workers) of
                {ok, TaskId} ->
                    handle_task_failed(
                        SessionId,
                        TaskId,
                        {worker_crashed, Reason},
                        AccState
                    );
                not_found ->
                    AccState
            end
        end,
        State,
        Sessions
    );
handle_worker_down(_Pid, _Reason, State) ->
    State.

find_task_by_pid(Pid, Workers) ->
    case
        maps:fold(
            fun(TaskId, P, Acc) ->
                case P of
                    Pid -> {ok, TaskId};
                    _ -> Acc
                end
            end,
            not_found,
            Workers
        )
    of
        {ok, TaskId} -> {ok, TaskId};
        not_found -> not_found
    end.

%%--------------------------------------------------------------------
%% Internal: task building and spawning
%%--------------------------------------------------------------------

build_task_specs(TaskDefs, _Text, Variables) ->
    lists:foldl(
        fun(TaskDef, Acc) ->
            Id = maps:get(<<"id">>, TaskDef),
            AgentId = maps:get(<<"agent">>, TaskDef),
            Desc0 = maps:get(<<"description">>, TaskDef, <<>>),
            Needs = maps:get(<<"needs">>, TaskDef, []),
            Next = maps:get(<<"next">>, TaskDef, <<>>),

            %% Interpolate variables into description
            Desc = interpolate_variables(Desc0, Variables),

            Status =
                case Needs of
                    [] -> pending_ready;
                    _ -> pending_waiting
                end,

            Acc#{
                Id => #{
                    id => Id,
                    agent_id => AgentId,
                    description => Desc,
                    needs => Needs,
                    next => Next,
                    status => Status
                }
            }
        end,
        #{},
        TaskDefs
    ).

interpolate_variables(Template, Variables) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            Pattern = <<"{", Key/binary, "}">>,
            binary:replace(Acc, Pattern, Value, [global])
        end,
        Template,
        Variables
    ).

spawn_entry_tasks(SessionId, Tasks, ReplyTopic, Correlation) ->
    maps:fold(
        fun(
            TaskId,
            #{
                needs := Needs,
                agent_id := AgentId,
                description := Desc
            },
            Workers
        ) ->
            case Needs of
                [] ->
                    spawn_task_worker(
                        SessionId,
                        TaskId,
                        AgentId,
                        Desc,
                        [],
                        ReplyTopic,
                        Correlation,
                        Workers
                    );
                _ ->
                    Workers
            end
        end,
        #{},
        Tasks
    ).

spawn_unblocked_tasks(SessionId, Session, Results, Workers) ->
    Tasks = maps:get(tasks, Session),
    ReplyTopic = maps:get(reply_topic, Session),
    Correlation = maps:get(correlation, Session),
    maps:fold(
        fun(
            TaskId,
            #{
                needs := Needs,
                status := Status,
                agent_id := AgentId,
                description := Desc
            } = TaskSpec,
            {AccWorkers, AccSession}
        ) ->
            case Status of
                pending_waiting ->
                    %% Check if all dependencies are satisfied
                    AllReady = lists:all(
                        fun(DepId) -> maps:is_key(DepId, Results) end,
                        Needs
                    ),
                    case AllReady of
                        true ->
                            %% Gather context from upstream results
                            Context = [
                                #{task => DepId, result => maps:get(DepId, Results)}
                             || DepId <- Needs
                            ],
                            Tasks1 = maps:get(tasks, AccSession),
                            Tasks2 = Tasks1#{TaskId := TaskSpec#{status := running}},
                            AccSession1 = AccSession#{tasks := Tasks2},
                            AccWorkers1 = spawn_task_worker(
                                SessionId,
                                TaskId,
                                AgentId,
                                Desc,
                                Context,
                                ReplyTopic,
                                Correlation,
                                AccWorkers
                            ),
                            {AccWorkers1, AccSession1};
                        false ->
                            {AccWorkers, AccSession}
                    end;
                _ ->
                    {AccWorkers, AccSession}
            end
        end,
        {Workers, Session},
        Tasks
    ).

spawn_task_worker(
    SessionId,
    TaskId,
    AgentId,
    Desc,
    Context,
    ReplyTopic,
    Correlation,
    Workers
) ->
    case emqx_a2a_store:get_agent_cfg(AgentId) of
        {ok, AgentCfg} ->
            WorkerArgs = #{
                session_id => SessionId,
                task_id => TaskId,
                agent_cfg => AgentCfg,
                prompt => Desc,
                context => Context,
                reply_topic => ReplyTopic,
                correlation => Correlation
            },
            case emqx_a2a_worker_sup:start_worker(WorkerArgs) of
                {ok, Pid} ->
                    erlang:monitor(process, Pid),
                    Workers#{TaskId => Pid};
                {error, Reason} ->
                    ?LOG(error, #{
                        msg => "failed_to_spawn_worker",
                        task_id => TaskId,
                        reason => Reason
                    }),
                    Workers
            end;
        {error, not_found} ->
            ?LOG(error, #{
                msg => "agent_cfg_not_found_for_task",
                task_id => TaskId,
                agent_id => AgentId
            }),
            Workers
    end.

find_terminal_result(Tasks, Results) ->
    %% Find the task with next=output or the last completed task
    Terminal = maps:fold(
        fun(TaskId, #{next := Next}, Acc) ->
            case Next of
                <<"output">> -> TaskId;
                <<>> -> TaskId;
                _ -> Acc
            end
        end,
        <<>>,
        Tasks
    ),
    maps:get(Terminal, Results, <<>>).

%%--------------------------------------------------------------------
%% Internal: MQTT publishing
%%--------------------------------------------------------------------

publish_status(ReplyTopic, _Correlation, SessionId, Status, Message) ->
    Payload = emqx_utils_json:encode(#{
        type => <<"TaskStatusUpdateEvent">>,
        task_id => SessionId,
        status => #{
            state => Status,
            message => Message
        }
    }),
    Msg = emqx_message:make(emqx_a2a, ReplyTopic, Payload),
    _ = emqx_broker:safe_publish(Msg),
    ok.

publish_error(ReplyTopic, Correlation, ErrorMsg) ->
    publish_status(ReplyTopic, Correlation, <<>>, failed, ErrorMsg).

generate_session_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).
