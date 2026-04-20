%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Pipeline instance gen_statem.
%%
%% One process per active pipeline instance.  Globally registered as
%% {emqx_agent_pipeline, Iid} so it is unique cluster-wide.
%%
%% Lifecycle
%%   start_link/2 is called by emqx_agent_pipeline_sup (simple_one_for_one).
%%   The iid is generated here, ensuring uniqueness.
%%   On completion the process enters `done` and starts an idle timer.
%%   Any message arriving in `done` resets and re-executes from step 0.
%%   When the idle timer fires the process stops normally.
%%
%% States
%%   running        — executing a deterministic step
%%   llm_loop       — active LLM session; proxying tool_request ↔ cap/invoke
%%   waiting_cap    — cap/invoke sent for a call_skill step; awaiting cap_reply
%%   waiting_event  — paused at a wait_for_event step
%%   done           — all steps done (or failed); idle timer running
%%
%% Incoming OTP messages (all sent as gen_statem casts by emqx_agent_pipeline_mgr)
%%   #sess_frame{sid, frame}   — from sess/out/<sid>/
%%   #cap_reply{req_id, frame} — from cap/reply/<req_id>
%%   #pipe_evt{topic, event}   — from evt/... (for wait_for_event steps)
%%
%% Context and JSONPath
%%   The pipeline maintains a `context` map.  Reading uses dotted paths
%%   starting with $ (e.g. "$.triage.result.incident_id" traverses the map
%%   recursively).  Writing supports top-level paths only ("$.triage" writes
%%   to context[<<"triage">>]).
%%
%% Tool specs
%%   Format:  "<type>@<skill_id>"  e.g. "message.publish@slack-dev"
%%   The type becomes the cap/invoke topic segment.
%%   The tool name sent to the LLM is the spec with non-[a-zA-Z0-9_-] replaced
%%   by underscore (e.g. "message_publish_slack_dev").
%%
%% Session config
%%   Each llm_loop step must supply LLM connection parameters, either inline
%%   via "session_config": {...} or by referencing a named profile registered
%%   with emqx_agent_pipeline_registry:register_profile/2.

-module(emqx_agent_pipeline).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_agent_pipeline.hrl").

%% Public API
-export([start_link/2, whereis/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-define(NAME(Iid), {?MODULE, Iid}).
-define(REG(Iid), {global, ?NAME(Iid)}).
%% 5-minute idle window before the process self-destructs after completion.
-define(IDLE_TIMEOUT_MS, 300_000).

-record(data, {
    pipeline_id :: binary(),
    iid :: binary(),
    trace_id :: binary(),
    definition :: map(),
    steps :: [map()],
    step_idx = 0 :: non_neg_integer(),
    context :: map(),
    %% llm_loop step state
    active_sid :: binary() | undefined,
    %% tool_name => {Type, SkillId}
    tool_map = #{} :: #{binary() => {binary(), binary()}},
    %% req_id => call_id (tracks outstanding skill calls within a loop)
    pending_calls = #{} :: #{binary() => binary()},
    %% value stored by a set_result tool call; written to result_path on final
    set_result_value = undefined :: map() | undefined,
    %% waiting_cap (call_skill step)
    cap_req_id :: binary() | undefined,
    cap_result_path :: binary() | undefined,
    %% waiting_event step
    wait_topic :: binary() | undefined,
    wait_where :: binary() | undefined,
    wait_result_path :: binary() | undefined,
    %% idle timer reference (done state)
    idle_timer_ref :: reference() | undefined
}).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec start_link(map(), map()) -> {ok, pid()} | {error, term()}.
start_link(Def, TriggerEvent) ->
    PipelineId = maps:get(<<"pipeline_id">>, Def),
    Iid = gen_iid(PipelineId),
    gen_statem:start_link(?REG(Iid), ?MODULE, {Iid, Def, TriggerEvent}, []).

-spec whereis(binary()) -> pid() | undefined.
whereis(Iid) ->
    global:whereis_name(?NAME(Iid)).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init({Iid, Def, TriggerEvent}) ->
    PipelineId = maps:get(<<"pipeline_id">>, Def),
    TraceId = maps:get(<<"trace_id">>, TriggerEvent, Iid),
    Steps = maps:get(<<"steps">>, Def, []),
    Data = #data{
        pipeline_id = PipelineId,
        iid = Iid,
        trace_id = TraceId,
        definition = Def,
        steps = Steps,
        step_idx = 0,
        context = #{<<"event">> => TriggerEvent},
        active_sid = undefined,
        tool_map = #{},
        pending_calls = #{},
        cap_req_id = undefined,
        cap_result_path = undefined,
        wait_topic = undefined,
        wait_where = undefined,
        wait_result_path = undefined,
        idle_timer_ref = undefined
    },
    ?SLOG(info, #{msg => "pipeline_started", pipeline_id => PipelineId, iid => Iid}),
    publish_pipeline_event(Data, #{<<"type">> => <<"pipeline_started">>}),
    {ok, running, Data, [{next_event, internal, step}]}.

terminate(Reason, waiting_event, #data{iid = Iid} = Data) ->
    emqx_agent_pipeline_mgr:unregister_waiting(Iid),
    do_log_terminate(Reason, Data);
terminate(Reason, _State, Data) ->
    do_log_terminate(Reason, Data).

do_log_terminate(Reason, #data{pipeline_id = PipelineId, iid = Iid}) ->
    ?SLOG(info, #{
        msg => "pipeline_terminating",
        pipeline_id => PipelineId,
        iid => Iid,
        reason => Reason
    }),
    ok.

%%--------------------------------------------------------------------
%% handle_event/4
%%--------------------------------------------------------------------

%% ── running: execute next step ───────────────────────────────────────────────

handle_event(internal, step, running, #data{step_idx = Idx, steps = Steps} = Data) ->
    case Idx < length(Steps) of
        false ->
            do_complete(Data);
        true ->
            Step = lists:nth(Idx + 1, Steps),
            execute_step(Step, Data)
    end;
%% ── llm_loop: session emitted a tool_request ─────────────────────────────────

handle_event(
    cast,
    #sess_frame{sid = Sid, frame = #{<<"type">> := <<"tool_request">>} = Frame},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    dispatch_tool_request(Frame, Data);
%% ── llm_loop: session emitted final ──────────────────────────────────────────

handle_event(
    cast,
    #sess_frame{sid = Sid, frame = #{<<"type">> := <<"final">>} = Frame},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    Result =
        case Data#data.set_result_value of
            undefined -> maps:get(<<"result">>, Frame, #{});
            Val -> Val
        end,
    Step = current_step(Data),
    Data1 = write_context(maps:get(<<"result_path">>, Step, undefined), Result, Data),
    Data2 = Data1#data{
        active_sid = undefined, tool_map = #{}, pending_calls = #{},
        set_result_value = undefined
    },
    ?SLOG(info, #{
        msg => "pipeline_llm_step_done",
        iid => Data#data.iid,
        step => maps:get(<<"id">>, Step, <<"?">>)
    }),
    advance_and_step(Data2);
%% ── llm_loop: session emitted error ──────────────────────────────────────────

handle_event(
    cast,
    #sess_frame{sid = Sid, frame = #{<<"type">> := <<"error">>} = Frame},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    Reason = maps:get(<<"reason">>, Frame, <<"llm_error">>),
    ?SLOG(error, #{msg => "pipeline_llm_error", iid => Data#data.iid, reason => Reason}),
    do_fail(Data, Reason);
%% ── llm_loop: cap/reply arrives for an outstanding tool call ─────────────────

handle_event(cast, #cap_reply{req_id = ReqId, frame = Frame}, llm_loop, Data) ->
    log_received(cap_reply, Data, #{req_id => ReqId, frame => Frame}),
    route_cap_reply_to_session(ReqId, Frame, Data);
%% ── waiting_cap: cap/reply for the call_skill step ───────────────────────────

handle_event(
    cast,
    #cap_reply{req_id = ReqId, frame = Frame},
    waiting_cap,
    #data{cap_req_id = ReqId} = Data
) ->
    log_received(cap_reply, Data, #{req_id => ReqId, frame => Frame}),
    Result = maps:get(<<"data">>, Frame, #{}),
    Data1 = write_context(Data#data.cap_result_path, Result, Data),
    Data2 = Data1#data{cap_req_id = undefined, cap_result_path = undefined},
    ?SLOG(info, #{
        msg => "pipeline_call_skill_done",
        iid => Data#data.iid,
        step => maps:get(<<"id">>, current_step(Data), <<"?">>)
    }),
    advance_and_step(Data2);
%% ── waiting_event: an external event arrived ─────────────────────────────────

handle_event(cast, #pipe_evt{topic = Topic, event = Event}, waiting_event, Data) ->
    log_received(evt, Data, #{topic => Topic, event => Event}),
    handle_waited_event(Topic, Event, Data);
%% ── done: idle timer fired ────────────────────────────────────────────────────

handle_event(info, idle_timeout, done, Data) ->
    ?SLOG(info, #{msg => "pipeline_idle_timeout_stop", iid => Data#data.iid}),
    {stop, normal, Data};
%% ── done: any cast — restart from step 0 ─────────────────────────────────────

handle_event(cast, Msg, done, Data) ->
    ?SLOG(info, #{msg => "pipeline_restarting_from_done", iid => Data#data.iid, trigger => Msg}),
    Data1 = cancel_idle_timer(Data),
    Data2 = Data1#data{step_idx = 0, pending_calls = #{}, active_sid = undefined},
    publish_pipeline_event(Data2, #{<<"type">> => <<"pipeline_started">>}),
    {next_state, running, Data2, [{next_event, internal, step}]};
%% ── catch-all ─────────────────────────────────────────────────────────────────

handle_event(_Type, _Content, _State, Data) ->
    {keep_state, Data}.

%%--------------------------------------------------------------------
%% Step execution
%%--------------------------------------------------------------------

execute_step(#{<<"type">> := <<"llm_loop">>} = Step, Data) ->
    start_llm_loop(Step, Data);
execute_step(#{<<"type">> := <<"call_skill">>} = Step, Data) ->
    invoke_call_skill(Step, Data);
execute_step(#{<<"type">> := <<"break">>} = Step, Data) ->
    maybe_break(Step, Data);
execute_step(#{<<"type">> := <<"wait_for_event">>} = Step, Data) ->
    enter_wait_event(Step, Data);
execute_step(Step, Data) ->
    ?SLOG(warning, #{
        msg => "pipeline_unknown_step_type",
        iid => Data#data.iid,
        type => maps:get(<<"type">>, Step, undefined)
    }),
    advance_and_step(Data).

%% -- llm_loop step ----------------------------------------------------------

start_llm_loop(Step, Data) ->
    ToolSpecs = maps:get(<<"tools">>, Step, []),
    {ToolManifest0, ToolMap0} = build_tool_manifest(ToolSpecs),
    SetResultSchema = maps:get(<<"set_result_schema">>, Step, undefined),
    {ToolManifest, ToolMap} = maybe_inject_set_result(ToolManifest0, ToolMap0, SetResultSchema),
    InputSpec = maps:get(<<"input">>, Step, #{}),
    Input = resolve_map(InputSpec, Data#data.context),
    SessionCfg = resolve_session_config(Step),
    StepId = maps:get(<<"id">>, Step, <<"llm">>),
    %% Stable Sid: same session is reused across every trigger of this pipeline
    %% step, allowing the model to accumulate full conversation history.
    Sid = stable_sid(Data#data.pipeline_id, StepId),
    _ = ensure_session(Sid),
    %% Default to ephemeral mode (stop_on_finish=true): each event gets a fresh
    %% session with no history from prior triggers. For persistent mode where the
    %% session accumulates conversation history across events, explicitly set
    %% "stop_on_finish": false in the step configuration.
    StopOnFinish = maps:get(<<"stop_on_finish">>, Step, true),
    Request = maps:merge(SessionCfg, #{
        <<"type">> => <<"request">>,
        <<"iid">> => Data#data.iid,
        <<"trace_id">> => Data#data.trace_id,
        <<"tools">> => ToolManifest,
        <<"input">> => Input,
        <<"stop_on_finish">> => StopOnFinish
    }),
    publish_to_sess_in(Sid, Request),
    Data1 = Data#data{active_sid = Sid, tool_map = ToolMap, pending_calls = #{}},
    ?SLOG(info, #{
        msg => "pipeline_llm_loop_started",
        iid => Data#data.iid,
        sid => Sid,
        step => StepId
    }),
    {next_state, llm_loop, Data1}.

%% Stable session ID: pipeline_id + step_id, slashes replaced so the result
%% stays a single MQTT topic level.
stable_sid(PipelineId, StepId) ->
    Safe = binary:replace(PipelineId, <<"/">>, <<"-">>, [global]),
    <<Safe/binary, "-", StepId/binary>>.

%% Start the session process if it is not already running.
ensure_session(Sid) ->
    case emqx_agent_session:whereis(Sid) of
        undefined -> emqx_agent_sess_sup:start_session(Sid);
        _Pid -> ok
    end.

%% -- call_skill step --------------------------------------------------------

invoke_call_skill(Step, Data) ->
    SkillSpec = maps:get(<<"skill">>, Step),
    Args = resolve_map(maps:get(<<"args">>, Step, #{}), Data#data.context),
    ResultPath = maps:get(<<"result_path">>, Step, undefined),
    {Type, SkillId} = parse_tool_spec(SkillSpec),
    ReqId = gen_req_id(),
    publish_cap_invoke(Type, SkillId, #{
        <<"req_id">> => ReqId,
        <<"iid">> => Data#data.iid,
        <<"trace_id">> => Data#data.trace_id,
        <<"args">> => Args
    }),
    Data1 = Data#data{cap_req_id = ReqId, cap_result_path = ResultPath},
    ?SLOG(info, #{
        msg => "pipeline_call_skill_invoked",
        iid => Data#data.iid,
        skill => SkillSpec,
        req_id => ReqId
    }),
    {next_state, waiting_cap, Data1}.

%% -- break step -------------------------------------------------------------

maybe_break(Step, Data) ->
    Path = maps:get(<<"path">>, Step, undefined),
    Negate = maps:get(<<"not">>, Step, false) =:= true,
    EqValue = maps:get(<<"eq">>, Step, undefined),
    Value = resolve_context_path(Path, Data#data.context),
    IsTrue =
        case EqValue of
            undefined -> Value =:= true;
            Eq -> Value =:= Eq
        end,
    ShouldBreak =
        case Negate of
            true -> not IsTrue;
            false -> IsTrue
        end,
    ?SLOG(info, #{
        msg => "pipeline_break_evaluated",
        iid => Data#data.iid,
        path => Path,
        'not' => Negate,
        value => Value,
        should_break => ShouldBreak
    }),
    case ShouldBreak of
        true -> do_complete(Data);
        false -> advance_and_step(Data)
    end.

%% -- wait_for_event step ----------------------------------------------------

enter_wait_event(Step, Data) ->
    Topic = maps:get(<<"topic">>, Step),
    Where = maps:get(<<"where">>, Step, undefined),
    ResultPath = maps:get(<<"result_path">>, Step, undefined),
    emqx_agent_pipeline_mgr:register_waiting(Data#data.iid, Topic),
    Data1 = Data#data{
        wait_topic = Topic,
        wait_where = Where,
        wait_result_path = ResultPath
    },
    ?SLOG(info, #{
        msg => "pipeline_waiting_for_event",
        iid => Data#data.iid,
        topic => Topic
    }),
    {next_state, waiting_event, Data1}.

%%--------------------------------------------------------------------
%% Tool request proxying (llm_loop ↔ skills)
%%--------------------------------------------------------------------

dispatch_tool_request(Frame, Data) ->
    CallId = maps:get(<<"call_id">>, Frame),
    ToolName = maps:get(<<"tool">>, Frame),
    Args = maps:get(<<"args">>, Frame, #{}),
    case maps:get(ToolName, Data#data.tool_map, undefined) of
        undefined ->
            ?SLOG(warning, #{
                msg => "pipeline_unknown_tool",
                iid => Data#data.iid,
                tool => ToolName
            }),
            send_tool_result(Data#data.active_sid, CallId, false, #{
                <<"error">> => <<"unknown_tool">>
            }),
            {keep_state, Data};
        {<<"pipeline">>, <<"set_result">>} ->
            %% Inline tool — store args and acknowledge immediately.
            ?SLOG(info, #{msg => "pipeline_set_result_called", iid => Data#data.iid, args => Args}),
            send_tool_result(Data#data.active_sid, CallId, true, #{<<"ok">> => true}),
            {keep_state, Data#data{set_result_value = Args}};
        {Type, SkillId} ->
            ReqId = gen_req_id(),
            publish_cap_invoke(Type, SkillId, #{
                <<"req_id">> => ReqId,
                <<"iid">> => Data#data.iid,
                <<"trace_id">> => Data#data.trace_id,
                <<"sid">> => Data#data.active_sid,
                <<"call_id">> => CallId,
                <<"args">> => Args
            }),
            Pending1 = maps:put(ReqId, CallId, Data#data.pending_calls),
            {keep_state, Data#data{pending_calls = Pending1}}
    end.

route_cap_reply_to_session(ReqId, Frame, Data) ->
    case maps:get(ReqId, Data#data.pending_calls, undefined) of
        undefined ->
            %% Stale or unrecognised reply — ignore
            {keep_state, Data};
        CallId ->
            Ok = maps:get(<<"ok">>, Frame, true),
            Result = maps:get(<<"data">>, Frame, #{}),
            send_tool_result(Data#data.active_sid, CallId, Ok, Result),
            Pending1 = maps:remove(ReqId, Data#data.pending_calls),
            {keep_state, Data#data{pending_calls = Pending1}}
    end.

%%--------------------------------------------------------------------
%% Wait-for-event matching
%%--------------------------------------------------------------------

handle_waited_event(Topic, Event, Data) ->
    WaitTopic = Data#data.wait_topic,
    case emqx_topic:match(Topic, WaitTopic) of
        false ->
            {keep_state, Data};
        true ->
            case eval_where(Data#data.wait_where, Event, Data#data.context) of
                false ->
                    {keep_state, Data};
                true ->
                    emqx_agent_pipeline_mgr:unregister_waiting(Data#data.iid),
                    Data1 = write_context(Data#data.wait_result_path, Event, Data),
                    Data2 = Data1#data{
                        wait_topic = undefined,
                        wait_where = undefined,
                        wait_result_path = undefined
                    },
                    ?SLOG(info, #{
                        msg => "pipeline_waited_event_received",
                        iid => Data#data.iid,
                        topic => Topic
                    }),
                    advance_and_step(Data2)
            end
    end.

%%--------------------------------------------------------------------
%% Completion / failure helpers
%%--------------------------------------------------------------------

do_complete(Data) ->
    ?SLOG(info, #{
        msg => "pipeline_completed",
        pipeline_id => Data#data.pipeline_id,
        iid => Data#data.iid
    }),
    publish_pipeline_event(Data, #{<<"type">> => <<"pipeline_completed">>}),
    {next_state, done, enter_done(Data)}.

do_fail(Data, Reason) ->
    ReasonBin = iolist_to_binary(io_lib:format("~0p", [Reason])),
    ?SLOG(error, #{
        msg => "pipeline_failed",
        pipeline_id => Data#data.pipeline_id,
        iid => Data#data.iid,
        reason => ReasonBin
    }),
    publish_pipeline_event(Data, #{
        <<"type">> => <<"pipeline_failed">>,
        <<"reason">> => ReasonBin
    }),
    {next_state, done, enter_done(Data)}.

enter_done(Data) ->
    Ref = erlang:send_after(?IDLE_TIMEOUT_MS, self(), idle_timeout),
    Data#data{idle_timer_ref = Ref}.

cancel_idle_timer(#data{idle_timer_ref = undefined} = Data) ->
    Data;
cancel_idle_timer(#data{idle_timer_ref = Ref} = Data) ->
    erlang:cancel_timer(Ref),
    Data#data{idle_timer_ref = undefined}.

advance_and_step(Data) ->
    Data1 = Data#data{step_idx = Data#data.step_idx + 1},
    {next_state, running, Data1, [{next_event, internal, step}]}.

current_step(#data{step_idx = Idx, steps = Steps}) ->
    lists:nth(Idx + 1, Steps).

%%--------------------------------------------------------------------
%% Context helpers — JSONPath-style read/write
%%--------------------------------------------------------------------

%% Write a value to a top-level context key.
%% "$.foo"       → context[<<"foo">>] = Value
%% "$.foo.bar"   → context[<<"foo.bar">>] = Value  (limitation: shallow write)
write_context(undefined, _Value, Data) ->
    Data;
write_context(<<"$.", Key/binary>>, Value, Data) ->
    %% For writing we only support single-segment paths.
    TopKey = hd(binary:split(Key, <<".">>)),
    Data#data{context = maps:put(TopKey, Value, Data#data.context)};
write_context(_Other, _Value, Data) ->
    Data.

%% Read a (possibly nested) value.
%% "$.foo"           → context[<<"foo">>]
%% "$.foo.bar.baz"   → context[<<"foo">>][<<"bar">>][<<"baz">>]
resolve_context_path(<<"$.", Path/binary>>, Context) ->
    Parts = binary:split(Path, <<".">>, [global]),
    deep_get(Parts, Context);
resolve_context_path(Value, _Context) ->
    Value.

%% Recursively resolve all binary values in a map that start with "$".
resolve_map(Map, Context) when is_map(Map) ->
    maps:map(
        fun
            (_K, V) when is_binary(V) -> resolve_context_path(V, Context);
            (_K, V) -> V
        end,
        Map
    );
resolve_map(Other, _Context) ->
    Other.

deep_get([], Value) ->
    Value;
deep_get([Key | Rest], Map) when is_map(Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> null;
        V -> deep_get(Rest, V)
    end;
deep_get(_, _) ->
    null.

%% Evaluate a simple "lhs == rhs" where clause.
%% LHS:  dotted path into the event  (e.g. "data.incident_id")
%% RHS:  context path starting with $ (e.g. "$.triage.result.incident_id")
%%        or a literal string.
eval_where(undefined, _Event, _Context) ->
    true;
eval_where(Where, Event, Context) ->
    case binary:split(Where, <<" == ">>) of
        [LhsStr, RhsStr] ->
            Lhs = deep_get(binary:split(string:trim(LhsStr), <<".">>, [global]), Event),
            Rhs = resolve_context_path(string:trim(RhsStr), Context),
            Lhs =:= Rhs;
        _ ->
            true
    end.

%%--------------------------------------------------------------------
%% Tool manifest building
%%--------------------------------------------------------------------

%% Inject the built-in set_result tool when the step declares a result schema.
maybe_inject_set_result(Manifest, ToolMap, undefined) ->
    {Manifest, ToolMap};
maybe_inject_set_result(Manifest, ToolMap, Schema) ->
    Entry = #{
        <<"name">> => <<"set_result">>,
        <<"description">> => <<"Submit the final structured result for this pipeline step.">>,
        <<"parameters">> => Schema
    },
    {[Entry | Manifest], maps:put(<<"set_result">>, {<<"pipeline">>, <<"set_result">>}, ToolMap)}.

%% Returns {ManifestList, ToolMap} where ManifestList is the OpenAI-format
%% tool list and ToolMap maps sanitised tool names → {Type, SkillId}.
build_tool_manifest(ToolSpecs) ->
    lists:foldl(
        fun(Spec, {ManAcc, MapAcc}) ->
            case parse_tool_spec(Spec) of
                undefined ->
                    {ManAcc, MapAcc};
                {Type, SkillId} ->
                    ToolName = sanitize_tool_name(Spec),
                    case emqx_agent_skill_registry:lookup(Type, SkillId) of
                        {ok, Skill} ->
                            Entry = #{
                                <<"name">> => ToolName,
                                <<"description">> => maps:get(description, Skill, Spec),
                                <<"parameters">> => maps:get(
                                    input_schema, Skill, #{<<"type">> => <<"object">>}
                                )
                            },
                            {[Entry | ManAcc], maps:put(ToolName, {Type, SkillId}, MapAcc)};
                        {error, not_found} ->
                            ?SLOG(warning, #{
                                msg => "pipeline_tool_spec_not_in_registry",
                                spec => Spec
                            }),
                            {ManAcc, MapAcc}
                    end
            end
        end,
        {[], #{}},
        ToolSpecs
    ).

%% "message.publish@slack-dev"  →  {<<"message.publish">>, <<"slack-dev">>}
parse_tool_spec(Spec) ->
    case binary:split(Spec, <<"@">>) of
        [Type, SkillId] -> {Type, SkillId};
        _ -> undefined
    end.

%% Replace any character outside [a-zA-Z0-9_-] with _, then truncate to 64
%% characters (OpenAI's hard limit on function names).  Truncation preserves
%% uniqueness within a single pipeline because the skill ID suffix is the part
%% most likely to differ across tools.
sanitize_tool_name(Name) ->
    Sanitized = <<<<(san(C))>> || <<C>> <= Name>>,
    case byte_size(Sanitized) of
        N when N =< 64 -> Sanitized;
        _ -> binary:part(Sanitized, byte_size(Sanitized) - 64, 64)
    end.

san(C) when C >= $a, C =< $z -> C;
san(C) when C >= $A, C =< $Z -> C;
san(C) when C >= $0, C =< $9 -> C;
san($-) -> $-;
san(_) -> $_.

%%--------------------------------------------------------------------
%% Session config resolution
%%--------------------------------------------------------------------

%% Inline config takes precedence over a named profile.
%% Keys expected: api_key, base_url, model, instructions, output_schema.
resolve_session_config(Step) ->
    case maps:get(<<"session_config">>, Step, undefined) of
        Cfg when is_map(Cfg) ->
            Cfg;
        undefined ->
            ProfileName = maps:get(<<"session_profile">>, Step, <<"default">>),
            case emqx_agent_pipeline_registry:lookup_profile(ProfileName) of
                {ok, Cfg} ->
                    Cfg;
                {error, not_found} ->
                    ?SLOG(warning, #{
                        msg => "pipeline_session_profile_not_found",
                        profile => ProfileName
                    }),
                    #{}
            end
    end.

%%--------------------------------------------------------------------
%% Publishing helpers
%%--------------------------------------------------------------------

publish_to_sess_in(Sid, Payload) ->
    Topic = <<"sess/in/", Sid/binary, "/">>,
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

publish_cap_invoke(Type, SkillId, Payload) ->
    Topic = <<"cap/invoke/", Type/binary, "/", SkillId/binary>>,
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

send_tool_result(Sid, CallId, Ok, ResultData) ->
    Payload = #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"ok">> => Ok,
        <<"data">> => ResultData
    },
    publish_to_sess_in(Sid, Payload).

publish_pipeline_event(
    #data{pipeline_id = PipelineId, iid = Iid, trace_id = TraceId, context = Ctx},
    Frame
) ->
    Topic = <<"pipe/", PipelineId/binary, "/inst/", Iid/binary, "/events">>,
    Payload = maps:merge(
        #{
            <<"pipeline_id">> => PipelineId,
            <<"iid">> => Iid,
            <<"trace_id">> => TraceId,
            <<"context">> => Ctx
        },
        Frame
    ),
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

log_received(Kind, #data{iid = Iid}, Payload) ->
    ?SLOG(warning, #{msg => "pipeline_received", iid => Iid, kind => Kind, payload => Payload}).

%%--------------------------------------------------------------------
%% ID generation
%%--------------------------------------------------------------------

%% IID is a slash-free identifier: must not split MQTT topic levels.
gen_iid(_PipelineId) ->
    Seq = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<"inst-", Seq/binary>>.

gen_req_id() ->
    Seq = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<"preq-", Seq/binary>>.
