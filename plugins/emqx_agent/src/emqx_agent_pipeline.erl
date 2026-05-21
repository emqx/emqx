%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Pipeline instance gen_statem.
%%
%% One process per trigger. Pipeline instances are one-off and terminate after
%% completion or failure.
%%
%% Lifecycle
%%   start_link/2 is called by emqx_agent_pipeline_sup (simple_one_for_one).
%%   The iid is generated here, ensuring uniqueness.
%%
%% States
%%   running        — executing a deterministic step
%%   llm_loop       — active LLM session; proxying tool_request ↔ $cap/
%%   waiting_cap    — $cap/ request sent for a call_skill step; awaiting cap_reply
%%
%% Incoming OTP messages (all sent as gen_statem casts by emqx_agent_pipeline_mgr)
%%   #sess_frame{sid, frame}   — from $sess/out/<sid>/
%%   #cap_reply{req_id, frame} — from $cap/<type>/<id>/response/<req_id>
%%
%% Context and JSONPath
%%   The pipeline maintains a `context` map.  Reading uses dotted paths
%%   starting with $ (e.g. "$.triage.result.incident_id" traverses the map
%%   recursively).  Writing supports top-level paths only ("$.triage" writes
%%   to context[<<"triage">>]).
%%
%% Tool specs
%%   Format:  "<type>@<skill_id>"  e.g. "message__publish@slack-dev"
%%   The type becomes the $cap/<type> topic segment.
%%   The tool name sent to the LLM is the spec with non-[a-zA-Z0-9_-] replaced
%%   by underscore (e.g. "message_publish_slack_dev").
%%
%% AI provider
%%   Each llm_loop step references an AI provider configured in
%%   emqx_ai_completion via provider_name.

-module(emqx_agent_pipeline).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_agent_pipeline.hrl").

%% Public API
-export([start_link/2, match_triggers/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-define(CAP_REPLY_TIMEOUT_MS, 60_000).
-define(LLM_REPLY_TIMEOUT_MS, 180_000).

-spec match_triggers(binary()) -> [map()].
match_triggers(Topic) ->
    [
        Pipeline
     || Pipeline <- emqx_agent_config:parsed_config([pipelines], []),
        trigger_matches(Topic, Pipeline)
    ].

-record(data, {
    pipeline_id :: binary(),
    iid :: binary(),
    key :: binary(),
    key_base62 :: binary(),
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
    %% topics subscribed by this pipeline process while awaiting replies
    reply_topics = [] :: [binary()],
    %% active reply timeout reference
    reply_timer_ref :: reference() | undefined
}).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec start_link(map(), map()) -> {ok, pid()} | {error, term()} | ignore.
start_link(Def, TriggerInput) ->
    PipelineId = maps:get(<<"pipeline_id">>, Def),
    Iid = gen_iid(PipelineId),
    % ct:print("start_link: ~p:~p~n~p", [PipelineId, Iid, Def]),
    gen_statem:start_link(?MODULE, {Iid, Def, TriggerInput}, []).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init({Iid, Def, TriggerInput}) ->
    PipelineId = maps:get(<<"pipeline_id">>, Def),
    case pipeline_key(Def, TriggerInput) of
        {ok, Key} ->
            Event = maps:get(event, TriggerInput, #{}),
            TraceId = maps:get(<<"trace_id">>, Event, Iid),
            KeyBase62 = emqx_base62:encode(Key),
            Steps = maps:get(<<"steps">>, Def, []),
            Data = #data{
                pipeline_id = PipelineId,
                iid = Iid,
                key = Key,
                key_base62 = KeyBase62,
                trace_id = TraceId,
                definition = Def,
                steps = Steps,
                step_idx = 0,
                context = emqx_agent_pipeline_ctx:init(Event, Key),
                active_sid = undefined,
                tool_map = #{},
                pending_calls = #{},
                cap_req_id = undefined,
                cap_result_path = undefined,
                reply_topics = [],
                reply_timer_ref = undefined
            },
            ?SLOG(info, #{
                msg => "pipeline_started",
                pipeline_id => PipelineId,
                iid => Iid,
                key => Key
            }),
            publish_pipeline_event(Data, #{<<"type">> => <<"pipeline_started">>}),
            {ok, running, Data, [{next_event, internal, step}]};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "pipeline_key_expression_error",
                pipeline_id => PipelineId,
                iid => Iid,
                reason => Reason
            }),
            {stop, Reason}
    end.

terminate(Reason, _State, Data) ->
    _ = cleanup_reply_wait(Data),
    do_log_terminate(Reason, Data).

do_log_terminate(Reason, #data{pipeline_id = PipelineId, iid = Iid}) ->
    ?SLOG(info, #{
        msg => "pipeline_terminating",
        pipeline_id => PipelineId,
        iid => Iid,
        reason => Reason
    }),
    ok.

trigger_matches(Topic, Pipeline) ->
    case maps:get(<<"topic">>, maps:get(<<"trigger">>, Pipeline, #{}), undefined) of
        undefined -> false;
        Filter -> emqx_topic:match(Topic, Filter)
    end.

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
    handle_llm_tool_request(Sid, Frame, Data);
%% ── llm_loop: session emitted final ──────────────────────────────────────────

handle_event(
    cast,
    #sess_frame{sid = Sid, frame = #{<<"type">> := <<"final">>} = Frame},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    handle_llm_final(Sid, Frame, Data);
%% ── llm_loop: session emitted error ──────────────────────────────────────────

handle_event(
    cast,
    #sess_frame{sid = Sid, frame = #{<<"type">> := <<"error">>} = Frame},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    handle_llm_error(Sid, Frame, Data);
%% ── llm_loop: cap response arrives for an outstanding tool call ──────────────

handle_event(cast, #cap_reply{req_id = ReqId, frame = Frame}, llm_loop, Data) ->
    handle_llm_cap_reply(ReqId, Frame, Data);
%% ── waiting_cap: cap response for the call_skill step ────────────────────────

handle_event(
    cast,
    #cap_reply{req_id = ReqId, frame = Frame},
    waiting_cap,
    #data{cap_req_id = ReqId} = Data
) ->
    handle_waiting_cap_reply(ReqId, Frame, Data);
%% ── reply deliveries owned by this pipeline process ─────────────────────────

handle_event(
    info,
    #deliver{topic = Topic, message = #message{payload = Payload}},
    llm_loop,
    #data{active_sid = Sid} = Data
) ->
    case Topic =:= sess_out_topic(Sid) of
        true ->
            handle_sess_delivery(Sid, Payload, Data);
        false ->
            handle_cap_delivery(Topic, Payload, llm_loop, Data)
    end;
handle_event(
    info,
    #deliver{topic = Topic, message = #message{payload = Payload}},
    waiting_cap,
    Data
) ->
    handle_cap_delivery(Topic, Payload, waiting_cap, Data);
handle_event(info, #deliver{}, _State, Data) ->
    {keep_state, Data};
%% ── reply wait timeout ──────────────────────────────────────────────────────

handle_event(info, {pipeline_reply_timeout, Kind}, _State, Data) ->
    do_timeout_fail(Data, Kind);
%% ── catch-all ─────────────────────────────────────────────────────────────────

handle_event(_Type, _Content, _State, Data) ->
    {keep_state, Data}.

%%--------------------------------------------------------------------
%% Step execution
%%--------------------------------------------------------------------

execute_step(Step, Data) ->
    do_execute_step(Step, Data).

do_execute_step(#{<<"type">> := <<"llm_loop">>} = Step, Data) ->
    start_llm_loop(Step, Data);
do_execute_step(#{<<"type">> := <<"call_skill">>} = Step, Data) ->
    invoke_call_skill(Step, Data);
do_execute_step(#{<<"type">> := <<"break">>} = Step, Data) ->
    maybe_break(Step, Data);
do_execute_step(Step, Data) ->
    ?SLOG(warning, #{
        msg => "pipeline_unknown_step_type",
        iid => Data#data.iid,
        type => maps:get(<<"type">>, Step, undefined)
    }),
    advance_and_step(Data).

%% -- llm_loop step ----------------------------------------------------------

start_llm_loop(
    #{<<"id">> := StepId, <<"provider_name">> := ProviderName, <<"model">> := Model} = Step,
    Data
) ->
    Instructions = maps:get(<<"instructions">>, Step, <<"You are a helpful assistant.">>),
    ToolSpecs = maps:get(<<"tools">>, Step, []),
    InputSpec = maps:get(<<"input">>, Step, #{}),
    Persistent = maps:get(<<"persistent">>, Step, false),
    MaxTokens = maps:get(<<"max_tokens">>, Step, 2048),
    MaxTotalTokens = maps:get(<<"max_total_tokens">>, Step, 50000),
    SetResultSchema = maps:get(<<"set_result_schema">>, Step, undefined),
    {ToolManifest0, ToolMap0} = build_tool_manifest(ToolSpecs),
    {ToolManifest, ToolMap} = maybe_inject_set_result(ToolManifest0, ToolMap0, SetResultSchema),
    Input = emqx_agent_pipeline_ctx:resolve_map(InputSpec, Data#data.context),
    Sid = llm_sid(Persistent, StepId, Data),
    SessOutTopic = sess_out_topic(Sid),
    Data0 = subscribe_reply_topic(SessOutTopic, Data),
    Request = #{
        <<"type">> => <<"request">>,
        <<"iid">> => Data0#data.iid,
        <<"trace_id">> => Data0#data.trace_id,
        <<"provider_name">> => ProviderName,
        <<"tools">> => ToolManifest,
        <<"input">> => Input,
        <<"model">> => Model,
        <<"instructions">> => Instructions,
        <<"persistent">> => Persistent,
        <<"max_tokens">> => MaxTokens,
        <<"max_total_tokens">> => MaxTotalTokens
    },
    publish_to_sess_in(Sid, Request),
    Data1 = start_reply_timer(
        llm_reply_timeout,
        step_timeout_ms(Step, ?LLM_REPLY_TIMEOUT_MS),
        Data0#data{active_sid = Sid, tool_map = ToolMap, pending_calls = #{}}
    ),
    ?SLOG(info, #{
        msg => "pipeline_llm_loop_started",
        iid => Data0#data.iid,
        sid => Sid,
        step => StepId
    }),
    {next_state, llm_loop, Data1}.

llm_sid(false, StepId, #data{iid = Iid}) ->
    <<"pipe-", (emqx_base62:encode(<<Iid/binary, 0, StepId/binary>>))/binary>>;
llm_sid(true, StepId, #data{pipeline_id = PipelineId, key = Key}) ->
    <<"pipe-",
        (emqx_base62:encode(<<PipelineId/binary, 0, StepId/binary, 0, Key/binary>>))/binary>>.

%% -- call_skill step --------------------------------------------------------

invoke_call_skill(Step, Data) ->
    SkillSpec = maps:get(<<"skill">>, Step),
    Args = emqx_agent_pipeline_ctx:resolve_map(maps:get(<<"args">>, Step, #{}), Data#data.context),
    ResultPath = maps:get(<<"result_path">>, Step, undefined),
    {Type, SkillId} = parse_tool_spec(SkillSpec),
    ReqId = gen_req_id(),
    ReplyTopic = cap_response_topic(Type, SkillId, ReqId),
    Data0 = subscribe_reply_topic(ReplyTopic, Data),
    publish_cap_invoke(Type, SkillId, #{
        <<"req_id">> => ReqId,
        <<"iid">> => Data0#data.iid,
        <<"trace_id">> => Data0#data.trace_id,
        <<"args">> => Args
    }),
    Data1 = start_reply_timer(
        cap_reply_timeout,
        step_timeout_ms(Step, ?CAP_REPLY_TIMEOUT_MS),
        Data0#data{cap_req_id = ReqId, cap_result_path = ResultPath}
    ),
    ?SLOG(info, #{
        msg => "pipeline_call_skill_invoked",
        iid => Data0#data.iid,
        skill => SkillSpec,
        req_id => ReqId
    }),
    {next_state, waiting_cap, Data1}.

%% -- break step -------------------------------------------------------------

maybe_break(Step, Data) ->
    Path = maps:get(<<"path">>, Step, undefined),
    Negate = maps:get(<<"not">>, Step, false) =:= true,
    EqValue = maps:get(<<"eq">>, Step, undefined),
    Value = emqx_agent_pipeline_ctx:resolve(Path, Data#data.context),
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

%%--------------------------------------------------------------------
%% Tool request proxying (llm_loop ↔ skills)
%%--------------------------------------------------------------------

handle_sess_delivery(Sid, Payload, Data) ->
    Frame = safe_decode(Payload),
    case Frame of
        #{<<"type">> := <<"tool_request">>} ->
            handle_llm_tool_request(Sid, Frame, Data);
        #{<<"type">> := <<"final">>} ->
            handle_llm_final(Sid, Frame, Data);
        #{<<"type">> := <<"error">>} ->
            handle_llm_error(Sid, Frame, Data);
        _ ->
            {keep_state, Data}
    end.

handle_cap_delivery(Topic, Payload, State, Data) ->
    Frame = safe_decode(Payload),
    ReqId = maps:get(<<"req_id">>, Frame, req_id_from_cap_response_topic(Topic)),
    WaitingReqId = Data#data.cap_req_id,
    case {State, ReqId} of
        {llm_loop, ReqId1} when is_binary(ReqId1) ->
            handle_llm_cap_reply(ReqId1, Frame, Data);
        {waiting_cap, WaitingReqId} ->
            handle_waiting_cap_reply(ReqId, Frame, Data);
        _ ->
            {keep_state, Data}
    end.

handle_llm_tool_request(Sid, Frame, Data) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    dispatch_tool_request(Frame, Data).

handle_llm_final(Sid, Frame, Data) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    Step = current_step(Data),
    Data0 = cleanup_reply_wait(Data),
    case {Data0#data.set_result_value, maps:is_key(<<"set_result_schema">>, Step)} of
        {undefined, true} ->
            ?SLOG(error, #{
                msg => "pipeline_llm_final_without_set_result",
                iid => Data0#data.iid,
                step => maps:get(<<"id">>, Step, <<"?">>),
                final_result => maps:get(<<"result">>, Frame, undefined),
                frame => Frame
            }),
            do_fail(Data0, missing_set_result);
        {SetResultValue, _} ->
            #{<<"result_path">> := ResultPath} = Step,
            Result =
                case SetResultValue of
                    undefined -> maps:get(<<"result">>, Frame, #{});
                    Val -> Val
                end,
            DataClean = cleanup_reply_wait(Data0),
            Data1 = DataClean#data{
                context = emqx_agent_pipeline_ctx:write(ResultPath, Result, DataClean#data.context)
            },
            Data2 = Data1#data{
                active_sid = undefined,
                tool_map = #{},
                pending_calls = #{},
                set_result_value = undefined
            },
            ?SLOG(info, #{
                msg => "pipeline_llm_step_done",
                iid => Data0#data.iid,
                step => maps:get(<<"id">>, Step, <<"?">>)
            }),
            advance_and_step(Data2)
    end.

handle_llm_error(Sid, Frame, Data) ->
    log_received(sess_out, Data, #{sid => Sid, frame => Frame}),
    Reason = maps:get(<<"reason">>, Frame, <<"llm_error">>),
    ?SLOG(error, #{msg => "pipeline_llm_error", iid => Data#data.iid, reason => Reason}),
    do_fail(cleanup_reply_wait(Data), Reason).

handle_llm_cap_reply(ReqId, Frame, Data) ->
    log_received(cap_reply, Data, #{req_id => ReqId, frame => Frame}),
    route_cap_reply_to_session(ReqId, Frame, Data).

handle_waiting_cap_reply(ReqId, Frame, Data) ->
    log_received(cap_reply, Data, #{req_id => ReqId, frame => Frame}),
    Result = emqx_agent_skill_helpers:cap_response(Frame),
    Data0 = cleanup_reply_wait(Data),
    Data1 = Data0#data{
        context = emqx_agent_pipeline_ctx:write(
            Data0#data.cap_result_path, Result, Data0#data.context
        )
    },
    Data2 = Data1#data{cap_req_id = undefined, cap_result_path = undefined},
    ?SLOG(info, #{
        msg => "pipeline_call_skill_done",
        iid => Data0#data.iid,
        step => maps:get(<<"id">>, current_step(Data0), <<"?">>)
    }),
    advance_and_step(Data2).

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
            send_tool_result(Data#data.active_sid, CallId, #{
                <<"status">> => <<"error">>,
                <<"reason">> => <<"unknown_tool">>
            }),
            {keep_state, Data};
        {<<"pipeline">>, <<"set_result">>} ->
            %% Inline tool — store args and acknowledge immediately.
            ?SLOG(info, #{msg => "pipeline_set_result_called", iid => Data#data.iid, args => Args}),
            send_tool_result(Data#data.active_sid, CallId, #{<<"status">> => <<"ok">>}),
            {keep_state, Data#data{set_result_value = Args}};
        {Type, SkillId} ->
            ReqId = gen_req_id(),
            ReplyTopic = cap_response_topic(Type, SkillId, ReqId),
            Data0 = subscribe_reply_topic(ReplyTopic, Data),
            publish_cap_invoke(Type, SkillId, #{
                <<"req_id">> => ReqId,
                <<"iid">> => Data0#data.iid,
                <<"trace_id">> => Data0#data.trace_id,
                <<"sid">> => Data0#data.active_sid,
                <<"call_id">> => CallId,
                <<"args">> => Args
            }),
            Pending1 = maps:put(ReqId, CallId, Data0#data.pending_calls),
            TimeoutMs = step_timeout_ms(current_step(Data0), ?CAP_REPLY_TIMEOUT_MS),
            {keep_state,
                start_reply_timer(cap_reply_timeout, TimeoutMs, Data0#data{pending_calls = Pending1})}
    end.

route_cap_reply_to_session(ReqId, Frame, Data) ->
    case maps:get(ReqId, Data#data.pending_calls, undefined) of
        undefined ->
            %% Stale or unrecognised reply — ignore
            {keep_state, Data};
        CallId ->
            Result = emqx_agent_skill_helpers:cap_response(Frame),
            send_tool_result(Data#data.active_sid, CallId, Result),
            Pending1 = maps:remove(ReqId, Data#data.pending_calls),
            Data1 = unsubscribe_cap_response_topic(ReqId, Data#data{pending_calls = Pending1}),
            Data2 =
                case map_size(Pending1) of
                    0 ->
                        start_reply_timer(
                            llm_reply_timeout,
                            step_timeout_ms(current_step(Data1), ?LLM_REPLY_TIMEOUT_MS),
                            Data1
                        );
                    _ ->
                        start_reply_timer(
                            cap_reply_timeout,
                            step_timeout_ms(current_step(Data1), ?CAP_REPLY_TIMEOUT_MS),
                            Data1
                        )
                end,
            {keep_state, Data2}
    end.

%%--------------------------------------------------------------------
%% Completion / failure helpers
%%--------------------------------------------------------------------

do_complete(Data) ->
    Data0 = cleanup_reply_wait(Data),
    ?SLOG(info, #{
        msg => "pipeline_completed",
        pipeline_id => Data0#data.pipeline_id,
        iid => Data0#data.iid
    }),
    publish_pipeline_event(Data0, #{<<"type">> => <<"pipeline_completed">>}),
    {stop, normal, Data0}.

do_fail(Data, Reason) ->
    Data0 = cleanup_reply_wait(Data),
    ReasonBin = iolist_to_binary(io_lib:format("~0p", [Reason])),
    ?SLOG(error, #{
        msg => "pipeline_failed",
        pipeline_id => Data0#data.pipeline_id,
        iid => Data0#data.iid,
        reason => ReasonBin
    }),
    publish_pipeline_event(Data0, #{
        <<"type">> => <<"pipeline_failed">>,
        <<"reason">> => ReasonBin
    }),
    {stop, normal, Data0}.

do_timeout_fail(Data, Reason) ->
    Data0 = cleanup_reply_wait(Data),
    ReasonBin = iolist_to_binary(io_lib:format("~0p", [Reason])),
    ?SLOG(error, #{
        msg => "pipeline_reply_timeout",
        pipeline_id => Data0#data.pipeline_id,
        iid => Data0#data.iid,
        reason => ReasonBin
    }),
    publish_pipeline_event(Data0, #{
        <<"type">> => <<"pipeline_failed">>,
        <<"reason">> => ReasonBin
    }),
    {stop, {shutdown, Reason}, Data0}.

advance_and_step(Data) ->
    Data1 = Data#data{step_idx = Data#data.step_idx + 1},
    {next_state, running, Data1, [{next_event, internal, step}]}.

current_step(#data{step_idx = Idx, steps = Steps}) ->
    lists:nth(Idx + 1, Steps).

step_timeout_ms(Step, Default) ->
    case maps:get(<<"timeout_ms">>, Step, Default) of
        TimeoutMs when is_integer(TimeoutMs), TimeoutMs > 0 -> TimeoutMs;
        _ -> Default
    end.

pipeline_key(Def, TriggerInput) ->
    Expression = maps:get(<<"key_expression">>, Def, <<"message.topic">>),
    Bindings = #{message => message_to_map(maps:get(message, TriggerInput))},
    case emqx_variform:compile(Expression) of
        {ok, Compiled} ->
            case emqx_variform:render(Compiled, Bindings, #{eval_as_string => true}) of
                {ok, Key} -> {ok, Key};
                {error, Reason} -> {error, {render_key_expression, Reason}}
            end;
        {error, Reason} ->
            {error, {compile_key_expression, Reason}}
    end.

message_to_map(Message) ->
    convert_message([user_property, peername, peerhost], emqx_message:to_map(Message)).

convert_message(
    [user_property | Rest],
    #{headers := #{properties := #{'User-Property' := UserProperty}} = Headers} = Map
) ->
    convert_message(Rest, Map#{
        headers => Headers#{properties => #{'User-Property' => maps:from_list(UserProperty)}}
    });
convert_message(
    [peername | Rest], #{headers := #{peername := {_Host, _Port} = Peername} = Headers} = Map
) ->
    convert_message(Rest, Map#{headers => Headers#{peername => ntoa(Peername)}});
convert_message([peerhost | Rest], #{headers := #{peerhost := Peerhost} = Headers} = Map) ->
    convert_message(Rest, Map#{headers => Headers#{peerhost => ntoa(Peerhost)}});
convert_message(_, Map) ->
    Map.

ntoa(Addr) ->
    list_to_binary(emqx_utils:ntoa(Addr)).

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

%% "message__publish@slack-dev"  ->  {<<"message__publish">>, <<"slack-dev">>}
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
%% Publishing helpers
%%--------------------------------------------------------------------

safe_decode(Payload) ->
    try
        emqx_utils_json:decode(Payload)
    catch
        _:_ -> #{}
    end.

sess_out_topic(Sid) ->
    emqx_agent_topics:sess_out_topic(Sid).

cap_response_topic(Type, SkillId, ReqId) ->
    emqx_agent_topics:cap_response_topic(Type, SkillId, ReqId).

req_id_from_cap_response_topic(Topic) ->
    emqx_agent_topics:req_id_from_cap_response_topic(Topic).

subscribe_reply_topic(Topic, #data{reply_topics = Topics0} = Data) ->
    case lists:member(Topic, Topics0) of
        true ->
            Data;
        false ->
            ok = emqx:subscribe(Topic),
            Data#data{reply_topics = [Topic | Topics0]}
    end.

unsubscribe_reply_topic(Topic, #data{reply_topics = Topics0} = Data) ->
    case lists:member(Topic, Topics0) of
        true ->
            ok = emqx:unsubscribe(Topic),
            Data#data{reply_topics = lists:delete(Topic, Topics0)};
        false ->
            Data
    end.

unsubscribe_cap_response_topic(ReqId, #data{reply_topics = Topics} = Data) ->
    case [Topic || Topic <- Topics, req_id_from_cap_response_topic(Topic) =:= ReqId] of
        [] ->
            Data;
        [Topic | _] ->
            unsubscribe_reply_topic(Topic, Data)
    end.

cleanup_reply_wait(Data) ->
    Data1 = cancel_reply_timer(Data),
    lists:foreach(fun(Topic) -> ok = emqx:unsubscribe(Topic) end, Data1#data.reply_topics),
    Data1#data{reply_topics = []}.

start_reply_timer(Kind, TimeoutMs, Data) ->
    Data1 = cancel_reply_timer(Data),
    Ref = erlang:send_after(TimeoutMs, self(), {pipeline_reply_timeout, Kind}),
    Data1#data{reply_timer_ref = Ref}.

cancel_reply_timer(#data{reply_timer_ref = undefined} = Data) ->
    Data;
cancel_reply_timer(#data{reply_timer_ref = Ref} = Data) ->
    _ = erlang:cancel_timer(Ref, [{async, false}, {info, false}]),
    Data#data{reply_timer_ref = undefined}.

publish_to_sess_in(Sid, Payload) ->
    Topic = emqx_agent_topics:sess_in_topic(Sid),
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

publish_cap_invoke(Type, SkillId, PayloadMap) ->
    ReqId = maps:get(<<"req_id">>, PayloadMap),
    Topic = emqx_agent_topics:cap_request_topic(Type, SkillId, ReqId),
    Msg = emqx_message:make(
        ?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(maps:remove(<<"req_id">>, PayloadMap))
    ),
    _ = emqx_broker:publish(Msg),
    ok.

send_tool_result(Sid, CallId, Result) ->
    Payload = #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"response">> => Result
    },
    publish_to_sess_in(Sid, Payload).

publish_pipeline_event(
    #data{pipeline_id = PipelineId, iid = Iid, trace_id = TraceId, context = Ctx},
    Frame
) ->
    Topic = emqx_agent_topics:pipe_events_topic(PipelineId, Iid),
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
