%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Session gen_statem: one process per logical session, globally registered
%% so it is unique across the cluster.
%%
%% Incoming topic:  sess/in/<SID>/
%% Outgoing topic:  sess/out/<SID>/
%%
%% Message types on in-topic:
%%   request     — start an LLM reasoning loop; carries optional stop_on_finish (default true)
%%   tool_result — result of a previously requested tool call
%%   event       — new context to be merged into the next LLM turn
%%   stop        — explicitly terminate the session from outside
%%
%% Message types on out-topic:
%%   tool_request — session asks the pipeline to invoke a skill
%%   final        — loop finished; carries result + usage counters
%%
%% Architecture
%%   The gen_statem IS the loop.  Only the blocking LLM HTTP request is
%%   offloaded to a short-lived child process (spawn_monitor).  The child
%%   sends {llm_result, Tag, Result} back and exits.
%%
%% State machine:
%%
%%   initial_idle ──request──▶  calling_llm  ──tool_calls──▶  waiting_tools
%%                                   │                              │
%%                                   │◀──────── all results ────────┘
%%                                   │
%%                                stop/no pending, stop_on_finish=true  ──▶  {stop, normal}
%%                                stop/no pending, stop_on_finish=false ──▶  idle
%%                                stop/pending                          ──▶  calling_llm (extend)
%%
%%   idle ──event──▶    calling_llm  (continue reasoning with accumulated context)
%%   idle ──request──▶  calling_llm  (append new input to history, refresh tools/system)
%%
%%   A request arriving in calling_llm or waiting_tools is queued in
%%   `queued_request` and replayed as an idle-state event when the current
%%   turn completes (only the latest request is retained).
%%
%% Events in calling_llm / waiting_tools are buffered in `pending` and flushed
%% on the next transition to calling_llm.
%% Events in initial_idle are buffered and folded in when the first request arrives.

-module(emqx_agent_session).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("common_test/include/ct.hrl").

%% Public API
-export([start_link/1, whereis/1]).

%% Hook management (called from emqx_agent_app)
-export([init_hook/0, deinit_hook/0, on_message_publish/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-ifdef(TEST).
-export([init_stream_acc/0, test_feed_sse/2, test_stream_chunks/1]).
-endif.

-define(NAME(Sid), {?MODULE, Sid}).
-define(REG(Sid), {global, ?NAME(Sid)}).
-define(OUT(Sid), <<"sess/out/", (Sid)/binary, "/">>).
-define(MAX_ITERATIONS, 20).

%%--------------------------------------------------------------------
%% Data (gen_statem "extended state")
%%--------------------------------------------------------------------

-record(data, {
    sid :: binary(),
    iid :: binary(),
    trace_id :: binary(),
    provider_name :: binary(),
    model :: binary(),
    tools :: [map()],
    output_schema :: map(),
    max_tokens = 2048 :: non_neg_integer(),
    recv_timeout_ms = 180000 :: non_neg_integer(),
    temperature = 0.1 :: number(),
    stream = true :: boolean(),
    tool_choice = <<"required">> :: binary() | map(),
    %% Growing OpenAI-format message list
    messages = [] :: [map()],
    %% Events buffered across all states; flushed before the next LLM call
    pending = [] :: [map()],
    %% Incoming request buffered when the session is busy (calling_llm /
    %% waiting_tools).  Processed as soon as the session returns to idle.
    queued_request = undefined :: undefined | map(),
    %% Accumulated usage counters
    usage = #{
        <<"iterations">> => 0,
        <<"tool_calls">> => 0,
        <<"tokens_in">> => 0,
        <<"tokens_out">> => 0
    } :: map(),
    %% When true (default), the session stops after publishing final.
    %% When false, it transitions to idle where events drive further reasoning.
    stop_on_finish = true :: boolean(),
    %% {Tag, MonRef} for the active LLM subprocess (state: calling_llm)
    llm_ref = undefined :: undefined | {reference(), reference()},
    %% Tool call ids still outstanding (state: waiting_tools)
    waiting_calls = [] :: [binary()],
    %% Collected OpenAI tool-role messages (state: waiting_tools)
    tool_result_msgs = [] :: [map()]
}).

%%--------------------------------------------------------------------
%% Hook management
%%--------------------------------------------------------------------

-spec init_hook() -> ok.
init_hook() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit_hook() -> ok.
deinit_hook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

on_message_publish(
    #message{topic = <<"sess/in/", Rest/binary>>, payload = Payload} = Message
) ->
    case binary:split(Rest, <<"/">>) of
        [Sid, <<>>] -> route(Sid, Payload);
        _ -> ok
    end,
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec start_link(binary()) -> {ok, pid()} | {error, term()}.
start_link(Sid) ->
    gen_statem:start_link(?REG(Sid), ?MODULE, Sid, []).

-spec whereis(binary()) -> pid() | undefined.
whereis(Sid) ->
    global:whereis_name(?NAME(Sid)).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init(Sid) ->
    ?SLOG(info, #{msg => "session_started", sid => Sid}),
    {ok, initial_idle, #data{sid = Sid}}.

terminate(_Reason, _State, #data{sid = Sid}) ->
    ?SLOG(info, #{msg => "session_terminating", sid => Sid}),
    ok.

%%--------------------------------------------------------------------
%% handle_event/4
%%
%% Clauses are ordered: specific state first, catch-alls last.
%%--------------------------------------------------------------------

%% ── initial_idle: accept the first (and only) request ────────────────────

handle_event(cast, {in, #{<<"type">> := <<"request">>} = Msg}, initial_idle, Data) ->
    #{
        <<"iid">> := Iid,
        <<"trace_id">> := TraceId,
        <<"provider_name">> := ProviderName,
        <<"model">> := Model,
        <<"tools">> := Tools,
        <<"input">> := Input,
        <<"instructions">> := Instructions
    } = Msg,
    OutputSchema = maps:get(<<"output_schema">>, Msg, undefined),
    SysMsg = #{<<"role">> => <<"system">>, <<"content">> => format_instructions(Instructions)},
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    %% Events buffered before the first request are folded in after the user message.
    PendingMsgs = [event_to_llm_msg(E) || E <- Data#data.pending],
    Data1 = Data#data{
        iid = Iid,
        trace_id = TraceId,
        provider_name = ProviderName,
        model = Model,
        tools = Tools,
        output_schema = OutputSchema,
        max_tokens = maps:get(<<"max_tokens">>, Msg, 2048),
        recv_timeout_ms = maps:get(<<"recv_timeout_ms">>, Msg, 180000),
        temperature = maps:get(<<"temperature">>, Msg, 0.1),
        stream = maps:get(<<"stream">>, Msg, true),
        tool_choice = maps:get(<<"tool_choice">>, Msg, <<"required">>),
        stop_on_finish = maps:get(<<"stop_on_finish">>, Msg, true),
        messages = [SysMsg, UserMsg] ++ PendingMsgs,
        pending = [],
        usage = #{
            <<"iterations">> => 0,
            <<"tool_calls">> => 0,
            <<"tokens_in">> => 0,
            <<"tokens_out">> => 0
        }
    },
    start_llm_call(Data1);
%% ── idle: new request — append input to history, optionally refresh config ───
%%
%% The session stays alive (stop_on_finish = false) so history accumulates
%% across triggers.  Each new pipeline run appends its input as the next user
%% message; the model sees the full prior conversation.

handle_event(cast, {in, #{<<"type">> := <<"request">>, <<"input">> := Input} = Msg}, idle, Data) ->
    %% Refresh mutable per-run fields: iid, trace_id, tools, tool_choice.
    %% Restoring tool_choice ensures the model is forced to issue a command
    %% on every new turn even if it was relaxed to "auto" after the previous
    %% tool call.
    Iid = maps:get(<<"iid">>, Msg, Data#data.iid),
    TraceId = maps:get(<<"trace_id">>, Msg, Data#data.trace_id),
    Tools = maps:get(<<"tools">>, Msg, Data#data.tools),
    ProviderName = maps:get(<<"provider_name">>, Msg, Data#data.provider_name),
    OutputSchema = maps:get(<<"output_schema">>, Msg, Data#data.output_schema),
    ToolChoice = maps:get(<<"tool_choice">>, Msg, Data#data.tool_choice),
    %% If instructions changed, replace the system message (always first).
    Messages0 =
        case maps:get(<<"instructions">>, Msg, undefined) of
            undefined ->
                Data#data.messages;
            NewInstructions ->
                SysMsg = #{
                    <<"role">> => <<"system">>,
                    <<"content">> => format_instructions(NewInstructions)
                },
                [SysMsg | tl(Data#data.messages)]
        end,
    %% Append the new user message so the model sees continuity.
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    Data1 = Data#data{
        iid = Iid,
        trace_id = TraceId,
        provider_name = ProviderName,
        tools = Tools,
        output_schema = OutputSchema,
        tool_choice = ToolChoice,
        messages = Messages0 ++ [UserMsg],
        stop_on_finish = maps:get(<<"stop_on_finish">>, Msg, false),
        pending = []
    },
    ?SLOG(info, #{
        msg => "session_continuing",
        sid => Data#data.sid,
        iid => Iid,
        history_len => length(Data1#data.messages)
    }),
    start_llm_call(Data1);
%% ── calling_llm: LLM subprocess finished ─────────────────────────────────

%% Child exits with {llm_result, Tag, Result} — normal completion path
handle_event(
    info,
    {'DOWN', Mon, process, _, {llm_result, Tag, Result}},
    calling_llm,
    #data{llm_ref = {Tag, Mon}} = Data
) ->
    Data1 = Data#data{llm_ref = undefined},
    case Result of
        {ok, Choice, TokensIn, TokensOut} ->
            Usage0 = Data1#data.usage,
            Data2 = Data1#data{
                usage = Usage0#{
                    <<"iterations">> => maps:get(<<"iterations">>, Usage0) + 1,
                    <<"tokens_in">> => maps:get(<<"tokens_in">>, Usage0) + TokensIn,
                    <<"tokens_out">> => maps:get(<<"tokens_out">>, Usage0) + TokensOut
                }
            },
            on_llm_choice(Choice, Data2);
        {error, Reason} ->
            ?SLOG(error, #{msg => "session_llm_error", sid => Data1#data.sid, reason => Reason}),
            {stop, {llm_error, Reason}, Data1}
    end;
%% Child exited with any other reason — unexpected crash
handle_event(
    info,
    {'DOWN', Mon, process, _, Reason},
    calling_llm,
    #data{llm_ref = {_Tag, Mon}} = Data
) ->
    ?SLOG(error, #{
        msg => "session_llm_process_crashed",
        sid => Data#data.sid,
        reason => Reason
    }),
    {stop, {llm_process_crashed, Reason}, Data};
%% ── waiting_tools: collect tool results ───────────────────────────────────

handle_event(
    cast,
    {in, #{<<"type">> := <<"tool_result">>, <<"call_id">> := CallId} = Msg},
    waiting_tools,
    Data
) ->
    on_tool_result(CallId, Msg, Data);
%% ── any state: explicit stop ──────────────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"stop">>}}, _State, Data) ->
    ?SLOG(info, #{msg => "session_stopped_explicitly", sid => Data#data.sid}),
    {stop, normal, Data};
%% ── idle + event: immediately continue reasoning ─────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"event">>} = Msg}, idle, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    ?SLOG(info, #{msg => "session_event_triggers_llm", sid => Data#data.sid}),
    Data1 = Data#data{messages = Data#data.messages ++ [event_to_llm_msg(Event)]},
    start_llm_call(Data1);
%% ── any other state: buffer events ───────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"event">>} = Msg}, _State, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    {keep_state, Data#data{pending = Data#data.pending ++ [Event]}};
%% Request arriving while the session is busy (calling_llm / waiting_tools):
%% buffer it so it can be applied once the session reaches idle.
%% Only the latest request is kept — earlier ones are superseded.
handle_event(cast, {in, #{<<"type">> := <<"request">>} = Msg}, _State, Data) ->
    ?SLOG(info, #{
        msg => "session_request_queued",
        sid => Data#data.sid,
        iid => maps:get(<<"iid">>, Msg, undefined)
    }),
    {keep_state, Data#data{queued_request = Msg}};
%% ── calls ─────────────────────────────────────────────────────────────────

handle_event({call, From}, _Req, _State, Data) ->
    {keep_state, Data, [{reply, From, {error, unknown_request}}]};
%% ── catch-all ─────────────────────────────────────────────────────────────

handle_event(_EventType, _EventContent, _State, Data) ->
    {keep_state, Data}.

%%--------------------------------------------------------------------
%% LLM subprocess
%%--------------------------------------------------------------------

start_llm_call(Data) ->
    case resolve_provider(Data#data.provider_name) of
        {error, Reason0} ->
            Reason = emqx_utils:readable_error_msg(Reason0),
            ?SLOG(error, #{
                msg => "session_provider_unavailable",
                sid => Data#data.sid,
                provider_name => Data#data.provider_name,
                reason => Reason0
            }),
            ok = publish(Data, #{<<"type">> => <<"error">>, <<"reason">> => Reason}),
            {stop, normal, Data};
        {ok, Provider} ->
            do_start_llm_call(Data, Provider)
    end.

resolve_provider(Name) ->
    case emqx_ai_completion_config:get_provider(Name) of
        {ok, #{type := openai} = Provider} -> {ok, Provider};
        {ok, #{type := Type}} -> {error, {unsupported_provider_type, Type}};
        not_found -> {error, {provider_not_found, Name}}
    end.

do_start_llm_call(Data, Provider) ->
    Tag = make_ref(),
    #{base_url := BaseUrl, api_key := ApiKeySecret} = Provider,
    ApiKey = emqx_secret:unwrap(ApiKeySecret),
    OpenAITools = [to_openai_tool(T) || T <- Data#data.tools],
    ToolNames = [
        maps:get(<<"name">>, maps:get(<<"function">>, T, #{}), <<"?">>)
     || T <- OpenAITools
    ],
    Messages = Data#data.messages,
    Model = Data#data.model,
    MaxTokens = Data#data.max_tokens,
    RecvTimeoutMs = provider_recv_timeout(Provider, Data#data.recv_timeout_ms),
    Temperature = Data#data.temperature,
    ToolChoice = Data#data.tool_choice,
    ?SLOG(warning, #{
        msg => "session_llm_request",
        sid => Data#data.sid,
        model => Model,
        messages => length(Messages),
        tools_count => length(OpenAITools),
        tools => ToolNames,
        max_tokens => MaxTokens,
        recv_timeout_ms => RecvTimeoutMs,
        temperature => Temperature,
        stream => true,
        tool_choice => ToolChoice
    }),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        Result = call_llm(
            Messages,
            OpenAITools,
            ApiKey,
            BaseUrl,
            Model,
            MaxTokens,
            RecvTimeoutMs,
            Temperature,
            ToolChoice,
            Data#data.output_schema,
            Data#data.sid,
            Data#data.iid,
            Data#data.trace_id,
            Data#data.usage
        ),
        exit({llm_result, Tag, Result})
    end),
    {next_state, calling_llm, Data#data{llm_ref = {Tag, MonRef}}}.

provider_recv_timeout(#{transport_options := #{recv_timeout := RecvTimeout}}, _Default) ->
    RecvTimeout;
provider_recv_timeout(_Provider, Default) ->
    Default.

%%--------------------------------------------------------------------
%% LLM choice handling
%%--------------------------------------------------------------------

on_llm_choice(Choice, Data) ->
    LLMMsg = maps:get(<<"message">>, Choice, #{}),
    FinishReason = maps:get(<<"finish_reason">>, Choice, <<"stop">>),
    ToolCalls = maps:get(<<"tool_calls">>, LLMMsg, []),
    Data1 = Data#data{messages = Data#data.messages ++ [LLMMsg]},

    case {FinishReason, ToolCalls} of
        {<<"tool_calls">>, [_ | _]} ->
            CallIds = [publish_tool_request(TC, Data1) || TC <- ToolCalls],
            Usage0 = Data1#data.usage,
            Data2 = Data1#data{
                usage = Usage0#{
                    <<"tool_calls">> => maps:get(<<"tool_calls">>, Usage0) + length(ToolCalls)
                },
                waiting_calls = CallIds,
                tool_result_msgs = []
            },
            {next_state, waiting_tools, Data2};
        _ ->
            on_reasoning_done(LLMMsg, FinishReason, Data1)
    end.

on_tool_result(CallId, Msg, Data) ->
    Ok = maps:get(<<"ok">>, Msg, true),
    ResultData = maps:get(<<"data">>, Msg, #{}),
    %% Build the tool-role acknowledgement.  Tool messages must carry a string
    %% content; image_url is not a valid tool-message content type for OpenAI.
    %% When the result contains an image we acknowledge the tool call with a
    %% plain string, then append a separate user-role message whose content is
    %% the multimodal array — that is the form gpt-4o accepts for vision input.
    {ToolContent, ExtraMsgs} =
        case
            Ok =:= true andalso
                try_extract_image_url(
                    maps:get(<<"payload">>, ResultData, undefined)
                )
        of
            {ok, ImageUrl} ->
                AckJson = emqx_utils_json:encode(
                    #{<<"ok">> => true, <<"data">> => <<"image received">>}
                ),
                VisionMsg = #{
                    <<"role">> => <<"user">>,
                    <<"content">> => [
                        #{
                            <<"type">> => <<"image_url">>,
                            <<"image_url">> => #{<<"url">> => ImageUrl}
                        }
                    ]
                },
                {AckJson, [VisionMsg]};
            _ ->
                {emqx_utils_json:encode(#{<<"ok">> => Ok, <<"data">> => ResultData}), []}
        end,
    ToolMsg = #{
        <<"role">> => <<"tool">>,
        <<"tool_call_id">> => CallId,
        <<"content">> => ToolContent
    },
    NewMsgs = [ToolMsg | ExtraMsgs],
    Waiting1 = lists:delete(CallId, Data#data.waiting_calls),
    Data1 = Data#data{
        waiting_calls = Waiting1,
        tool_result_msgs = Data#data.tool_result_msgs ++ NewMsgs
    },
    case Waiting1 of
        [_ | _] ->
            %% Still waiting for more results
            {keep_state, Data1};
        [] ->
            %% All results in; fold pending events and continue.
            %% Relax tool_choice after the first tool call so the model can
            %% output plain text ("done") to close the turn rather than being
            %% forced to call another tool.  Both a specific-function map and
            %% "required" are relaxed to "auto"; "none" and "auto" are unchanged.
            TC1 =
                case Data1#data.tool_choice of
                    TC when is_map(TC) -> <<"auto">>;
                    <<"required">> -> <<"auto">>;
                    TC -> TC
                end,
            Results = Data1#data.tool_result_msgs,
            EventMsgs = [event_to_llm_msg(E) || E <- Data1#data.pending],
            Data2 = Data1#data{
                messages = Data1#data.messages ++ Results ++ EventMsgs,
                tool_result_msgs = [],
                pending = [],
                tool_choice = TC1
            },
            maybe_next_llm_call(Data2)
    end.

%% LLM signalled it is done (no tool calls).
%% Restart with pending events if any; otherwise publish final and either
%% stop or return to idle depending on stop_on_finish.
on_reasoning_done(LLMMsg, FinishReason, Data) ->
    case Data#data.pending of
        [] ->
            Content = maps:get(<<"content">>, LLMMsg, <<"">>),
            publish(Data, #{
                <<"type">> => <<"final">>,
                <<"result">> => try_parse_result(Content),
                <<"finish_reason">> => FinishReason
            }),
            finish(Data);
        Events ->
            ?SLOG(info, #{
                msg => "session_restarting",
                sid => Data#data.sid,
                pending => length(Events)
            }),
            EventMsgs = [event_to_llm_msg(E) || E <- Events],
            Data1 = Data#data{messages = Data#data.messages ++ EventMsgs, pending = []},
            start_llm_call(Data1)
    end.

%% Guard against runaway tool-call loops.
maybe_next_llm_call(Data) ->
    Iter = maps:get(<<"iterations">>, Data#data.usage),
    case Iter >= ?MAX_ITERATIONS of
        true ->
            ?SLOG(warning, #{
                msg => "session_max_iterations_reached",
                sid => Data#data.sid,
                iterations => Iter
            }),
            publish(Data, #{
                <<"type">> => <<"final">>,
                <<"result">> => #{<<"error">> => <<"max_iterations_reached">>}
            }),
            finish(Data);
        false ->
            start_llm_call(Data)
    end.

%% After publishing final: stop or return to idle based on stop_on_finish.
%% If a request arrived while the session was busy, process it immediately
%% rather than sitting in idle waiting for the next cast.
finish(#data{stop_on_finish = true} = Data) ->
    {stop, normal, Data};
finish(#data{stop_on_finish = false, queued_request = undefined} = Data) ->
    ?SLOG(info, #{msg => "session_idle", sid => Data#data.sid}),
    {next_state, idle, Data};
finish(#data{stop_on_finish = false, queued_request = Queued} = Data) ->
    ?SLOG(info, #{msg => "session_draining_queued_request", sid => Data#data.sid}),
    Data1 = Data#data{queued_request = undefined},
    {next_state, idle, Data1, [{next_event, cast, {in, Queued}}]}.

%%--------------------------------------------------------------------
%% Routing
%%--------------------------------------------------------------------

route(Sid, Payload) ->
    Msg =
        try
            emqx_utils_json:decode(Payload)
        catch
            _:_ -> undefined
        end,
    case Msg of
        undefined ->
            ok;
        #{<<"type">> := <<"request">>} ->
            Pid = find_or_start(Sid),
            gen_statem:cast(Pid, {in, Msg});
        #{<<"type">> := _} ->
            case global:whereis_name(?NAME(Sid)) of
                undefined -> ok;
                Pid -> gen_statem:cast(Pid, {in, Msg})
            end
    end.

find_or_start(Sid) ->
    case global:whereis_name(?NAME(Sid)) of
        undefined ->
            case emqx_agent_sess_sup:start_session(Sid) of
                {ok, Pid} -> Pid;
                {error, {already_started, Pid}} -> Pid
            end;
        Pid ->
            Pid
    end.

%%--------------------------------------------------------------------
%% Tool request publishing
%%--------------------------------------------------------------------

publish_tool_request(ToolCall, Data) ->
    CallId = maps:get(<<"id">>, ToolCall),
    Function = maps:get(<<"function">>, ToolCall, #{}),
    ToolName0 = maps:get(<<"name">>, Function, undefined),
    ToolName =
        case ToolName0 of
            null -> tool_name_from_call_id(CallId);
            undefined -> tool_name_from_call_id(CallId);
            _ -> ToolName0
        end,
    Args =
        try
            emqx_utils_json:decode(maps:get(<<"arguments">>, Function, <<"{}">>))
        catch
            _:_ -> #{}
        end,
    publish(Data, #{
        <<"type">> => <<"tool_request">>,
        <<"call_id">> => CallId,
        <<"tool">> => ToolName,
        <<"args">> => Args
    }),
    CallId.

%%--------------------------------------------------------------------
%% LLM client (OpenAI-compatible) — runs in child process
%%--------------------------------------------------------------------

call_llm(
    Messages,
    Tools,
    ApiKey,
    BaseUrl,
    Model,
    MaxTokens,
    RecvTimeoutMs,
    Temperature,
    ToolChoice,
    OutputSchema,
    Sid,
    Iid,
    TraceId,
    Usage
) ->
    Url = <<BaseUrl/binary, "/chat/completions">>,
    Body0 = #{
        <<"model">> => Model,
        <<"messages">> => Messages,
        <<"max_completion_tokens">> => MaxTokens,
        <<"temperature">> => Temperature,
        <<"stream">> => true
    },
    %% Add response_format for structured output when an output schema is given.
    %% OpenAI accepts json_schema response_format alongside tools; the schema
    %% applies only to the text content of the final (non-tool-call) response.
    %% Only use json_schema response_format when the schema has explicit properties
    %% defined. A bare {type: object} is treated as "no constraint" and skipped
    %% so that non-OpenAI endpoints (e.g. Ollama) are not broken.
    Body1 =
        case OutputSchema of
            Schema when is_map_key(<<"properties">>, Schema) ->
                %% OpenAI strict mode requires additionalProperties: false
                StrictSchema = Schema#{<<"additionalProperties">> => false},
                Body0#{
                    <<"response_format">> => #{
                        <<"type">> => <<"json_schema">>,
                        <<"json_schema">> => #{
                            <<"name">> => <<"result">>,
                            <<"strict">> => true,
                            <<"schema">> => StrictSchema
                        }
                    }
                };
            _ ->
                Body0
        end,
    Body =
        case Tools of
            [] ->
                Body1;
            _ ->
                TCChoice =
                    case ToolChoice of
                        <<"required">> -> <<"required">>;
                        <<"auto">> -> <<"auto">>;
                        <<"none">> -> <<"none">>;
                        V when is_map(V) -> V;
                        _ -> <<"required">>
                    end,
                Body1#{<<"tools">> => Tools, <<"tool_choice">> => TCChoice}
        end,
    Headers = [
        {<<"authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"content-type">>, <<"application/json">>}
    ],
    % ct:print("stream_llm_body: url:~p, headers:~p", [Url, Headers]),
    stream_llm_response(
        Url,
        Headers,
        Body,
        RecvTimeoutMs,
        Model,
        Sid,
        Iid,
        TraceId,
        Usage
    ).

%%--------------------------------------------------------------------
%% LLM HTTP streaming — runs in the child process spawned by start_llm_call
%%--------------------------------------------------------------------

%% Entry point.  Adds stream_options, calls hackney, then drives the
%% SSE receive loop.
stream_llm_response(Url, Headers, Body0, RecvTimeoutMs, Model, Sid, Iid, TraceId, Usage) ->
    Body = Body0#{<<"stream_options">> => #{<<"include_usage">> => true}},
    Opts = [{connect_timeout, 30_000}, {recv_timeout, RecvTimeoutMs}],
    case hackney:request(post, Url, Headers, emqx_utils_json:encode(Body), Opts) of
        {ok, 200, _RespHdrs, ClientRef} ->
            % ct:print("stream_llm_response ok sid=~ts, headers=~p", [Sid, RespHdrs]),
            collect_stream(ClientRef, Model, Sid, Iid, TraceId, Usage);
        {ok, Status, _RespHdrs, ClientRef} ->
            ErrBody = drain_stream_body(ClientRef, <<>>),
            % ct:print("stream_http_err sid=~ts status=~p body=~p", [Sid, Status, ErrBody]),
            ?SLOG(error, #{
                msg => "session_llm_http_error",
                sid => Sid,
                status => Status
            }),
            {error, {http_error, Status, ErrBody}};
        {error, Reason} ->
            % ct:print("stream_request_err sid=~ts reason=~p", [Sid, Reason]),
            ?SLOG(error, #{
                msg => "session_llm_request_failed",
                sid => Sid,
                reason => Reason
            }),
            {error, {request_failed, Reason}}
    end.

collect_stream(ClientRef, Model, Sid, Iid, TraceId, Usage) ->
    case stream_receive_loop(ClientRef, <<>>, init_stream_acc(), Sid, Iid, TraceId, Usage) of
        {ok, Acc} ->
            finalize_stream_result(Acc, Model);
        {error, _} = Err ->
            Err
    end.

%% Receive loop: reads raw TCP chunks from hackney and feeds them into
%% the SSE buffer splitter.
stream_receive_loop(ClientRef, Buf, Acc, Sid, Iid, TraceId, Usage) ->
    case hackney:stream_body(ClientRef) of
        {ok, Chunk} ->
            % ct:print("stream_chunk: ~p", [Chunk]),
            Combined = <<Buf/binary, Chunk/binary>>,
            Normalized = binary:replace(Combined, <<"\r\n">>, <<"\n">>, [global]),
            {Buf1, Acc1} = feed_sse(Normalized, Acc, Sid, Iid, TraceId, Usage),
            stream_receive_loop(ClientRef, Buf1, Acc1, Sid, Iid, TraceId, Usage);
        done ->
            % ct:print("stream_done", []),
            %% Flush any remaining bytes in the buffer (edge case: server
            %% does not terminate the last event with \n\n).
            Acc1 = flush_sse_buf(Buf, Acc, Sid, Iid, TraceId, Usage),
            {ok, Acc1};
        {error, Reason} ->
            % ct:print("stream_error: ~p", [Reason]),
            ?SLOG(error, #{
                msg => "session_stream_recv_error",
                sid => Sid,
                reason => Reason
            }),
            {error, {stream_failed, Reason}}
    end.

%% Split the buffer on \n\n and process every complete SSE event.
%% Returns {UnprocessedRemainder, UpdatedAcc}.
feed_sse(Buf, Acc, Sid, Iid, TraceId, Usage) ->
    case binary:split(Buf, <<"\n\n">>, []) of
        [_Incomplete] ->
            %% No complete event yet — keep buffering.
            {Buf, Acc};
        [Event, Rest] ->
            Acc1 = apply_sse_event(Event, Acc, Sid, Iid, TraceId, Usage),
            feed_sse(Rest, Acc1, Sid, Iid, TraceId, Usage)
    end.

%% Called at stream end to process any remaining buffered bytes.
flush_sse_buf(<<>>, Acc, _Sid, _Iid, _TraceId, _Usage) ->
    Acc;
flush_sse_buf(Buf, Acc, Sid, Iid, TraceId, Usage) ->
    case iolist_to_binary(string:trim(Buf)) of
        <<>> -> Acc;
        Trimmed -> apply_sse_event(Trimmed, Acc, Sid, Iid, TraceId, Usage)
    end.

%% Parse one SSE event block (possibly multi-line: id/event/data/comment).
%% Extracts all "data: …" lines, joins them, then decodes JSON.
apply_sse_event(Event, Acc, Sid, Iid, TraceId, Usage) ->
    Lines = binary:split(Event, <<"\n">>, [global]),
    DataParts = lists:filtermap(
        fun(Line) ->
            case iolist_to_binary(string:trim(Line)) of
                <<"data:", Rest/binary>> -> {true, iolist_to_binary(string:trim(Rest))};
                _ -> false
            end
        end,
        Lines
    ),
    case DataParts of
        [] ->
            Acc;
        _ ->
            JsonBin = iolist_to_binary(lists:join(<<"\n">>, DataParts)),
            case JsonBin of
                <<"[DONE]">> ->
                    Acc;
                _ ->
                    apply_stream_json(JsonBin, Acc, Sid, Iid, TraceId, Usage)
            end
    end.

apply_stream_json(JsonBin, Acc, Sid, Iid, TraceId, Usage) ->
    case emqx_utils_json:safe_decode(JsonBin) of
        {ok, Chunk} when is_map(Chunk) ->
            apply_stream_chunk(Chunk, Acc, Sid, Iid, TraceId, Usage);
        {ok, _NonMap} ->
            Acc;
        {error, Reason} ->
            % ct:print(
            %     "stream_json_err sid=~ts bytes=~p reason=~p prefix=~p",
            %     [
            %         Sid,
            %         byte_size(JsonBin),
            %         Reason,
            %         binary:part(JsonBin, 0, min(80, byte_size(JsonBin)))
            %     ]
            % ),
            ?SLOG(warning, #{
                msg => "session_stream_json_error",
                sid => Sid,
                reason => Reason,
                bytes => byte_size(JsonBin)
            }),
            Acc
    end.

apply_stream_chunk(Chunk, Acc, Sid, Iid, TraceId, Usage) ->
    %% Extract usage tokens when present (arrives in a final chunk with
    %% choices:[] from OpenAI-compatible servers that set include_usage).
    Acc1 =
        case maps:get(<<"usage">>, Chunk, undefined) of
            U when is_map(U) ->
                Acc#{
                    tokens_in =>
                        maps:get(<<"prompt_tokens">>, U, maps:get(tokens_in, Acc)),
                    tokens_out =>
                        maps:get(<<"completion_tokens">>, U, maps:get(tokens_out, Acc))
                };
            _ ->
                Acc
        end,
    case maps:get(<<"choices">>, Chunk, []) of
        [Choice | _] -> apply_choice(Choice, Acc1, Sid, Iid, TraceId, Usage);
        _ -> Acc1
    end.

apply_choice(Choice, Acc, Sid, Iid, TraceId, Usage) ->
    %% Only update finish_reason when we receive a real value.
    %% Intermediate streaming chunks carry "finish_reason": null;
    %% we must NOT overwrite a previously accumulated real value with null.
    Acc1 =
        case maps:get(<<"finish_reason">>, Choice, undefined) of
            V when V =:= undefined; V =:= null -> Acc;
            FR -> Acc#{finish_reason => FR}
        end,
    %% delta is present in streaming responses; message in non-streaming.
    DeltaOrMsg = maps:get(<<"delta">>, Choice, maps:get(<<"message">>, Choice, undefined)),
    case DeltaOrMsg of
        D when is_map(D) -> apply_delta(D, Acc1, Sid, Iid, TraceId, Usage);
        _ -> Acc1
    end.

apply_delta(Delta, Acc, Sid, Iid, TraceId, Usage) ->
    %% Accumulate content fragments.
    Acc1 =
        case maps:get(<<"content">>, Delta, undefined) of
            C when is_binary(C), C =/= <<>> ->
                maybe_publish_stream_chunk(<<"content">>, C, Sid, Iid, TraceId, Usage),
                Acc#{content => append_bin(maps:get(content, Acc), C)};
            _ ->
                Acc
        end,
    %% Reasoning tokens are internal model thinking; accumulate but do not
    %% publish — they create O(N) MQTT traffic through pipeline hooks for no benefit.
    Acc2 = Acc1,
    %% Accumulate tool call delta fragments.
    case maps:get(<<"tool_calls">>, Delta, []) of
        TCs when is_list(TCs), TCs =/= [] ->
            TCs1 = lists:foldl(
                fun merge_tc_delta/2,
                maps:get(tool_calls, Acc2),
                TCs
            ),
            Acc2#{tool_calls => TCs1};
        _ ->
            Acc2
    end.

%% Merge one tool-call delta chunk into the accumulator map (keyed by index).
merge_tc_delta(TCDelta, ToolCallsMap) ->
    Index = maps:get(<<"index">>, TCDelta, 0),
    Prev = maps:get(Index, ToolCallsMap, #{}),
    maps:put(Index, deep_merge_tc(Prev, TCDelta), ToolCallsMap).

%% Deep-merge a streaming tool-call delta into the accumulated tool call.
%% The key invariant: argument strings are *concatenated*, not replaced.
deep_merge_tc(Prev, Delta) ->
    PrevFun = maps:get(<<"function">>, Prev, #{}),
    DeltaFun = maps:get(<<"function">>, Delta, #{}),
    PrevArgs = maps:get(<<"arguments">>, PrevFun, <<>>),
    DeltaArgs = maps:get(<<"arguments">>, DeltaFun, <<>>),
    Fun0 = maps:merge(PrevFun, DeltaFun#{
        <<"arguments">> => <<PrevArgs/binary, DeltaArgs/binary>>
    }),
    %% Prefer the first non-null name (arrives in the first chunk only).
    Fun1 =
        case
            prefer_non_null(
                maps:get(<<"name">>, DeltaFun, undefined),
                maps:get(<<"name">>, PrevFun, undefined)
            )
        of
            undefined -> Fun0;
            Name -> Fun0#{<<"name">> => Name}
        end,
    Merged0 = maps:merge(Prev, Delta#{<<"function">> => Fun1}),
    Merged1 =
        case
            prefer_non_null(
                maps:get(<<"id">>, Delta, undefined),
                maps:get(<<"id">>, Prev, undefined)
            )
        of
            undefined -> Merged0;
            Id -> Merged0#{<<"id">> => Id}
        end,
    case
        prefer_non_null(
            maps:get(<<"type">>, Delta, undefined),
            maps:get(<<"type">>, Prev, undefined)
        )
    of
        undefined -> Merged1;
        Type -> Merged1#{<<"type">> => Type}
    end.

prefer_non_null(Value, Fallback) when Value =:= undefined; Value =:= null ->
    Fallback;
prefer_non_null(Value, _Fallback) ->
    Value.

init_stream_acc() ->
    #{
        content => <<>>,
        tool_calls => #{},
        finish_reason => undefined,
        tokens_in => 0,
        tokens_out => 0
    }.

finalize_stream_result(Acc, Model) ->
    Content = maps:get(content, Acc),
    ToolCallsMap = maps:get(tool_calls, Acc),
    ToolCalls = [maps:get(I, ToolCallsMap) || I <- lists:sort(maps:keys(ToolCallsMap))],
    FinishReason =
        case maps:get(finish_reason, Acc) of
            undefined -> <<"stop">>;
            FR -> FR
        end,
    ?SLOG(info, #{
        msg => "session_llm_response",
        model => Model,
        finish_reason => FinishReason,
        tool_calls_count => length(ToolCalls),
        has_content => Content =/= <<>>,
        tokens_in => maps:get(tokens_in, Acc, 0),
        tokens_out => maps:get(tokens_out, Acc, 0)
    }),
    Msg =
        case ToolCalls of
            [] ->
                #{<<"role">> => <<"assistant">>, <<"content">> => Content};
            _ ->
                #{
                    <<"role">> => <<"assistant">>,
                    <<"content">> => Content,
                    <<"tool_calls">> => ToolCalls
                }
        end,
    {
        ok,
        #{<<"message">> => Msg, <<"finish_reason">> => FinishReason},
        maps:get(tokens_in, Acc, 0),
        maps:get(tokens_out, Acc, 0)
    }.

drain_stream_body(ClientRef, Acc) ->
    case hackney:stream_body(ClientRef) of
        {ok, Chunk} -> drain_stream_body(ClientRef, <<Acc/binary, Chunk/binary>>);
        done -> Acc;
        {error, _} -> Acc
    end.

tool_name_from_call_id(CallId) when is_binary(CallId) ->
    case CallId of
        <<"functions.", Rest/binary>> ->
            hd(binary:split(Rest, <<":">>, [global]));
        _ ->
            undefined
    end;
tool_name_from_call_id(_) ->
    undefined.

append_bin(A, B) when is_binary(A), is_binary(B) ->
    <<A/binary, B/binary>>;
append_bin(A, _B) ->
    A.

maybe_publish_stream_chunk(_ChunkType, Chunk, _Sid, _Iid, _TraceId, _Usage) when
    Chunk =:= undefined;
    Chunk =:= <<>>
->
    ok;
maybe_publish_stream_chunk(ChunkType, Chunk, Sid, Iid, TraceId, Usage) when is_binary(Chunk) ->
    publish_stream_frame(Sid, Iid, TraceId, Usage, #{
        <<"type">> => <<"intermediate">>,
        <<"chunk_type">> => ChunkType,
        <<"chunk">> => Chunk
    }).

publish_stream_frame(Sid, Iid, TraceId, Usage, Frame) ->
    Payload = maps:merge(
        #{
            <<"sid">> => Sid,
            <<"iid">> => Iid,
            <<"trace_id">> => TraceId,
            <<"usage">> => Usage
        },
        Frame
    ),
    Topic = ?OUT(Sid),
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

to_openai_tool(#{<<"name">> := Name, <<"description">> := Desc} = T) ->
    #{
        <<"type">> => <<"function">>,
        <<"function">> => #{
            <<"name">> => Name,
            <<"description">> => Desc,
            <<"parameters">> => maps:get(<<"parameters">>, T, #{<<"type">> => <<"object">>})
        }
    }.

event_to_llm_msg(Event) ->
    #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Event)}.

format_instructions(Instructions) when is_list(Instructions) ->
    iolist_to_binary(lists:join(<<"\n">>, Instructions));
format_instructions(Instructions) when is_binary(Instructions) ->
    Instructions.

%% Publish a frame to sess/out/<Sid>/, automatically filling correlation fields
%% (sid, iid, trace_id) and usage counters from Data.
publish(#data{sid = Sid, iid = Iid, trace_id = TraceId, usage = Usage} = _Data, Frame) ->
    Payload = maps:merge(
        #{
            <<"sid">> => Sid,
            <<"iid">> => Iid,
            <<"trace_id">> => TraceId,
            <<"usage">> => Usage
        },
        Frame
    ),
    Topic = ?OUT(Sid),
    Msg = emqx_message:make(?MODULE, ?QOS_0, Topic, emqx_utils_json:encode(Payload)),
    _ = emqx_broker:publish(Msg),
    ok.

%% Parse `Payload` (a binary JSON string) and return the image_url if present.
try_extract_image_url(Payload) when is_binary(Payload) ->
    case emqx_utils_json:safe_decode(Payload) of
        {ok, #{<<"image_url">> := <<"data:image/", _/binary>> = Url}} -> {ok, Url};
        _ -> error
    end;
try_extract_image_url(_) ->
    error.

try_parse_result(Content) when is_binary(Content) ->
    try
        emqx_utils_json:decode(Content)
    catch
        _:_ -> #{<<"summary">> => Content}
    end;
try_parse_result(Content) ->
    Content.

%%--------------------------------------------------------------------
%% Test helpers (exported only in TEST builds — see -export above)
%%--------------------------------------------------------------------

-ifdef(TEST).

-define(TEST_SID, <<"test-sid">>).
-define(TEST_IID, <<"test-iid">>).
-define(TEST_TRACE, <<"test-trace">>).

%% Feed a complete SSE byte sequence at once and return the final accumulator.
%% Use when the entire response is available as one binary.
test_feed_sse(Data, Acc) ->
    Normalized = binary:replace(Data, <<"\r\n">>, <<"\n">>, [global]),
    {Rest, Acc1} = feed_sse(Normalized, Acc, ?TEST_SID, ?TEST_IID, ?TEST_TRACE, #{}),
    flush_sse_buf(Rest, Acc1, ?TEST_SID, ?TEST_IID, ?TEST_TRACE, #{}).

%% Simulate chunked delivery: process a list of byte chunks in sequence,
%% threading the SSE buffer between calls (mirrors stream_receive_loop).
%% Returns the final accumulator after flushing any remaining buffer.
test_stream_chunks(Chunks) ->
    Acc0 = init_stream_acc(),
    {FinalBuf, Acc1} = lists:foldl(
        fun(Chunk, {Buf, A}) ->
            Combined = <<Buf/binary, Chunk/binary>>,
            Normalized = binary:replace(Combined, <<"\r\n">>, <<"\n">>, [global]),
            feed_sse(Normalized, A, ?TEST_SID, ?TEST_IID, ?TEST_TRACE, #{})
        end,
        {<<>>, Acc0},
        Chunks
    ),
    flush_sse_buf(FinalBuf, Acc1, ?TEST_SID, ?TEST_IID, ?TEST_TRACE, #{}).

-endif.
