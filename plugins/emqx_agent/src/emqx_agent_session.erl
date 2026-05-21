%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Session gen_statem: one process per logical session, globally registered
%% so it is unique across the cluster.
%%
%% Incoming topic:  $sess/in/<SID>/
%% Outgoing topic:  $sess/out/<SID>/
%%
%% Message types on in-topic:
%%   request     — start an LLM reasoning loop; carries optional persistent (default false)
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
%%                                stop/no pending, persistent=false ──▶  {stop, normal}
%%                                stop/no pending, persistent=true  ──▶  idle
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
-include("emqx_agent_pipeline.hrl").

%% Public API
-export([start_link/2, whereis/1, inspect/1]).

%% Hook management (called from emqx_agent_app)
-export([init/0, deinit/0, on_message_publish/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-ifdef(TEST).
-export([
    init_stream_acc/0,
    test_feed_sse/2,
    test_stream_chunks/1
]).
-endif.

-define(NAME(Sid), {?MODULE, Sid}).
-define(REG(Sid), {global, ?NAME(Sid)}).
-define(IN(Sid), <<?AGENT_SESS_IN_PREFIX/binary, (Sid)/binary, "/">>).
-define(OUT(Sid), <<?AGENT_SESS_OUT_PREFIX/binary, (Sid)/binary, "/">>).
-define(MAX_ITERATIONS, 20).
-define(PERSISTENT_IDLE_TIMEOUT_MS, 3_600_000).

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

    max_tokens = 2048 :: non_neg_integer(),
    max_total_tokens = 50000 :: non_neg_integer(),
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
    compact_request = undefined :: undefined | map(),
    %% Accumulated usage counters
    usage = #{
        <<"iterations">> => 0,
        <<"tool_calls">> => 0,
        <<"tokens_in">> => 0,
        <<"tokens_out">> => 0,
        <<"total_tokens">> => 0
    } :: map(),
    %% Iteration counter for the current request only; used for the runaway loop guard.
    request_iterations = 0 :: non_neg_integer(),
    %% When false (default), the session stops after publishing final.
    %% When true, it transitions to idle where events drive further reasoning.
    persistent = false :: boolean(),
    idle_timer_ref = undefined :: reference() | undefined,
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

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

on_message_publish(
    #message{topic = <<"$sess/in/", Rest/binary>>, payload = Payload} = Message
) ->
    case binary:split(Rest, <<"/">>) of
        [Sid, <<>>] ->
            Msg = safe_decode(Payload),
            case Msg of
                #{<<"type">> := <<"request">>} ->
                    Persistent = maps:get(<<"persistent">>, Msg, false),
                    case {Persistent, global:whereis_name(?NAME(Sid))} of
                        {true, undefined} ->
                            _ = emqx_agent_sess_sup:start_session(Sid, true);
                        {false, _} ->
                            _ = emqx_agent_sess_sup:start_session(Sid, false);
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    {ok, Message};
on_message_publish(Message) ->
    {ok, Message}.

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec start_link(binary(), boolean()) -> {ok, pid()} | {error, term()}.
start_link(Sid, true) ->
    gen_statem:start_link(?REG(Sid), ?MODULE, [Sid, true], []);
start_link(Sid, false) ->
    gen_statem:start_link(?MODULE, [Sid, false], []).

-spec whereis(binary()) -> pid() | undefined.
whereis(Sid) ->
    global:whereis_name(?NAME(Sid)).

-spec inspect(binary()) -> {ok, map()} | {error, not_found}.
inspect(Sid) ->
    case ?MODULE:whereis(Sid) of
        undefined -> {error, not_found};
        Pid -> gen_statem:call(Pid, inspect)
    end.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init([Sid, Persistent]) ->
    ?SLOG(info, #{msg => "session_started", sid => Sid, persistent => Persistent}),
    _ = emqx:subscribe(?IN(Sid)),
    {ok, initial_idle, #data{sid = Sid, persistent = Persistent}}.

terminate(_Reason, _State, #data{sid = Sid}) ->
    ?SLOG(info, #{msg => "session_terminating", sid => Sid}),
    _ = emqx:unsubscribe(?IN(Sid)),
    ok.

%%--------------------------------------------------------------------
%% handle_event/4
%%
%% Clauses are ordered: specific state first, catch-alls last.
%%--------------------------------------------------------------------

%% ── MQTT delivery: messages on $sess/in/<Sid>/ arrive via subscription ────

handle_event(
    info,
    #deliver{topic = Topic, message = #message{payload = Payload}},
    State,
    #data{sid = Sid} = Data
) when Topic =:= ?IN(Sid) ->
    case safe_decode(Payload) of
        #{<<"type">> := _} = Msg ->
            handle_sess_msg(Msg, State, Data);
        _ ->
            {keep_state, Data}
    end;
%% ── internal: replayed queued request via next_event ───────────────────────

handle_event(info, {sess_msg, Msg}, State, Data) ->
    handle_sess_msg(Msg, State, Data);
%% ── calling_llm: LLM subprocess finished ─────────────────────────────────

%% Child exits with {llm_result, Tag, Result} — normal completion path
handle_event(
    info,
    {'DOWN', Mon, process, _, {llm_result, Tag, Result}},
    compacting_history,
    #data{llm_ref = {Tag, Mon}, compact_request = Msg} = Data
) ->
    Data1 = Data#data{llm_ref = undefined, compact_request = undefined},
    case Result of
        {ok, Choice, _TokensIn, _TokensOut, TotalTokens} ->
            LLMMsg = maps:get(<<"message">>, Choice, #{}),
            Summary = maps:get(<<"content">>, LLMMsg, <<>>),
            Usage0 = Data1#data.usage,
            Data2 = Data1#data{usage = Usage0#{<<"total_tokens">> => TotalTokens}},
            start_llm_call(apply_compacted_request(Msg, Summary, Data2));
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "session_history_compaction_failed", sid => Data1#data.sid, reason => Reason
            }),
            ok = publish(Data1, #{
                <<"type">> => <<"error">>, <<"reason">> => emqx_utils:readable_error_msg(Reason)
            }),
            {stop, normal, Data1}
    end;
handle_event(
    info,
    {'DOWN', Mon, process, _, {llm_result, Tag, Result}},
    calling_llm,
    #data{llm_ref = {Tag, Mon}} = Data
) ->
    Data1 = Data#data{llm_ref = undefined},
    case Result of
        {ok, Choice, TokensIn, TokensOut, TotalTokens} ->
            Usage0 = Data1#data.usage,
            Data2 = Data1#data{
                usage = Usage0#{
                    <<"iterations">> => maps:get(<<"iterations">>, Usage0) + 1,
                    <<"tokens_in">> => maps:get(<<"tokens_in">>, Usage0) + TokensIn,
                    <<"tokens_out">> => maps:get(<<"tokens_out">>, Usage0) + TokensOut,
                    <<"total_tokens">> => TotalTokens
                },
                request_iterations = Data1#data.request_iterations + 1
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
    compacting_history,
    #data{llm_ref = {_Tag, Mon}} = Data
) ->
    ?SLOG(error, #{
        msg => "session_compaction_process_crashed",
        sid => Data#data.sid,
        reason => Reason
    }),
    {stop, {compaction_process_crashed, Reason}, Data};
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
%% ── idle timeout for persistent sessions ─────────────────────────────────

handle_event(info, persistent_idle_timeout, idle, Data) ->
    ?SLOG(info, #{msg => "session_persistent_idle_timeout", sid => Data#data.sid}),
    {stop, normal, Data#data{idle_timer_ref = undefined}};
handle_event(info, persistent_idle_timeout, _State, Data) ->
    {keep_state, Data#data{idle_timer_ref = undefined}};
%% ── calls ─────────────────────────────────────────────────────────────────

handle_event({call, From}, inspect, _State, Data) ->
    Info = #{
        messages => Data#data.messages,
        request_iterations => Data#data.request_iterations
    },
    {keep_state, Data, [{reply, From, {ok, Info}}]};
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
    ?SLOG(info, #{
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
            Data#data.sid,
            Data#data.iid,
            Data#data.trace_id,
            Data#data.usage
        ),
        exit({llm_result, Tag, Result})
    end),
    {next_state, calling_llm, Data#data{llm_ref = {Tag, MonRef}}}.

start_compaction_call(Msg, Data) ->
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
            do_start_compaction_call(Msg, Data, Provider)
    end.

do_start_compaction_call(Msg, Data, Provider) ->
    Tag = make_ref(),
    #{base_url := BaseUrl, api_key := ApiKeySecret} = Provider,
    ApiKey = emqx_secret:unwrap(ApiKeySecret),
    Messages = Data#data.messages ++ [compaction_request_msg()],
    ?SLOG(info, #{
        msg => "session_history_compaction_started",
        sid => Data#data.sid,
        messages => length(Messages),
        total_tokens => maps:get(<<"total_tokens">>, Data#data.usage, 0),
        max_total_tokens => Data#data.max_total_tokens
    }),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        Result = call_llm(
            Messages,
            [],
            ApiKey,
            BaseUrl,
            Data#data.model,
            Data#data.max_tokens,
            provider_recv_timeout(Provider, Data#data.recv_timeout_ms),
            Data#data.temperature,
            <<"none">>,
            Data#data.sid,
            Data#data.iid,
            Data#data.trace_id,
            Data#data.usage
        ),
        exit({llm_result, Tag, Result})
    end),
    {next_state, compacting_history, Data#data{llm_ref = {Tag, MonRef}, compact_request = Msg}}.

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
    Result = maps:get(<<"response">>, Msg, #{}),
    %% Build the tool-role acknowledgement.  Tool messages must carry a string
    %% content; image_url is not a valid tool-message content type for OpenAI.
    %% When the result contains an image we acknowledge the tool call with a
    %% plain string, then append a separate user-role message whose content is
    %% the multimodal array — that is the form gpt-4o accepts for vision input.
    {ToolContent, ExtraMsgs} =
        case
            maps:get(<<"status">>, Result, <<"ok">>) =:= <<"ok">> andalso
                try_extract_image_url(
                    maps:get(<<"payload">>, maps:get(<<"result">>, Result, #{}), undefined)
                )
        of
            {ok, ImageUrl} ->
                AckJson = emqx_utils_json:encode(
                    #{<<"status">> => <<"ok">>, <<"result">> => <<"image received">>}
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
                {emqx_utils_json:encode(Result), []}
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
%% stop or return to idle depending on persistent.
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
    Iter = Data#data.request_iterations,
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

%% After publishing final: stop or return to idle based on persistent.
%% If a request arrived while the session was busy, process it immediately
%% rather than sitting in idle waiting for the next cast.
finish(#data{persistent = false} = Data) ->
    {stop, normal, Data};
finish(#data{persistent = true, queued_request = undefined} = Data) ->
    ?SLOG(info, #{msg => "session_idle", sid => Data#data.sid}),
    {next_state, idle, start_idle_timer(Data)};
finish(#data{persistent = true, queued_request = Queued} = Data) ->
    ?SLOG(info, #{msg => "session_draining_queued_request", sid => Data#data.sid}),
    Data1 = Data#data{queued_request = undefined},
    {next_state, idle, Data1, [{next_event, info, {sess_msg, Queued}}]}.

start_idle_timer(Data) ->
    Data1 = cancel_idle_timer(Data),
    Ref = erlang:send_after(?PERSISTENT_IDLE_TIMEOUT_MS, self(), persistent_idle_timeout),
    Data1#data{idle_timer_ref = Ref}.

cancel_idle_timer(#data{idle_timer_ref = undefined} = Data) ->
    Data;
cancel_idle_timer(#data{idle_timer_ref = Ref} = Data) ->
    _ = erlang:cancel_timer(Ref, [{async, false}, {info, false}]),
    Data#data{idle_timer_ref = undefined}.

%%--------------------------------------------------------------------
%% Session message dispatch — handles decoded JSON from $sess/in/<Sid>/
%%--------------------------------------------------------------------

%% ── initial_idle: accept the first request ─────────────────────────────

handle_sess_msg(#{<<"type">> := <<"request">>} = Msg, initial_idle, Data) ->
    #{
        <<"iid">> := Iid,
        <<"trace_id">> := TraceId,
        <<"provider_name">> := ProviderName,
        <<"model">> := Model,
        <<"tools">> := Tools,
        <<"input">> := Input,
        <<"instructions">> := Instructions
    } = Msg,
    SysMsg = #{<<"role">> => <<"system">>, <<"content">> => format_instructions(Instructions)},
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    PendingMsgs = [event_to_llm_msg(E) || E <- Data#data.pending],
    Data1 = Data#data{
        iid = Iid,
        trace_id = TraceId,
        provider_name = ProviderName,
        model = Model,
        tools = Tools,
        max_tokens = maps:get(<<"max_tokens">>, Msg, 2048),
        max_total_tokens = max_total_tokens(Msg),
        recv_timeout_ms = maps:get(<<"recv_timeout_ms">>, Msg, 180000),
        temperature = maps:get(<<"temperature">>, Msg, 0.1),
        stream = maps:get(<<"stream">>, Msg, true),
        tool_choice = maps:get(<<"tool_choice">>, Msg, <<"required">>),
        persistent = maps:get(<<"persistent">>, Msg, false),
        messages = [SysMsg, UserMsg] ++ PendingMsgs,
        pending = [],
        request_iterations = 0,
        usage = #{
            <<"iterations">> => 0,
            <<"tool_calls">> => 0,
            <<"tokens_in">> => 0,
            <<"tokens_out">> => 0,
            <<"total_tokens">> => 0
        }
    },
    start_llm_call(Data1);
%% ── idle: new request — append input to history ────────────────────────

handle_sess_msg(#{<<"type">> := <<"request">>, <<"input">> := Input} = Msg, idle, Data) ->
    Data0 = cancel_idle_timer(Data),
    Iid = maps:get(<<"iid">>, Msg, Data#data.iid),
    TraceId = maps:get(<<"trace_id">>, Msg, Data#data.trace_id),
    Tools = maps:get(<<"tools">>, Msg, Data#data.tools),
    ProviderName = maps:get(<<"provider_name">>, Msg, Data#data.provider_name),
    ToolChoice = maps:get(<<"tool_choice">>, Msg, Data#data.tool_choice),
    Messages0 =
        case maps:get(<<"instructions">>, Msg, undefined) of
            undefined ->
                Data0#data.messages;
            NewInstructions ->
                SysMsg = #{
                    <<"role">> => <<"system">>,
                    <<"content">> => format_instructions(NewInstructions)
                },
                [SysMsg | tl(Data0#data.messages)]
        end,
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    Data1 = Data0#data{
        iid = Iid,
        trace_id = TraceId,
        provider_name = ProviderName,
        tools = Tools,
        tool_choice = ToolChoice,
        max_tokens = maps:get(<<"max_tokens">>, Msg, Data#data.max_tokens),
        max_total_tokens = max_total_tokens(Msg, Data#data.max_total_tokens),
        messages = Messages0,
        persistent = maps:get(<<"persistent">>, Msg, true),
        pending = [],
        request_iterations = 0
    },
    ?SLOG(info, #{
        msg => "session_continuing",
        sid => Data#data.sid,
        iid => Iid,
        history_len => length(Data1#data.messages) + 1
    }),
    case should_compact_history(Data1) of
        true ->
            start_compaction_call(Msg, Data1);
        false ->
            start_llm_call(Data1#data{messages = Messages0 ++ [UserMsg]})
    end;
%% ── waiting_tools: collect tool results ────────────────────────────────

handle_sess_msg(
    #{<<"type">> := <<"tool_result">>, <<"call_id">> := CallId} = Msg, waiting_tools, Data
) ->
    on_tool_result(CallId, Msg, Data);
%% ── any state: explicit stop ───────────────────────────────────────────

handle_sess_msg(#{<<"type">> := <<"stop">>}, _State, Data) ->
    ?SLOG(info, #{msg => "session_stopped_explicitly", sid => Data#data.sid}),
    {stop, normal, Data};
%% ── idle + event: immediately continue reasoning ───────────────────────

handle_sess_msg(#{<<"type">> := <<"event">>} = Msg, idle, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    ?SLOG(info, #{msg => "session_event_triggers_llm", sid => Data#data.sid}),
    Data0 = cancel_idle_timer(Data),
    Data1 = Data0#data{messages = Data0#data.messages ++ [event_to_llm_msg(Event)]},
    start_llm_call(Data1);
%% ── any other state: buffer events ─────────────────────────────────────

handle_sess_msg(#{<<"type">> := <<"event">>} = Msg, _State, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    {keep_state, Data#data{pending = Data#data.pending ++ [Event]}};
%% Request while busy: buffer for replay (latest wins) ───────────────────

handle_sess_msg(#{<<"type">> := <<"request">>} = Msg, _State, Data) ->
    ?SLOG(info, #{
        msg => "session_request_queued",
        sid => Data#data.sid,
        iid => maps:get(<<"iid">>, Msg, undefined)
    }),
    {keep_state, Data#data{queued_request = Msg}};
%% ── unknown type: ignore ───────────────────────────────────────────────

handle_sess_msg(#{<<"type">> := _}, _State, Data) ->
    {keep_state, Data}.

safe_decode(Payload) ->
    try
        emqx_utils_json:decode(Payload)
    catch
        _:_ -> #{}
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
    Body =
        case Tools of
            [] ->
                Body0;
            _ ->
                TCChoice =
                    case ToolChoice of
                        <<"required">> -> <<"required">>;
                        <<"auto">> -> <<"auto">>;
                        <<"none">> -> <<"none">>;
                        V when is_map(V) -> V;
                        _ -> <<"required">>
                    end,
                Body0#{<<"tools">> => Tools, <<"tool_choice">> => TCChoice}
        end,
    Headers = [
        {<<"authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"content-type">>, <<"application/json">>}
    ],
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
            collect_stream(ClientRef, Model, Sid, Iid, TraceId, Usage);
        {ok, Status, _RespHdrs, ClientRef} ->
            ErrBody = drain_stream_body(ClientRef, <<>>),
            ?SLOG(error, #{
                msg => "session_llm_http_error",
                sid => Sid,
                status => Status
            }),
            {error, {http_error, Status, ErrBody}};
        {error, Reason} ->
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
            Combined = <<Buf/binary, Chunk/binary>>,
            Normalized = binary:replace(Combined, <<"\r\n">>, <<"\n">>, [global]),
            {Buf1, Acc1} = feed_sse(Normalized, Acc, Sid, Iid, TraceId, Usage),
            stream_receive_loop(ClientRef, Buf1, Acc1, Sid, Iid, TraceId, Usage);
        done ->
            %% Flush any remaining bytes in the buffer (edge case: server
            %% does not terminate the last event with \n\n).
            Acc1 = flush_sse_buf(Buf, Acc, Sid, Iid, TraceId, Usage),
            {ok, Acc1};
        {error, Reason} ->
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
                        maps:get(<<"completion_tokens">>, U, maps:get(tokens_out, Acc)),
                    total_tokens =>
                        maps:get(<<"total_tokens">>, U, maps:get(total_tokens, Acc))
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
        tokens_out => 0,
        total_tokens => 0
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
        tokens_out => maps:get(tokens_out, Acc, 0),
        total_tokens => maps:get(total_tokens, Acc, 0)
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
        maps:get(tokens_out, Acc, 0),
        maps:get(total_tokens, Acc, 0)
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

-define(SET_RESULT_SUPERPROMPT, <<
    "If a set_result tool is available, you MUST finish the step by calling ",
    "set_result with arguments matching its schema. Do not finish with a ",
    "plain final response when set_result is available."
>>).

format_instructions(Instructions) when is_list(Instructions) ->
    format_instructions(iolist_to_binary(lists:join(<<"\n">>, Instructions)));
format_instructions(Instructions) when is_binary(Instructions) ->
    <<?SET_RESULT_SUPERPROMPT/binary, "\n\n", Instructions/binary>>.

max_total_tokens(Msg) ->
    max_total_tokens(Msg, 50000).

max_total_tokens(Msg, Default) ->
    case maps:get(<<"max_total_tokens">>, Msg, Default) of
        V when is_integer(V), V > 0 -> V;
        _ -> 50000
    end.

should_compact_history(#data{persistent = true, max_total_tokens = Max, usage = Usage}) ->
    maps:get(<<"total_tokens">>, Usage, 0) >= Max;
should_compact_history(_) ->
    false.

apply_compacted_request(#{<<"input">> := Input}, Summary, #data{messages = Messages} = Data) ->
    SysMsg = hd(Messages),
    CompactMsg = #{
        <<"role">> => <<"assistant">>,
        <<"content">> => <<"Compacted prior conversation history:\n", Summary/binary>>
    },
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    Data#data{messages = [SysMsg, CompactMsg, UserMsg]}.

compaction_request_msg() ->
    #{
        <<"role">> => <<"user">>,
        <<"content">> => <<
            "Compact the prior conversation history into a concise but complete summary. ",
            "Preserve facts, decisions, user preferences, tool results, and unresolved work. ",
            "Return only the compacted history text."
        >>
    }.

%% Publish a frame to $sess/out/<Sid>/, automatically filling correlation fields
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
