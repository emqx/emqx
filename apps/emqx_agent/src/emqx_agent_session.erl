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
%%   idle ──event──▶  calling_llm  (continue reasoning with accumulated context)
%%   idle ──request──▶  (discarded — context is fixed after the first request)
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

%% Public API
-export([start_link/1, whereis/1]).

%% Hook management (called from emqx_agent_app)
-export([init_hook/0, deinit_hook/0, on_message_publish/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

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
    %% LLM connection params supplied per-request
    api_key :: binary(),
    base_url :: binary(),
    model :: binary(),
    tools :: [map()],
    output_schema :: map(),
    max_tokens = 2048 :: non_neg_integer(),
    recv_timeout_ms = 180000 :: non_neg_integer(),
    temperature = 0.1 :: number(),
    stream = true :: boolean(),
    tool_choice = <<"required">> :: binary() | map(),
    max_iterations = ?MAX_ITERATIONS :: non_neg_integer(),
    %% Growing OpenAI-format message list
    messages = [] :: [map()],
    %% Events buffered across all states; flushed before the next LLM call
    pending = [] :: [map()],
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
        <<"api_key">> := ApiKey,
        <<"base_url">> := BaseUrl,
        <<"model">> := Model,
        <<"tools">> := Tools,
        <<"input">> := Input,
        <<"instructions">> := Instructions,
        <<"output_schema">> := OutputSchema
    } = Msg,
    SysMsg = #{<<"role">> => <<"system">>, <<"content">> => format_instructions(Instructions)},
    UserMsg = #{<<"role">> => <<"user">>, <<"content">> => emqx_utils_json:encode(Input)},
    %% Events buffered before the first request are folded in after the user message.
    PendingMsgs = [event_to_llm_msg(E) || E <- Data#data.pending],
    Data1 = Data#data{
        iid = Iid,
        trace_id = TraceId,
        api_key = ApiKey,
        base_url = BaseUrl,
        model = Model,
        tools = Tools,
        output_schema = OutputSchema,
        max_tokens = maps:get(<<"max_tokens">>, Msg, 2048),
        recv_timeout_ms = maps:get(<<"recv_timeout_ms">>, Msg, 180000),
        temperature = maps:get(<<"temperature">>, Msg, 0.1),
        stream = maps:get(<<"stream">>, Msg, true),
        tool_choice = maps:get(<<"tool_choice">>, Msg, <<"required">>),
        max_iterations = maps:get(<<"max_iterations">>, Msg, ?MAX_ITERATIONS),
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
    {next_state, calling_llm, start_llm_call(Data1)};
%% ── idle: further requests are discarded ─────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"request">>}}, idle, Data) ->
    ?SLOG(info, #{msg => "session_request_discarded", sid => Data#data.sid}),
    {keep_state, Data};
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
    {next_state, calling_llm, start_llm_call(Data1)};
%% ── any other state: buffer events ───────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"event">>} = Msg}, _State, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    {keep_state, Data#data{pending = Data#data.pending ++ [Event]}};
%% Duplicate / out-of-order request in active states — ignore
handle_event(cast, {in, #{<<"type">> := <<"request">>}}, _State, Data) ->
    {keep_state, Data};
%% ── calls ─────────────────────────────────────────────────────────────────

handle_event({call, From}, _Req, _State, Data) ->
    {keep_state, Data, [{reply, From, {error, unknown_request}}]};
%% ── catch-all ─────────────────────────────────────────────────────────────

handle_event(_EventType, _EventContent, _State, Data) ->
    {keep_state, Data}.

%%--------------------------------------------------------------------
%% LLM subprocess
%%--------------------------------------------------------------------

%% Spawns the HTTP call; returns updated Data ready for state calling_llm.
%% The child exits with reason {llm_result, Tag, Result}, delivered to the
%% parent as {'DOWN', MonRef, process, _, {llm_result, Tag, Result}}.
start_llm_call(Data) ->
    Tag = make_ref(),
    OpenAITools = [to_openai_tool(T) || T <- Data#data.tools],
    ToolNames = [
        maps:get(<<"name">>, maps:get(<<"function">>, T, #{}), <<"?">>)
     || T <- OpenAITools
    ],
    Messages = Data#data.messages,
    ApiKey = Data#data.api_key,
    BaseUrl = Data#data.base_url,
    Model = Data#data.model,
    MaxTokens = Data#data.max_tokens,
    RecvTimeoutMs = Data#data.recv_timeout_ms,
    Temperature = Data#data.temperature,
    Stream = Data#data.stream,
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
        stream => Stream,
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
            Stream,
            ToolChoice,
            Data#data.sid,
            Data#data.iid,
            Data#data.trace_id,
            Data#data.usage
        ),
        exit({llm_result, Tag, Result})
    end),
    Data#data{llm_ref = {Tag, MonRef}}.

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
    ToolMsg = #{
        <<"role">> => <<"tool">>,
        <<"tool_call_id">> => CallId,
        <<"content">> => emqx_utils_json:encode(#{
            <<"ok">> => Ok,
            <<"data">> => maps:get(<<"data">>, Msg, #{})
        })
    },
    Waiting1 = lists:delete(CallId, Data#data.waiting_calls),
    Data1 = Data#data{
        waiting_calls = Waiting1,
        tool_result_msgs = Data#data.tool_result_msgs ++ [ToolMsg]
    },
    case Waiting1 of
        [_ | _] ->
            %% Still waiting for more results
            {keep_state, Data1};
        [] ->
            %% All results in; fold pending events and continue
            Results = Data1#data.tool_result_msgs,
            EventMsgs = [event_to_llm_msg(E) || E <- Data1#data.pending],
            Data2 = Data1#data{
                messages = Data1#data.messages ++ Results ++ EventMsgs,
                tool_result_msgs = [],
                pending = []
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
            {next_state, calling_llm, start_llm_call(Data1)}
    end.

%% Guard against runaway tool-call loops.
maybe_next_llm_call(Data) ->
    Iter = maps:get(<<"iterations">>, Data#data.usage),
    case Iter >= Data#data.max_iterations of
        true ->
            ?SLOG(warning, #{
                msg => "session_max_iterations_reached",
                sid => Data#data.sid,
                iterations => Iter,
                max_iterations => Data#data.max_iterations
            }),
            publish(Data, #{
                <<"type">> => <<"final">>,
                <<"result">> => #{<<"error">> => <<"max_iterations_reached">>}
            }),
            finish(Data);
        false ->
            {next_state, calling_llm, start_llm_call(Data)}
    end.

%% After publishing final: stop or return to idle based on stop_on_finish.
finish(#data{stop_on_finish = true} = Data) ->
    {stop, normal, Data};
finish(#data{stop_on_finish = false} = Data) ->
    ?SLOG(info, #{msg => "session_idle", sid => Data#data.sid}),
    {next_state, idle, Data}.

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
    Function = maps:get(<<"function">>, ToolCall),
    ToolName = maps:get(<<"name">>, Function),
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
    Stream,
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
        <<"max_tokens">> => MaxTokens,
        <<"temperature">> => Temperature,
        <<"stream">> => Stream
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
    case Stream of
        true ->
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
            );
        false ->
            call_llm_non_stream(Url, Headers, Body, RecvTimeoutMs, Model)
    end.

call_llm_non_stream(Url, Headers, Body, RecvTimeoutMs, Model) ->
    Opts = [with_body, {connect_timeout, 30_000}, {recv_timeout, RecvTimeoutMs}],
    case hackney:request(post, Url, Headers, emqx_utils_json:encode(Body), Opts) of
        {ok, 200, _RespHdrs, RespBody} ->
            Decoded = emqx_utils_json:decode(RespBody),
            case maps:get(<<"choices">>, Decoded, []) of
                [FirstChoice | _] ->
                    Msg = maps:get(<<"message">>, FirstChoice, #{}),
                    FR = maps:get(<<"finish_reason">>, FirstChoice, undefined),
                    TC = maps:get(<<"tool_calls">>, Msg, []),
                    ?SLOG(info, #{
                        msg => "session_llm_response",
                        model => Model,
                        finish_reason => FR,
                        tool_calls_count => length(TC),
                        has_content => maps:is_key(<<"content">>, Msg)
                    }),
                    UsageMap = maps:get(<<"usage">>, Decoded, #{}),
                    TokensIn = maps:get(<<"prompt_tokens">>, UsageMap, 0),
                    TokensOut = maps:get(<<"completion_tokens">>, UsageMap, 0),
                    {ok, FirstChoice, TokensIn, TokensOut};
                [] ->
                    {error, no_choices}
            end;
        {ok, Status, _RespHdrs, RespBody} ->
            {error, {http_error, Status, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

stream_llm_response(Url, Headers, Body0, RecvTimeoutMs, Model, Sid, Iid, TraceId, Usage) ->
    Body = Body0#{<<"stream_options">> => #{<<"include_usage">> => true}},
    Opts = [{connect_timeout, 30_000}, {recv_timeout, RecvTimeoutMs}],
    case hackney:request(post, Url, Headers, emqx_utils_json:encode(Body), Opts) of
        {ok, 200, _RespHdrs, ClientRef} ->
            case stream_llm_body(ClientRef, Model, Sid, Iid, TraceId, Usage) of
                {ok, Choice, TokensIn, TokensOut} = Ok ->
                    case stream_result_empty(Choice, TokensIn, TokensOut) of
                        true ->
                            ?SLOG(warning, #{
                                msg => "session_stream_empty_fallback_nonstream",
                                model => Model,
                                sid => Sid
                            }),
                            call_llm_non_stream(
                                Url,
                                Headers,
                                disable_stream(Body0),
                                RecvTimeoutMs,
                                Model
                            );
                        false ->
                            Ok
                    end;
                Error ->
                    Error
            end;
        {ok, Status, _RespHdrs, ClientRef} ->
            {error, {http_error, Status, read_stream_error_body(ClientRef, <<>>)}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

disable_stream(Body) ->
    maps:remove(<<"stream_options">>, Body#{<<"stream">> => false}).

stream_result_empty(Choice, TokensIn, TokensOut) ->
    Msg = maps:get(<<"message">>, Choice, #{}),
    Content = maps:get(<<"content">>, Msg, <<>>),
    ToolCalls = maps:get(<<"tool_calls">>, Msg, []),
    Content =:= <<>> andalso ToolCalls =:= [] andalso TokensIn =:= 0 andalso TokensOut =:= 0.

stream_llm_body(ClientRef, Model, Sid, Iid, TraceId, Usage) ->
    stream_llm_body(ClientRef, <<>>, init_stream_acc(), Model, Sid, Iid, TraceId, Usage).

stream_llm_body(ClientRef, Buffer, Acc, Model, Sid, Iid, TraceId, Usage) ->
    case hackney:stream_body(ClientRef) of
        {ok, Chunk} ->
            {Rest, Acc1} = consume_stream_chunk(Buffer, Chunk, Acc, Sid, Iid, TraceId, Usage),
            stream_llm_body(ClientRef, Rest, Acc1, Model, Sid, Iid, TraceId, Usage);
        done ->
            Acc1 = process_stream_rest(Buffer, Acc, Sid, Iid, TraceId, Usage),
            finalize_stream_result(Acc1, Model);
        {error, Reason} ->
            {error, {stream_failed, Reason}}
    end.

consume_stream_chunk(Buffer, Chunk, Acc0, Sid, Iid, TraceId, Usage) ->
    All = <<Buffer/binary, Chunk/binary>>,
    {Events, Rest} = split_stream_events(All),
    Acc =
        lists:foldl(
            fun(Event, A0) ->
                process_sse_event(Event, A0, Sid, Iid, TraceId, Usage)
            end,
            Acc0,
            Events
        ),
    {Rest, Acc}.

split_sse_events(Bin) ->
    Normalized = binary:replace(Bin, <<"\r\n">>, <<"\n">>, [global]),
    case binary:split(Normalized, <<"\n\n">>, [global]) of
        [] ->
            {[], <<>>};
        [Single] ->
            {[], Single};
        Parts ->
            Rest = lists:last(Parts),
            {lists:sublist(Parts, length(Parts) - 1), Rest}
    end.

split_stream_events(Bin) ->
    case binary:match(Bin, <<"data:">>) of
        nomatch -> split_ndjson_events(Bin);
        _ -> split_sse_events(Bin)
    end.

split_ndjson_events(Bin) ->
    Normalized = binary:replace(Bin, <<"\r\n">>, <<"\n">>, [global]),
    case binary:split(Normalized, <<"\n">>, [global]) of
        [] ->
            {[], <<>>};
        [Single] ->
            {[], Single};
        Parts ->
            Rest = lists:last(Parts),
            Events = [Line || Line <- lists:sublist(Parts, length(Parts) - 1), Line =/= <<>>],
            {Events, Rest}
    end.

process_stream_rest(<<>>, Acc, _Sid, _Iid, _TraceId, _Usage) ->
    Acc;
process_stream_rest(Rest0, Acc, Sid, Iid, TraceId, Usage) ->
    Rest = trim_binary(Rest0),
    case Rest of
        <<>> ->
            Acc;
        _ ->
            process_sse_event(Rest, Acc, Sid, Iid, TraceId, Usage)
    end.

process_sse_event(Event, Acc, Sid, Iid, TraceId, Usage) ->
    DataParts =
        [
            trim_binary(Value)
         || Line <- binary:split(Event, <<"\n">>, [global]),
            {match, 0} =:= binary:match(Line, <<"data:">>),
            Value <- [binary:part(Line, 5, byte_size(Line) - 5)]
        ],
    case DataParts of
        [] ->
            process_raw_stream_event(trim_binary(Event), Acc, Sid, Iid, TraceId, Usage);
        _ ->
            JsonBin = iolist_to_binary(lists:join(<<"\n">>, DataParts)),
            case JsonBin of
                <<"[DONE]">> ->
                    Acc;
                _ ->
                    process_sse_json(JsonBin, Acc, Sid, Iid, TraceId, Usage)
            end
    end.

process_raw_stream_event(<<>>, Acc, _Sid, _Iid, _TraceId, _Usage) ->
    Acc;
process_raw_stream_event(<<"[DONE]">>, Acc, _Sid, _Iid, _TraceId, _Usage) ->
    Acc;
process_raw_stream_event(JsonBin, Acc, Sid, Iid, TraceId, Usage) ->
    process_sse_json(JsonBin, Acc, Sid, Iid, TraceId, Usage).

process_sse_json(JsonBin, Acc, Sid, Iid, TraceId, Usage) ->
    try
        Chunk = emqx_utils_json:decode(JsonBin),
        update_stream_acc(Chunk, Acc, Sid, Iid, TraceId, Usage)
    catch
        _:_ ->
            Acc
    end.

update_stream_acc(Chunk, Acc0, Sid, Iid, TraceId, Usage) ->
    UsageMap = maps:get(<<"usage">>, Chunk, #{}),
    TokensIn = maps:get(<<"prompt_tokens">>, UsageMap, maps:get(tokens_in, Acc0)),
    TokensOut = maps:get(<<"completion_tokens">>, UsageMap, maps:get(tokens_out, Acc0)),
    case maps:get(<<"choices">>, Chunk, []) of
        [Choice | _] ->
            update_stream_choice(Choice, Acc0, Sid, Iid, TraceId, Usage, TokensIn, TokensOut);
        _ ->
            case maps:get(<<"message">>, Chunk, undefined) of
                M when is_map(M) ->
                    Content = maps:get(<<"content">>, M, <<>>),
                    maybe_publish_stream_chunk(
                        <<"content">>, Content, Sid, Iid, TraceId, Usage
                    ),
                    Acc0#{
                        content => Content,
                        tool_calls => tool_calls_list_to_map(maps:get(<<"tool_calls">>, M, [])),
                        finish_reason => maps:get(
                            <<"finish_reason">>, Chunk, maps:get(finish_reason, Acc0)
                        ),
                        tokens_in => TokensIn,
                        tokens_out => TokensOut
                    };
                _ ->
                    Acc0#{tokens_in => TokensIn, tokens_out => TokensOut}
            end
    end.

update_stream_choice(Choice, Acc0, Sid, Iid, TraceId, Usage, TokensIn, TokensOut) ->
    Delta = maps:get(<<"delta">>, Choice, undefined),
    Msg = maps:get(<<"message">>, Choice, undefined),
    FinishReason = maps:get(<<"finish_reason">>, Choice, maps:get(finish_reason, Acc0)),
    case Delta of
        D when is_map(D), D =/= #{} ->
            maybe_publish_stream_chunk(
                <<"reasoning">>,
                maps:get(<<"reasoning_content">>, D, undefined),
                Sid,
                Iid,
                TraceId,
                Usage
            ),
            maybe_publish_stream_chunk(
                <<"content">>, maps:get(<<"content">>, D, undefined), Sid, Iid, TraceId, Usage
            ),
            Content1 = append_bin(maps:get(content, Acc0), maps:get(<<"content">>, D, <<>>)),
            ToolCalls1 = merge_tool_calls(
                maps:get(tool_calls, Acc0), maps:get(<<"tool_calls">>, D, [])
            ),
            Acc0#{
                content => Content1,
                tool_calls => ToolCalls1,
                finish_reason => FinishReason,
                tokens_in => TokensIn,
                tokens_out => TokensOut
            };
        _ ->
            case Msg of
                M when is_map(M) ->
                    Content = maps:get(<<"content">>, M, <<>>),
                    maybe_publish_stream_chunk(
                        <<"content">>, Content, Sid, Iid, TraceId, Usage
                    ),
                    ToolCalls = maps:get(<<"tool_calls">>, M, []),
                    Acc0#{
                        content => Content,
                        tool_calls => tool_calls_list_to_map(ToolCalls),
                        finish_reason => FinishReason,
                        tokens_in => TokensIn,
                        tokens_out => TokensOut
                    };
                _ ->
                    Acc0#{
                        finish_reason => FinishReason,
                        tokens_in => TokensIn,
                        tokens_out => TokensOut
                    }
            end
    end.

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
    ToolCalls = stream_tool_calls_to_list(maps:get(tool_calls, Acc)),
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
    FinishReason0 = maps:get(finish_reason, Acc, undefined),
    FinishReason =
        case FinishReason0 of
            undefined -> <<"stop">>;
            _ -> FinishReason0
        end,
    ?SLOG(info, #{
        msg => "session_llm_response_stream",
        model => Model,
        finish_reason => FinishReason,
        tool_calls_count => length(ToolCalls),
        has_content => Content =/= <<>>
    }),
    {
        ok,
        #{<<"message">> => Msg, <<"finish_reason">> => FinishReason},
        maps:get(tokens_in, Acc, 0),
        maps:get(tokens_out, Acc, 0)
    }.

stream_tool_calls_to_list(ToolCallsMap) ->
    lists:map(
        fun(Index) -> maps:get(Index, ToolCallsMap) end,
        lists:sort(maps:keys(ToolCallsMap))
    ).

tool_calls_list_to_map(ToolCalls) when is_list(ToolCalls) ->
    lists:foldl(
        fun(TC, Acc) ->
            Index = maps:get(<<"index">>, TC, maps:size(Acc)),
            maps:put(Index, TC, Acc)
        end,
        #{},
        ToolCalls
    );
tool_calls_list_to_map(_) ->
    #{}.

merge_tool_calls(ToolCallsMap, ToolCallsDelta) when is_list(ToolCallsDelta) ->
    lists:foldl(
        fun(TCDelta, Acc) ->
            Index = maps:get(<<"index">>, TCDelta, 0),
            Prev = maps:get(Index, Acc, #{}),
            maps:put(Index, merge_tool_call(Prev, TCDelta), Acc)
        end,
        ToolCallsMap,
        ToolCallsDelta
    );
merge_tool_calls(ToolCallsMap, _) ->
    ToolCallsMap.

merge_tool_call(Prev, Delta) ->
    PrevFun = maps:get(<<"function">>, Prev, #{}),
    DeltaFun = maps:get(<<"function">>, Delta, #{}),
    PrevArgs = maps:get(<<"arguments">>, PrevFun, <<>>),
    DeltaArgs = maps:get(<<"arguments">>, DeltaFun, <<>>),
    Fun = maps:merge(PrevFun, DeltaFun#{<<"arguments">> => append_bin(PrevArgs, DeltaArgs)}),
    maps:merge(Prev, Delta#{<<"function">> => Fun}).

append_bin(A, B) when is_binary(A), is_binary(B) ->
    <<A/binary, B/binary>>;
append_bin(A, _B) ->
    A.

trim_binary(Bin) ->
    iolist_to_binary(string:trim(Bin)).

read_stream_error_body(ClientRef, Acc) ->
    case hackney:stream_body(ClientRef) of
        {ok, Chunk} -> read_stream_error_body(ClientRef, <<Acc/binary, Chunk/binary>>);
        done -> Acc;
        {error, _} -> Acc
    end.

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

try_parse_result(Content) when is_binary(Content) ->
    try
        emqx_utils_json:decode(Content)
    catch
        _:_ -> #{<<"summary">> => Content}
    end;
try_parse_result(Content) ->
    Content.
