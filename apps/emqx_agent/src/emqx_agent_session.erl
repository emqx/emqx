%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Session gen_statem: one process per logical session, globally registered
%% so it is unique across the cluster.
%%
%% Incoming topic:  sess/<SID>/in
%% Outgoing topic:  sess/<SID>/out
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
%%   idle  ──request──▶  calling_llm  ──tool_calls──▶  waiting_tools
%%                            │                              │
%%                            │◀──────── all results ────────┘
%%                            │
%%                         stop/no pending, stop_on_finish=true  ──▶  {stop, normal}
%%                         stop/no pending, stop_on_finish=false ──▶  idle (keep alive)
%%                         stop/pending                          ──▶  calling_llm  (extend context)
%%
%% Events are always buffered in `pending` regardless of state.
%% They are flushed into the message context when transitioning to calling_llm.

-module(emqx_agent_session).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

%% Public API
-export([start_link/1]).

%% Hook management (called from emqx_agent_app)
-export([init_hook/0, deinit_hook/0, on_message_publish/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, handle_event/4, terminate/3]).

-define(REG(Sid), {global, {?MODULE, Sid}}).
-define(OUT(Sid), <<"sess/", Sid/binary, "/out">>).
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
    %% When false, it returns to idle and waits for more requests/events.
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
    #message{topic = <<"sess/", Rest/binary>>, payload = Payload} = Message
) ->
    case binary:split(Rest, <<"/">>) of
        [Sid, <<"in">>] -> route(Sid, Payload);
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

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init(Sid) ->
    ?SLOG(info, #{msg => "session_started", sid => Sid}),
    {ok, idle, #data{sid = Sid}}.

terminate(_Reason, _State, #data{sid = Sid}) ->
    ?SLOG(info, #{msg => "session_terminating", sid => Sid}),
    ok.

%%--------------------------------------------------------------------
%% handle_event/4
%%
%% Clauses are ordered: specific state first, catch-alls last.
%%--------------------------------------------------------------------

%% ── idle: accept a new request ────────────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"request">>} = Msg}, idle, Data) ->
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
    Data1 = Data#data{
        iid = Iid,
        trace_id = TraceId,
        api_key = ApiKey,
        base_url = BaseUrl,
        model = Model,
        tools = Tools,
        output_schema = OutputSchema,
        stop_on_finish = maps:get(<<"stop_on_finish">>, Msg, true),
        messages = [SysMsg, UserMsg],
        pending = [],
        usage = #{
            <<"iterations">> => 0,
            <<"tool_calls">> => 0,
            <<"tokens_in">> => 0,
            <<"tokens_out">> => 0
        }
    },
    {next_state, calling_llm, start_llm_call(Data1)};

%% ── calling_llm: LLM subprocess finished ─────────────────────────────────

%% Child exits with {llm_result, Tag, Result} — normal completion path
handle_event(info, {'DOWN', Mon, process, _, {llm_result, Tag, Result}}, calling_llm,
             #data{llm_ref = {Tag, Mon}} = Data) ->
    Data1 = Data#data{llm_ref = undefined},
    case Result of
        {ok, Choice, TokensIn, TokensOut} ->
            Usage0 = Data1#data.usage,
            Data2 = Data1#data{usage = Usage0#{
                <<"iterations">> => maps:get(<<"iterations">>, Usage0) + 1,
                <<"tokens_in">>  => maps:get(<<"tokens_in">>,  Usage0) + TokensIn,
                <<"tokens_out">> => maps:get(<<"tokens_out">>, Usage0) + TokensOut
            }},
            on_llm_choice(Choice, Data2);
        {error, Reason} ->
            ?SLOG(error, #{msg => "session_llm_error", sid => Data1#data.sid, reason => Reason}),
            {stop, {llm_error, Reason}, Data1}
    end;

%% Child exited with any other reason — unexpected crash
handle_event(info, {'DOWN', Mon, process, _, Reason}, calling_llm,
             #data{llm_ref = {_Tag, Mon}} = Data) ->
    ?SLOG(error, #{msg => "session_llm_process_crashed",
                   sid => Data#data.sid, reason => Reason}),
    {stop, {llm_process_crashed, Reason}, Data};

%% ── waiting_tools: collect tool results ───────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"tool_result">>, <<"call_id">> := CallId} = Msg},
             waiting_tools, Data) ->
    on_tool_result(CallId, Msg, Data);

%% ── any state: explicit stop ──────────────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"stop">>}}, _State, Data) ->
    ?SLOG(info, #{msg => "session_stopped_explicitly", sid => Data#data.sid}),
    {stop, normal, Data};

%% ── any state: buffer events ──────────────────────────────────────────────

handle_event(cast, {in, #{<<"type">> := <<"event">>} = Msg}, _State, Data) ->
    Event = maps:get(<<"event">>, Msg, Msg),
    {keep_state, Data#data{pending = Data#data.pending ++ [Event]}};

%% Duplicate / out-of-order request while processing — ignore
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
    Messages = Data#data.messages,
    ApiKey = Data#data.api_key,
    BaseUrl = Data#data.base_url,
    Model = Data#data.model,
    {_Pid, MonRef} = spawn_monitor(fun() ->
        Result = call_llm(Messages, OpenAITools, ApiKey, BaseUrl, Model),
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
            on_reasoning_done(LLMMsg, Data1)
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
on_reasoning_done(LLMMsg, Data) ->
    case Data#data.pending of
        [] ->
            Content = maps:get(<<"content">>, LLMMsg, <<"">>),
            publish(Data, #{
                <<"type">> => <<"final">>,
                <<"result">> => try_parse_result(Content)
            }),
            finish(Data);
        Events ->
            ?SLOG(info, #{msg => "session_restarting",
                          sid => Data#data.sid, pending => length(Events)}),
            EventMsgs = [event_to_llm_msg(E) || E <- Events],
            Data1 = Data#data{messages = Data#data.messages ++ EventMsgs, pending = []},
            {next_state, calling_llm, start_llm_call(Data1)}
    end.

%% Guard against runaway tool-call loops.
maybe_next_llm_call(Data) ->
    Iter = maps:get(<<"iterations">>, Data#data.usage),
    case Iter >= ?MAX_ITERATIONS of
        true ->
            ?SLOG(warning, #{msg => "session_max_iterations_reached",
                             sid => Data#data.sid, iterations => Iter}),
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
        try emqx_utils_json:decode(Payload)
        catch _:_ -> undefined
        end,
    case Msg of
        undefined ->
            ok;
        #{<<"type">> := <<"request">>} ->
            Pid = find_or_start(Sid),
            gen_statem:cast(Pid, {in, Msg});
        #{<<"type">> := _} ->
            case global:whereis_name({?MODULE, Sid}) of
                undefined -> ok;
                Pid -> gen_statem:cast(Pid, {in, Msg})
            end
    end.

find_or_start(Sid) ->
    case global:whereis_name({?MODULE, Sid}) of
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
        try emqx_utils_json:decode(maps:get(<<"arguments">>, Function, <<"{}">>) )
        catch _:_ -> #{}
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

call_llm(Messages, Tools, ApiKey, BaseUrl, Model) ->
    Url = <<BaseUrl/binary, "/chat/completions">>,
    Body0 = #{<<"model">> => Model, <<"messages">> => Messages},
    Body = case Tools of
        [] -> Body0;
        _  -> Body0#{<<"tools">> => Tools}
    end,
    Headers = [
        {<<"authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"content-type">>, <<"application/json">>}
    ],
    Opts = [with_body, {connect_timeout, 30_000}, {recv_timeout, 120_000}],
    case hackney:request(post, Url, Headers, emqx_utils_json:encode(Body), Opts) of
        {ok, 200, _RespHdrs, RespBody} ->
            Decoded = emqx_utils_json:decode(RespBody),
            case maps:get(<<"choices">>, Decoded, []) of
                [Choice | _] ->
                    UsageMap = maps:get(<<"usage">>, Decoded, #{}),
                    TokensIn  = maps:get(<<"prompt_tokens">>,     UsageMap, 0),
                    TokensOut = maps:get(<<"completion_tokens">>, UsageMap, 0),
                    {ok, Choice, TokensIn, TokensOut};
                [] ->
                    {error, no_choices}
            end;
        {ok, Status, _RespHdrs, RespBody} ->
            {error, {http_error, Status, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

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

%% Publish a frame to sess/<Sid>/out, automatically filling correlation fields
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
    try emqx_utils_json:decode(Content)
    catch _:_ -> #{<<"summary">> => Content}
    end;
try_parse_result(Content) ->
    Content.
