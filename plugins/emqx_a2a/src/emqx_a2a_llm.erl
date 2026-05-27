%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_llm).

-moduledoc """
Multi-provider LLM client using Erlang's built-in httpc.

Supports Anthropic (Claude) and OpenAI (GPT) with SSE streaming.
No external deps.
""".

-export([
    chat_stream/5
]).

-include("emqx_a2a.hrl").

-define(ANTHROPIC_URL, "https://api.anthropic.com/v1/messages").
-define(ANTHROPIC_VERSION, "2023-06-01").
-define(OPENAI_URL, "https://api.openai.com/v1/responses").

%% @doc Start a streaming chat request. Sends chunks to CallerPid as messages:
%%   {stream_chunk, Text}   — incremental text delta
%%   {stream_done, Text}    — final assembled text
%%   {stream_error, Reason} — error
%%
%% Provider is determined from config. Opts may include:
%%   system_prompt, max_tokens, provider (anthropic | openai)
chat_stream(CallerPid, ApiKey, Model, Messages, Opts) ->
    Provider = maps:get(provider, Opts, emqx_a2a_config:provider()),
    case Provider of
        anthropic ->
            chat_stream_anthropic(CallerPid, ApiKey, Model, Messages, Opts);
        openai ->
            chat_stream_openai(CallerPid, ApiKey, Model, Messages, Opts)
    end.

%%--------------------------------------------------------------------
%% Anthropic
%%--------------------------------------------------------------------

chat_stream_anthropic(CallerPid, ApiKey, Model, Messages, Opts) ->
    SystemPrompt = maps:get(system_prompt, Opts, <<>>),
    MaxTokens = maps:get(max_tokens, Opts, 4096),

    Body0 = #{
        <<"model">> => Model,
        <<"max_tokens">> => MaxTokens,
        <<"messages">> => Messages,
        <<"stream">> => true
    },
    Body =
        case SystemPrompt of
            <<>> -> Body0;
            SP -> Body0#{<<"system">> => SP}
        end,

    Headers = [
        {"x-api-key", binary_to_list(ApiKey)},
        {"anthropic-version", ?ANTHROPIC_VERSION},
        {"content-type", "application/json"},
        {"accept", "text/event-stream"}
    ],

    do_stream_request(CallerPid, ?ANTHROPIC_URL, Headers, Body, fun handle_anthropic_event/3).

%%--------------------------------------------------------------------
%% OpenAI
%%--------------------------------------------------------------------

chat_stream_openai(CallerPid, ApiKey, Model, Messages, Opts) ->
    SystemPrompt = maps:get(system_prompt, Opts, <<>>),
    MaxTokens = maps:get(max_tokens, Opts, 4096),

    %% Responses API uses `input` (list of input items) instead of `messages`,
    %% `instructions` instead of a system message, and `max_output_tokens`.
    Body0 = #{
        <<"model">> => Model,
        <<"max_output_tokens">> => MaxTokens,
        <<"input">> => Messages,
        <<"stream">> => true,
        <<"store">> => false
    },
    Body =
        case SystemPrompt of
            <<>> -> Body0;
            SP -> Body0#{<<"instructions">> => SP}
        end,

    Headers = [
        {"authorization", "Bearer " ++ binary_to_list(ApiKey)},
        {"content-type", "application/json"},
        {"accept", "text/event-stream"}
    ],

    do_stream_request(CallerPid, ?OPENAI_URL, Headers, Body, fun handle_openai_event/3).

%%--------------------------------------------------------------------
%% Shared: HTTP request + SSE collection
%%--------------------------------------------------------------------

do_stream_request(CallerPid, Url, Headers, Body, EventHandler) ->
    JsonBody = emqx_utils_json:encode(Body),
    Request = {Url, Headers, "application/json", JsonBody},
    case
        httpc:request(
            post,
            Request,
            [{timeout, 120000}],
            [{sync, false}, {stream, self}]
        )
    of
        {ok, RequestId} ->
            %% {TextAcc, LineBuf} — LineBuf carries incomplete lines across chunks
            collect_sse(CallerPid, RequestId, {[], <<>>}, EventHandler);
        {error, Reason} ->
            CallerPid ! {stream_error, Reason}
    end.

collect_sse(CallerPid, RequestId, {Acc, LineBuf}, EventHandler) ->
    receive
        {http, {RequestId, stream_start, _Headers}} ->
            collect_sse(CallerPid, RequestId, {Acc, LineBuf}, EventHandler);
        {http, {RequestId, stream, BinChunk}} ->
            {Acc1, LineBuf1} = process_sse_chunk(
                CallerPid, <<LineBuf/binary, BinChunk/binary>>, Acc, EventHandler
            ),
            collect_sse(CallerPid, RequestId, {Acc1, LineBuf1}, EventHandler);
        {http, {RequestId, stream_end, _Headers}} ->
            FinalText = iolist_to_binary(Acc),
            CallerPid ! {stream_done, FinalText};
        {http, {RequestId, {error, Reason}}} ->
            CallerPid ! {stream_error, Reason}
    after 120000 ->
        CallerPid ! {stream_error, timeout}
    end.

process_sse_chunk(CallerPid, Chunk, Acc, EventHandler) ->
    %% Split on newlines; the last segment may be incomplete (no trailing \n)
    Lines = binary:split(Chunk, <<"\n">>, [global]),
    %% The last element is either <<>> (chunk ended with \n) or an incomplete line
    {CompleteLines, Remainder} = split_last(Lines),
    Acc1 = process_sse_lines(CallerPid, CompleteLines, Acc, EventHandler, undefined),
    {Acc1, Remainder}.

split_last([]) -> {[], <<>>};
split_last(Lines) -> {lists:droplast(Lines), lists:last(Lines)}.

process_sse_lines(_CallerPid, [], Acc, _EventHandler, _EventType) ->
    Acc;
%% Skip empty lines (SSE event separators)
process_sse_lines(CallerPid, [<<>> | Rest], Acc, EventHandler, EventType) ->
    process_sse_lines(CallerPid, Rest, Acc, EventHandler, EventType);
%% Track the event type from `event: <type>` lines (used by OpenAI Responses API)
process_sse_lines(CallerPid, [<<"event: ", Type/binary>> | Rest], Acc, EventHandler, _EventType) ->
    process_sse_lines(CallerPid, Rest, Acc, EventHandler, Type);
process_sse_lines(CallerPid, [<<"data: ", Json/binary>> | Rest], Acc, EventHandler, EventType) ->
    Acc1 =
        case Json of
            <<"[DONE]">> ->
                Acc;
            _ ->
                try emqx_utils_json:decode(Json, [return_maps]) of
                    Event0 ->
                        %% Inject SSE event type into parsed JSON so handlers can dispatch on it
                        Event =
                            case EventType of
                                undefined -> Event0;
                                _ -> Event0#{<<"_event_type">> => EventType}
                            end,
                        EventHandler(CallerPid, Event, Acc)
                catch
                    _:_ -> Acc
                end
        end,
    process_sse_lines(CallerPid, Rest, Acc1, EventHandler, undefined);
process_sse_lines(CallerPid, [_ | Rest], Acc, EventHandler, EventType) ->
    process_sse_lines(CallerPid, Rest, Acc, EventHandler, EventType).

%%--------------------------------------------------------------------
%% Event handlers: Anthropic
%%--------------------------------------------------------------------

handle_anthropic_event(
    CallerPid,
    #{
        <<"type">> := <<"content_block_delta">>,
        <<"delta">> := #{
            <<"type">> := <<"text_delta">>,
            <<"text">> := Text
        }
    },
    Acc
) ->
    CallerPid ! {stream_chunk, Text},
    [Acc, Text];
handle_anthropic_event(
    CallerPid,
    #{
        <<"type">> := <<"error">>,
        <<"error">> := #{<<"message">> := Msg}
    },
    _Acc
) ->
    CallerPid ! {stream_error, {api_error, Msg}},
    [];
handle_anthropic_event(_CallerPid, _Event, Acc) ->
    Acc.

%%--------------------------------------------------------------------
%% Event handlers: OpenAI Responses API
%%--------------------------------------------------------------------

%% Text delta — the main streaming event
handle_openai_event(
    CallerPid,
    #{
        <<"_event_type">> := <<"response.output_text.delta">>,
        <<"delta">> := Delta
    },
    Acc
) ->
    CallerPid ! {stream_chunk, Delta},
    [Acc, Delta];
%% Response failed
handle_openai_event(CallerPid, #{<<"_event_type">> := <<"response.failed">>} = Event, _Acc) ->
    Msg =
        case Event of
            #{<<"response">> := #{<<"error">> := #{<<"message">> := M}}} -> M;
            _ -> <<"unknown error">>
        end,
    CallerPid ! {stream_error, {api_error, Msg}},
    [];
%% Top-level error object
handle_openai_event(CallerPid, #{<<"error">> := #{<<"message">> := Msg}}, _Acc) ->
    CallerPid ! {stream_error, {api_error, Msg}},
    [];
%% Everything else (response.created, response.output_text.done, response.completed, etc.)
handle_openai_event(_CallerPid, _Event, Acc) ->
    Acc.
