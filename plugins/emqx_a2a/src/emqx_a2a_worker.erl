%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_worker).

-moduledoc """
Agent worker process. Each worker executes a single task by calling
an LLM API (via emqx_a2a_llm), streams intermediate results to the
MQTT reply topic, and reports completion to the session manager.

IoT features:
- Reads retained messages from `context_topics` before calling the LLM,
  injecting live device state into the prompt.
- After the LLM responds, scans for ```mqtt-action``` fenced blocks
  and auto-publishes them to the broker (device commands, setpoints, etc).
""".

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_a2a.hrl").
-include_lib("emqx/include/emqx.hrl").

-record(state, {
    session_id :: binary(),
    task_id :: binary(),
    agent_cfg :: map(),
    prompt :: binary(),
    context :: list(),
    reply_topic :: binary(),
    correlation :: binary(),
    collected :: iolist()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(#{
    session_id := SessionId,
    task_id := TaskId,
    agent_cfg := AgentCfg,
    prompt := Prompt,
    context := Context,
    reply_topic := ReplyTopic,
    correlation := Correlation
}) ->
    ?LOG(info, #{msg => "worker_start", session_id => SessionId, task_id => TaskId}),
    self() ! run,
    {ok, #state{
        session_id = SessionId,
        task_id = TaskId,
        agent_cfg = AgentCfg,
        prompt = Prompt,
        context = Context,
        reply_topic = ReplyTopic,
        correlation = Correlation,
        collected = []
    }}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(run, State) ->
    run_agent(State);
handle_info({stream_chunk, Text}, State) ->
    #state{
        reply_topic = ReplyTopic,
        correlation = Correlation,
        session_id = SessionId,
        task_id = TaskId,
        collected = Collected
    } = State,
    publish_working(ReplyTopic, Correlation, SessionId, TaskId, Text),
    {noreply, State#state{collected = [Collected, Text]}};
handle_info({stream_done, FinalText}, State) ->
    #state{
        session_id = SessionId,
        task_id = TaskId,
        reply_topic = ReplyTopic,
        correlation = Correlation
    } = State,
    Result =
        case FinalText of
            <<>> -> iolist_to_binary(State#state.collected);
            _ -> FinalText
        end,
    %% Execute any mqtt-action blocks in the response
    Actions = extract_mqtt_actions(Result),
    execute_mqtt_actions(Actions, SessionId, ReplyTopic, Correlation),
    ?LOG(info, #{
        msg => "worker_done",
        session_id => SessionId,
        task_id => TaskId,
        actions_executed => length(Actions)
    }),
    emqx_a2a_session_mgr:task_completed(SessionId, TaskId, Result),
    {stop, normal, State};
handle_info({stream_error, Reason}, State) ->
    #state{session_id = SessionId, task_id = TaskId} = State,
    ?LOG(error, #{
        msg => "worker_llm_error",
        session_id => SessionId,
        task_id => TaskId,
        reason => Reason
    }),
    emqx_a2a_session_mgr:task_failed(SessionId, TaskId, Reason),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal: LLM execution
%%--------------------------------------------------------------------

run_agent(State) ->
    #state{agent_cfg = AgentCfg, prompt = Prompt, context = Context} = State,

    %% Fetch device context from retained messages
    DeviceContext = fetch_device_context(AgentCfg),

    Messages = build_messages(Prompt, Context, DeviceContext),
    Model = resolve_model(AgentCfg),
    MaxTokens = resolve_max_tokens(AgentCfg),
    SystemPrompt = build_system_prompt(AgentCfg),
    ApiKey = emqx_a2a_config:api_key(),

    Self = self(),

    spawn_link(fun() ->
        try
            emqx_a2a_llm:chat_stream(Self, ApiKey, Model, Messages, #{
                system_prompt => SystemPrompt,
                max_tokens => MaxTokens
            })
        catch
            Class:Reason:Stack ->
                ?LOG(error, #{
                    msg => "llm_call_crash",
                    class => Class,
                    reason => Reason,
                    stacktrace => Stack
                }),
                Self ! {stream_error, {Class, Reason}}
        end
    end),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal: device context (retained message reading)
%%--------------------------------------------------------------------

fetch_device_context(AgentCfg) ->
    TopicFilters = maps:get(context_topics, AgentCfg, []),
    lists:flatmap(fun fetch_retained_messages/1, TopicFilters).

fetch_retained_messages(TopicFilter) ->
    try
        Cursor = undefined,
        MatchOpts = #{batch_read_number => all_remaining},
        case emqx_retainer:match_messages(TopicFilter, Cursor, MatchOpts) of
            {ok, Msgs, _NextCursor} ->
                [
                    #{
                        topic => emqx_message:topic(M),
                        payload => emqx_message:payload(M)
                    }
                 || M <- Msgs
                ];
            _ ->
                []
        end
    catch
        _:_ -> []
    end.

%%--------------------------------------------------------------------
%% Internal: message building
%%--------------------------------------------------------------------

build_system_prompt(AgentCfg) ->
    Base = maps:get(system_prompt, AgentCfg, <<>>),
    ContextTopics = maps:get(context_topics, AgentCfg, []),
    case ContextTopics of
        [] ->
            Base;
        _ ->
            ActionInstructions = <<
                "\n\nYou can send MQTT messages to devices by including "
                "fenced action blocks in your response. Format:\n"
                "```mqtt-action\n"
                "{\"topic\": \"devices/dev-01/commands\", "
                "\"payload\": {\"cmd\": \"setSetpoint\", \"params\": {\"temp\": 22}}}\n"
                "```\n"
                "Each block will be published to the MQTT broker automatically. "
                "Use this to send commands, change setpoints, or control devices."
            >>,
            <<Base/binary, ActionInstructions/binary>>
    end.

build_messages(Prompt, Context, DeviceContext) ->
    ContextText = format_context(Context),
    DeviceText = format_device_context(DeviceContext),

    Parts =
        [Prompt] ++
            [
                <<"\n\n## Context from upstream tasks\n\n", CT/binary>>
             || CT <- [ContextText], CT =/= <<>>
            ] ++
            [
                <<"\n\n## Current device state\n\n", DT/binary>>
             || DT <- [DeviceText], DT =/= <<>>
            ],

    UserContent = iolist_to_binary(Parts),
    [#{<<"role">> => <<"user">>, <<"content">> => UserContent}].

format_context([]) ->
    <<>>;
format_context(ContextItems) ->
    Parts = lists:map(
        fun(#{task := TaskId, result := Result}) ->
            <<"### Result from '", TaskId/binary, "':\n", Result/binary, "\n">>
        end,
        ContextItems
    ),
    iolist_to_binary(Parts).

format_device_context([]) ->
    <<>>;
format_device_context(Items) ->
    Parts = lists:map(
        fun(#{topic := Topic, payload := Payload}) ->
            <<"**", Topic/binary, "**:\n```json\n", Payload/binary, "\n```\n">>
        end,
        Items
    ),
    iolist_to_binary(Parts).

%%--------------------------------------------------------------------
%% Internal: mqtt-action extraction and execution
%%--------------------------------------------------------------------

extract_mqtt_actions(Text) ->
    %% Find all ```mqtt-action ... ``` blocks
    case extract_fenced_blocks(Text, <<"mqtt-action">>) of
        [] ->
            [];
        Blocks ->
            lists:filtermap(
                fun(Block) ->
                    try
                        Json = emqx_utils_json:decode(Block, [return_maps]),
                        case Json of
                            #{<<"topic">> := Topic, <<"payload">> := _} when is_binary(Topic) ->
                                {true, Json};
                            _ ->
                                false
                        end
                    catch
                        _:_ -> false
                    end
                end,
                Blocks
            )
    end.

extract_fenced_blocks(Text, Lang) ->
    %% Match ```mqtt-action\n...\n``` patterns
    OpenTag = <<"```", Lang/binary>>,
    CloseTag = <<"```">>,
    extract_fenced_blocks(Text, OpenTag, CloseTag, []).

extract_fenced_blocks(Text, OpenTag, CloseTag, Acc) ->
    case binary:match(Text, OpenTag) of
        {Start, Len} ->
            Rest = binary:part(Text, Start + Len, byte_size(Text) - Start - Len),
            %% Skip to the next newline (end of opening fence)
            ContentStart =
                case binary:match(Rest, <<"\n">>) of
                    {NL, 1} -> binary:part(Rest, NL + 1, byte_size(Rest) - NL - 1);
                    _ -> Rest
                end,
            %% Find closing fence
            case binary:match(ContentStart, CloseTag) of
                {End, _} ->
                    Block = string:trim(binary:part(ContentStart, 0, End)),
                    Remaining = binary:part(
                        ContentStart,
                        End + byte_size(CloseTag),
                        byte_size(ContentStart) - End - byte_size(CloseTag)
                    ),
                    extract_fenced_blocks(Remaining, OpenTag, CloseTag, [Block | Acc]);
                nomatch ->
                    lists:reverse(Acc)
            end;
        nomatch ->
            lists:reverse(Acc)
    end.

execute_mqtt_actions([], _SessionId, _ReplyTopic, _Correlation) ->
    ok;
execute_mqtt_actions(Actions, SessionId, ReplyTopic, Correlation) ->
    lists:foreach(
        fun(#{<<"topic">> := Topic, <<"payload">> := Payload} = Action) ->
            PayloadBin =
                case is_binary(Payload) of
                    true -> Payload;
                    false -> emqx_utils_json:encode(Payload)
                end,
            Qos =
                case maps:get(<<"qos">>, Action, 0) of
                    Q when Q =:= 1; Q =:= 2 -> Q;
                    _ -> 0
                end,
            Retain = maps:get(<<"retain">>, Action, false),
            Msg0 = emqx_message:make(emqx_a2a, Qos, Topic, PayloadBin),
            Msg = emqx_message:set_flag(retain, Retain =:= true, Msg0),
            _ = emqx_broker:safe_publish(Msg),
            ?LOG(info, #{
                msg => "mqtt_action_published",
                session_id => SessionId,
                target_topic => Topic
            }),
            %% Notify caller about the action
            notify_action(ReplyTopic, Correlation, SessionId, Topic, PayloadBin)
        end,
        Actions
    ).

notify_action(ReplyTopic, _Correlation, SessionId, ActionTopic, ActionPayload) ->
    Payload = emqx_utils_json:encode(#{
        type => <<"TaskActionEvent">>,
        task_id => SessionId,
        action => #{
            topic => ActionTopic,
            payload_size => byte_size(ActionPayload)
        }
    }),
    Msg = emqx_message:make(emqx_a2a, ReplyTopic, Payload),
    _ = emqx_broker:safe_publish(Msg),
    ok.

%%--------------------------------------------------------------------
%% Internal: resolve config
%%--------------------------------------------------------------------

resolve_model(#{model := Model}) when Model =/= <<>>, Model =/= undefined ->
    Model;
resolve_model(_) ->
    emqx_a2a_config:default_model().

resolve_max_tokens(#{max_tokens := MT}) when is_integer(MT), MT > 0 ->
    MT;
resolve_max_tokens(_) ->
    emqx_a2a_config:default_max_tokens().

%%--------------------------------------------------------------------
%% Internal: MQTT publishing
%%--------------------------------------------------------------------

publish_working(ReplyTopic, _Correlation, SessionId, TaskId, Text) ->
    Payload = emqx_utils_json:encode(#{
        type => <<"TaskStatusUpdateEvent">>,
        task_id => SessionId,
        status => #{
            state => working,
            message => Text,
            metadata => #{
                task_name => TaskId
            }
        }
    }),
    Msg = emqx_message:make(emqx_a2a, ReplyTopic, Payload),
    _ = emqx_broker:safe_publish(Msg),
    ok.
