%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_test_grpc_server).

%% API
-export([
    update_subscription/2,
    create_subscription/2,
    streaming_pull/2
]).

-export([set_agent/1, clear_agent/0, agent_update/1, agent_set/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AGENT, {?MODULE, agent}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

update_subscription(Req, Meta) ->
    Type = ?FUNCTION_NAME,
    enter_non_streaming(Type, Req, Meta).

create_subscription(Req, Meta) ->
    Type = ?FUNCTION_NAME,
    enter_non_streaming(Type, Req, Meta).

streaming_pull(Req, Meta) ->
    loop(Req, Meta).

set_agent(AgentPid) ->
    persistent_term:put(?AGENT, AgentPid).

clear_agent() ->
    persistent_term:erase(?AGENT).

agent_update(Fn) ->
    AgentPid = get_agent(),
    emqx_utils_agent:update(AgentPid, Fn).

agent_set(St) ->
    AgentPid = get_agent(),
    emqx_utils_agent:set(AgentPid, St).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

get_agent() ->
    persistent_term:get(?AGENT).

get_agent_action(Key, Default) ->
    Agent = get_agent(),
    emqx_utils_agent:get_and_update(Agent, fun(Old) ->
        case Old of
            #{Key := [default]} ->
                {Default, Old};
            #{Key := default} ->
                {Default, Old};
            #{Key := [default | Actions]} ->
                {Default, Old#{Key := Actions}};
            #{Key := [Action]} ->
                {Action, Old};
            #{Key := [Action | Actions]} ->
                {Action, Old#{Key := Actions}};
            #{Key := Action} when not is_list(Action) ->
                {Action, Old};
            #{} ->
                {Default, Old}
        end
    end).

enter_non_streaming(Type, Req, Meta) ->
    ct:pal("grpc server ~p got:\n ~s: ~p", [self(), Type, Req]),
    Default = default_action(Type),
    Action = get_agent_action(Type, Default),
    ct:pal("agent action: ~p", [Action]),
    handle_msg_action_non_streaming(Action, Type, Req, Meta).

loop(Req0, Meta) ->
    case get_agent_action(enter_first_req, continue) of
        continue ->
            {More, Msgs, Req} = grpc_stream:recv(Req0),
            loop_msgs(Msgs, More, Meta, Req)
    end.

loop_msgs([], eos, _Meta, Req0) ->
    {ok, Req0};
loop_msgs([], more, Meta, Req0) ->
    ct:pal("grpc server ~p reached end of batch", [self()]),
    Action = get_agent_action(batch_done, continue),
    handle_batch_done_action(Action, Meta, Req0);
loop_msgs([Msg | Rest], More, Meta, Req) ->
    ct:pal("grpc server ~p got:\n  ~p", [self(), Msg]),
    Type = streaming_pull,
    Default = default_action(Type),
    Action = get_agent_action(Type, Default),
    ct:pal("agent action: ~p", [Action]),
    handle_msg_action(Action, Type, Msg, Rest, More, Meta, Req).

handle_msg_action_non_streaming(Action, Type, Req, Meta) ->
    case Action of
        default ->
            Default = default_action(Type),
            handle_msg_action_non_streaming(Default, Type, Req, Meta);
        {run, Fn} ->
            Ctx = #{req => Req, meta => Meta},
            NextAction = Fn(Ctx),
            handle_msg_action_non_streaming(NextAction, Type, Req, Meta);
        {shutdown, Code, Message} ->
            {Code, Message, Req};
        {reply, Resp} ->
            {ok, Resp, Meta};
        {reply, Code, Resp, Meta1} ->
            {Code, Resp, Meta1};
        {reply_error, Code} ->
            {error, Code};
        {reply_error, Code, Msg} ->
            {error, Code, Msg};
        {ask, Pid} ->
            ct:pal("grpc server ~p asking ~p what to do with ~p", [self(), Pid, Type]),
            Alias = alias([reply]),
            Ctx = #{msg => Req},
            Pid ! {Type, Alias, Ctx},
            NextAction =
                receive
                    {Alias, NextAction1} ->
                        NextAction1
                end,
            ct:pal("grpc server ~p got answer from ~p: ~p", [self(), Pid, NextAction]),
            handle_msg_action_non_streaming(NextAction, Type, Req, Meta)
    end.

handle_msg_action(Action, Type, Msg, Rest, More, Meta, Req) ->
    case Action of
        default ->
            Default = default_action(Type),
            handle_msg_action(Default, Type, Msg, Rest, More, Meta, Req);
        continue ->
            loop_msgs(Rest, More, Meta, Req);
        {spy, Dest} ->
            Ctx = #{msg => Msg, grpc_req => Req, meta => Meta},
            Dest ! {Type, Ctx},
            Default = default_action(Type),
            handle_msg_action(Default, Type, Msg, Rest, More, Meta, Req);
        {run, Fn} ->
            Ctx = #{msg => Msg, grpc_req => Req, meta => Meta},
            NextAction = Fn(Ctx),
            handle_msg_action(NextAction, Type, Msg, Rest, More, Meta, Req);
        {shutdown, Code, Message} ->
            {Code, Message, Req};
        {reply, Messages} ->
            grpc_stream:reply(Req, Messages),
            loop_msgs(Rest, More, Meta, Req);
        {ask, Pid} ->
            ct:pal("grpc server ~p asking ~p what to do with ~p", [self(), Pid, Type]),
            Alias = alias([reply]),
            Ctx = #{msg => Msg, grpc_req => Req},
            Pid ! {Type, Alias, Ctx},
            NextAction =
                receive
                    {Alias, NextAction1} ->
                        NextAction1
                end,
            ct:pal("grpc server ~p got answer from ~p: ~p", [self(), Pid, NextAction]),
            handle_msg_action(NextAction, Type, Msg, Rest, More, Meta, Req)
    end.

handle_batch_done_action(Action, Meta, Req0) ->
    case Action of
        continue ->
            ct:pal("grpc server ~p waiting for more", [self()]),
            {More, Msgs, Req} = grpc_stream:recv(Req0),
            ct:pal("grpc server ~p looping", [self()]),
            loop_msgs(Msgs, More, Meta, Req);
        {send, Replies} ->
            grpc_stream:reply(Req0, Replies),
            loop_msgs([], more, Meta, Req0);
        {exit, Reason} ->
            exit(Reason);
        {shutdown, Code, Message} ->
            {Code, Message, Req0};
        {run, Fn} ->
            NextAction = Fn(Req0, Meta),
            handle_batch_done_action(NextAction, Meta, Req0);
        {ask, Pid} ->
            ct:pal("grpc server ~p asking ~p what to do", [self(), Pid]),
            Alias = alias([reply]),
            Pid ! {batch_done, Alias},
            receive
                {Alias, NextAction} ->
                    handle_batch_done_action(NextAction, Meta, Req0)
            end
    end.

default_action(update_subscription) ->
    {reply, #{}};
default_action(create_subscription) ->
    {reply, #{}};
default_action(streaming_pull) ->
    continue.
