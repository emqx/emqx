%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_invocation).

-behaviour(gen_server).

-export([start_link/6]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("emqx/include/logger.hrl").

-record(state, {
    type :: binary(),
    tool_id :: binary(),
    module :: module(),
    context :: map(),
    request :: map(),
    timer_ref :: reference() | undefined,
    exec_pid :: pid() | undefined,
    exec_ref :: reference() | undefined,
    killed = false :: boolean(),
    replied = false :: boolean()
}).

start_link(Type, ToolId, Module, Context, Request, Timeout) ->
    gen_server:start_link(?MODULE, {Type, ToolId, Module, Context, Request, Timeout}, []).

init({Type, ToolId, Module, Context, Request, Timeout}) ->
    TimerRef = erlang:send_after(Timeout, self(), invoke_timeout),
    {Pid, MonRef} = spawn_monitor(fun() ->
        exit({invoke, Module:handle_invoke(Context, Request)})
    end),
    {ok, #state{
        type = Type,
        tool_id = ToolId,
        module = Module,
        context = Context,
        request = Request,
        timer_ref = TimerRef,
        exec_pid = Pid,
        exec_ref = MonRef
    }}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(invoke_timeout, #state{replied = true} = State) ->
    {stop, normal, State};
handle_info(invoke_timeout, #state{exec_pid = Pid, timer_ref = TRef} = State) ->
    erlang:cancel_timer(TRef),
    case Pid of
        undefined -> ok;
        _ -> exit(Pid, kill)
    end,
    ?SLOG(warning, #{
        msg => "tool_invocation_timeout",
        type => State#state.type,
        tool_id => State#state.tool_id
    }),
    {noreply, State#state{timer_ref = undefined, killed = true}};
handle_info(
    {'DOWN', Ref, process, Pid, {invoke, _Result}},
    #state{exec_ref = Ref, exec_pid = Pid, killed = true} = State
) ->
    publish_reply(State, #{<<"status">> => <<"error">>, <<"reason">> => <<"timeout">>}),
    {stop, normal, State#state{exec_pid = undefined, exec_ref = undefined, replied = true}};
handle_info(
    {'DOWN', Ref, process, Pid, {invoke, Result}},
    #state{exec_ref = Ref, exec_pid = Pid} = State
) ->
    erlang:cancel_timer(State#state.timer_ref),
    ReplyData =
        case Result of
            ok ->
                #{<<"status">> => <<"ok">>};
            {ok, Data} ->
                #{<<"status">> => <<"ok">>, <<"result">> => Data};
            {ok, Data, Attachments} ->
                #{
                    <<"status">> => <<"ok">>,
                    <<"result">> => Data,
                    <<"attachments">> => Attachments
                };
            {error, Reason} ->
                #{<<"status">> => <<"error">>, <<"reason">> => Reason}
        end,
    publish_reply(State, ReplyData),
    {stop, normal, State#state{
        exec_pid = undefined, exec_ref = undefined, timer_ref = undefined, replied = true
    }};
handle_info(
    {'DOWN', Ref, process, Pid, _Reason},
    #state{exec_ref = Ref, exec_pid = Pid, killed = true} = State
) ->
    publish_reply(State, #{<<"status">> => <<"error">>, <<"reason">> => <<"timeout">>}),
    {stop, normal, State#state{exec_pid = undefined, exec_ref = undefined, replied = true}};
handle_info(
    {'DOWN', Ref, process, Pid, Reason},
    #state{exec_ref = Ref, exec_pid = Pid} = State
) ->
    erlang:cancel_timer(State#state.timer_ref),
    ?SLOG(error, #{
        msg => "tool_invocation_crashed",
        type => State#state.type,
        tool_id => State#state.tool_id,
        reason => Reason
    }),
    publish_reply(State, #{<<"status">> => <<"error">>, <<"reason">> => format_reason(Reason)}),
    {stop, normal, State#state{
        exec_pid = undefined, exec_ref = undefined, timer_ref = undefined, replied = true
    }};
handle_info({'DOWN', _, process, _, _}, State) ->
    %% stale DOWN after timeout kill
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

publish_reply(State, ReplyData) ->
    emqx_agent_tool_helpers:publish_reply(
        State#state.type,
        State#state.tool_id,
        State#state.request,
        ReplyData
    ).

format_reason(Reason) ->
    emqx_agent_tool_helpers:format_error(Reason).
