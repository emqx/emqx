-module(alinkcore_supervisor).
-behaviour(supervisor).
-export([init/1, handle_info/2, handle_call/3, handle_cast/2, terminate/2, code_change/3]).
-export([start_link/2, call/3, cast/2]).

-record(state, {mod, sup_state, child_state}).

call(Pid, Msg, Timeout) ->
    gen_server:call(Pid, {call, Msg}, Timeout).

cast(Pid, Info) ->
    gen_server:call(Pid, {cast, Info}).

start_link(Mod, Args) ->
    gen_server:start_link(?MODULE, {self, Mod, Args}, []).

init({self, Mod, Args}) ->
    case Mod:init(Args) of
        {ok, ChildState} ->
            do_init(Mod, [], ChildState);
        {ok, Children, ChildState} ->
            do_init(Mod, Children, ChildState);
        ignore ->
            ignore;
        {stop, Reason} ->
            {stop, Reason}
    end;

init(Children) ->
    {ok, {#{
        strategy => one_for_all,
        intensity => 5,
        period => 30},
        Children}
    }.

do_init(Mod, Children, ChildState) ->
    case supervisor:init({self, ?MODULE, Children}) of
        {ok, SupChild} ->
            {ok, #state{
                mod = Mod,
                child_state = ChildState,
                sup_state = SupChild
            }};
        ignore ->
            ignore;
        {stop, Reason} ->
            {stop, Reason}
    end.

handle_call({call, Msg}, From, #state{ mod = Mod, child_state = ChildState} = State) ->
    case Mod:handle_call(Msg, From, ChildState) of
        {reply, Reply, NewChildState} ->
            {reply, Reply, State#state{child_state = NewChildState}};
        {stop, Reason, Reply, NewChildState} ->
            {stop, Reason, Reply, State#state{child_state = NewChildState}}
    end;
handle_call(Msg, From, #state{sup_state = SupState} = State) ->
    {reply, Reply, NSupState} = supervisor:handle_call(Msg, From, SupState),
    {reply, Reply, State#state{sup_state = NSupState}}.

handle_cast({cast, Info}, #state{mod = Mod, child_state = ChildState } = State) ->
    case Mod:handle_cast(Info, ChildState) of
        {noreply, NewChildState} ->
            {noreply, State#state{child_state = NewChildState}};
        {stop, Reason, NewChildState} ->
            {stop, Reason, State#state{child_state = NewChildState}}
    end;
handle_cast(Info, State) ->
    case supervisor:handle_cast(Info, State#state.sup_state) of
        {noreply, SupState} ->
            {noreply, State#state{sup_state = SupState}};
        {stop, Reason, SupState} ->
            {stop, Reason, State#state{sup_state = SupState}}
    end.

handle_info({'EXIT', Pid, Reason}, #state{sup_state = SupState} = State) ->
    case supervisor:handle_info({'EXIT', Pid, Reason}, SupState) of
        {noreply, SupState} ->
            {noreply, State#state{sup_state = SupState}};
        {stop, Reason, SupState} ->
            {stop, Reason, State#state{sup_state = SupState}}
    end;
handle_info(Info, #state{ mod = Mod, child_state = ChildState } = State) ->
    case Mod:handle_info(Info, ChildState) of
        {noreply, NewChildState} ->
            {noreply, State#state{child_state = NewChildState}};
        {stop, Reason, NewChildState} ->
            {stop, Reason, State#state{child_state = NewChildState}}
    end.

terminate(Reason, #state{ mod = Mod, child_state = ChildState } = State) ->
    ok = Mod:terminate(Reason, ChildState),
    supervisor:terminate(Reason, State#state.sup_state).

code_change(OldVsn, State, Extra) ->
    case supervisor:code_change(OldVsn, State, Extra) of
        {ok, SupState} ->
            {ok, State#state{sup_state = SupState}};
        {error, Reason} ->
            {error, Reason}
    end.
