%% This file is a copy of supervisor.erl from the R13B-3 Erlang/OTP
%% distribution, with the following modifications:
%%
%% 1) the module name is supervisor2
%%
%% 2) there is a new strategy called
%%    simple_one_for_one_terminate. This is exactly the same as for
%%    simple_one_for_one, except that children *are* explicitly
%%    terminated as per the shutdown component of the child_spec.
%%
%% 3) child specifications can contain, as the restart type, a tuple
%%    {permanent, Delay} | {transient, Delay} | {intrinsic, Delay}
%%    where Delay >= 0 (see point (4) below for intrinsic). The delay,
%%    in seconds, indicates what should happen if a child, upon being
%%    restarted, exceeds the MaxT and MaxR parameters. Thus, if a
%%    child exits, it is restarted as normal. If it exits sufficiently
%%    quickly and often to exceed the boundaries set by the MaxT and
%%    MaxR parameters, and a Delay is specified, then rather than
%%    stopping the supervisor, the supervisor instead continues and
%%    tries to start up the child again, Delay seconds later.
%%
%%    Note that you can never restart more frequently than the MaxT
%%    and MaxR parameters allow: i.e. you must wait until *both* the
%%    Delay has passed *and* the MaxT and MaxR parameters allow the
%%    child to be restarted.
%%
%%    Also note that the Delay is a *minimum*. There is no guarantee
%%    that the child will be restarted within that time, especially if
%%    other processes are dying and being restarted at the same time -
%%    essentially we have to wait for the delay to have passed and for
%%    the MaxT and MaxR parameters to permit the child to be
%%    restarted. This may require waiting for longer than Delay.
%%
%%    Sometimes, you may wish for a transient or intrinsic child to
%%    exit abnormally so that it gets restarted, but still log
%%    nothing. gen_server will log any exit reason other than
%%    'normal', 'shutdown' or {'shutdown', _}. Thus the exit reason of
%%    {'shutdown', 'restart'} is interpreted to mean you wish the
%%    child to be restarted according to the delay parameters, but
%%    gen_server will not log the error. Thus from gen_server's
%%    perspective it's a normal exit, whilst from supervisor's
%%    perspective, it's an abnormal exit.
%%
%% 4) Added an 'intrinsic' restart type. Like the transient type, this
%%    type means the child should only be restarted if the child exits
%%    abnormally. Unlike the transient type, if the child exits
%%    normally, the supervisor itself also exits normally. If the
%%    child is a supervisor and it exits normally (i.e. with reason of
%%    'shutdown') then the child's parent also exits normally.
%%
%% 5) normal, and {shutdown, _} exit reasons are all treated the same
%%    (i.e. are regarded as normal exits)
%%
%% All modifications are (C) 2010-2012 VMware, Inc.
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2009. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(supervisor2).

-behaviour(gen_server).

%% External exports
-export([start_link/2,start_link/3,
	 start_child/2, restart_child/2,
	 delete_child/2, terminate_child/2,
	 which_children/1, find_child/2,
	 check_childspecs/1]).

%% Internal exports
-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3]).
-export([handle_cast/2]).

-define(DICT, dict).

-record(state, {name,
		strategy,
		children = [],
		dynamics = ?DICT:new(),
		intensity,
		period,
		restarts = [],
	        module,
	        args}).

-record(child, {pid = undefined,  % pid is undefined when child is not running
		name,
		mfa,
		restart_type,
		shutdown,
		child_type,
		modules = []}).

-define(is_simple(State), State#state.strategy =:= simple_one_for_one orelse
        State#state.strategy =:= simple_one_for_one_terminate).
-define(is_terminate_simple(State),
        State#state.strategy =:= simple_one_for_one_terminate).

-ifdef(use_specs).

%%--------------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------------

-export_type([child_spec/0, startchild_ret/0, strategy/0, sup_name/0]).

-type child() :: 'undefined' | pid().
-type child_id() :: term().
-type mfargs() :: {M :: module(), F :: atom(), A :: [term()] | undefined}.
-type modules() :: [module()] | 'dynamic'.
-type delay() :: non_neg_integer().
-type restart() :: 'permanent' | 'transient' | 'temporary' | 'intrinsic'
                 | {'permanent', delay()} | {'transient', delay()}
                 | {'intrinsic', delay()}.
-type shutdown() :: 'brutal_kill' | timeout().
-type worker() :: 'worker' | 'supervisor'.
-type sup_name() :: {'local', Name :: atom()} | {'global', Name :: atom()}.
-type sup_ref() :: (Name :: atom())
                  | {Name :: atom(), Node :: node()}
                  | {'global', Name :: atom()}
                  | pid().
-type child_spec() :: {Id :: child_id(),
                       StartFunc :: mfargs(),
                       Restart :: restart(),
                       Shutdown :: shutdown(),
                       Type :: worker(),
                       Modules :: modules()}.


-type strategy() :: 'one_for_all' | 'one_for_one'
                  | 'rest_for_one' | 'simple_one_for_one'
                  | 'simple_one_for_one_terminate'.

-type child_rec() :: #child{pid :: child() | {restarting,pid()} | [pid()],
                            name :: child_id(),
                            mfa :: mfargs(),
                            restart_type :: restart(),
                            shutdown :: shutdown(),
                            child_type :: worker(),
                            modules :: modules()}.

-type state() :: #state{strategy :: strategy(),
                        children :: [child_rec()],
                        dynamics :: ?DICT(),
                        intensity :: non_neg_integer(),
                        period :: pos_integer()}.

%%--------------------------------------------------------------------------
%% Callback behaviour
%%--------------------------------------------------------------------------

-callback init(Args :: term()) ->
    {ok, {{RestartStrategy :: strategy(),
           MaxR :: non_neg_integer(),
           MaxT :: non_neg_integer()},
           [ChildSpec :: child_spec()]}}
    | ignore.

%%--------------------------------------------------------------------------
%% Specs
%%--------------------------------------------------------------------------

-type startchild_err() :: 'already_present'
			| {'already_started', Child :: child()} | term().
-type startchild_ret() :: {'ok', Child :: child()}
                        | {'ok', Child :: child(), Info :: term()}
			| {'error', startchild_err()}.

-spec start_child(SupRef, ChildSpec) -> startchild_ret() when
      SupRef :: sup_ref(),
      ChildSpec :: child_spec() | (List :: [term()]).

-spec restart_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: {'ok', Child :: child()}
              | {'ok', Child :: child(), Info :: term()}
              | {'error', Error},
      Error :: 'running' | 'not_found' | 'simple_one_for_one' | term().

-spec delete_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'running' | 'not_found' | 'simple_one_for_one'.

-spec terminate_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: pid() | child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'not_found' | 'simple_one_for_one'.

-spec which_children(SupRef) -> [{Id,Child,Type,Modules}] when
      SupRef :: sup_ref(),
      Id :: child_id() | 'undefined',
      Child :: child(),
      Type :: worker(),
      Modules :: modules().

-spec check_childspecs(ChildSpecs) -> Result when
      ChildSpecs :: [child_spec()],
      Result :: 'ok' | {'error', Error :: term()}.

-type init_sup_name() :: sup_name() | 'self'.

-type stop_rsn() :: 'shutdown' | {'bad_return', {module(),'init', term()}}
                  | {'bad_start_spec', term()} | {'start_spec', term()}
                  | {'supervisor_data', term()}.

-spec init({init_sup_name(), module(), [term()]}) ->
        {'ok', state()} | 'ignore' | {'stop', stop_rsn()}.

-type call() :: 'which_children'.
-spec handle_call(call(), term(), state()) -> {'reply', term(), state()}.

-spec handle_cast('null', state()) -> {'noreply', state()}.

-spec handle_info(term(), state()) ->
        {'noreply', state()} | {'stop', 'shutdown', state()}.

-spec terminate(term(), state()) -> 'ok'.

-spec code_change(term(), state(), term()) ->
        {'ok', state()} | {'error', term()}.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%% ---------------------------------------------------
%%% This is a general process supervisor built upon gen_server.erl.
%%% Servers/processes should/could also be built using gen_server.erl.
%%% SupName = {local, atom()} | {global, atom()}.
%%% ---------------------------------------------------
start_link(Mod, Args) ->
    gen_server:start_link(?MODULE, {self, Mod, Args}, []).

start_link(SupName, Mod, Args) ->
    gen_server:start_link(SupName, ?MODULE, {SupName, Mod, Args}, []).

%%% ---------------------------------------------------
%%% Interface functions.
%%% ---------------------------------------------------
start_child(Supervisor, ChildSpec) ->
    call(Supervisor, {start_child, ChildSpec}).

restart_child(Supervisor, Name) ->
    call(Supervisor, {restart_child, Name}).

delete_child(Supervisor, Name) ->
    call(Supervisor, {delete_child, Name}).

%%-----------------------------------------------------------------
%% Func: terminate_child/2
%% Returns: ok | {error, Reason}
%%          Note that the child is *always* terminated in some
%%          way (maybe killed).
%%-----------------------------------------------------------------
terminate_child(Supervisor, Name) ->
    call(Supervisor, {terminate_child, Name}).

which_children(Supervisor) ->
    call(Supervisor, which_children).

find_child(Supervisor, Name) ->
    [Pid || {Name1, Pid, _Type, _Modules} <- which_children(Supervisor),
            Name1 =:= Name].

call(Supervisor, Req) ->
    gen_server:call(Supervisor, Req, infinity).

check_childspecs(ChildSpecs) when is_list(ChildSpecs) ->
    case check_startspec(ChildSpecs) of
	{ok, _} -> ok;
	Error -> {error, Error}
    end;
check_childspecs(X) -> {error, {badarg, X}}.

%%% ---------------------------------------------------
%%%
%%% Initialize the supervisor.
%%%
%%% ---------------------------------------------------
init({SupName, Mod, Args}) ->
    process_flag(trap_exit, true),
    case Mod:init(Args) of
	{ok, {SupFlags, StartSpec}} ->
	    case init_state(SupName, SupFlags, Mod, Args) of
		{ok, State} when ?is_simple(State) ->
		    init_dynamic(State, StartSpec);
		{ok, State} ->
		    init_children(State, StartSpec);
		Error ->
		    {stop, {supervisor_data, Error}}
	    end;
	ignore ->
	    ignore;
	Error ->
	    {stop, {bad_return, {Mod, init, Error}}}
    end.

init_children(State, StartSpec) ->
    SupName = State#state.name,
    case check_startspec(StartSpec) of
        {ok, Children} ->
            case start_children(Children, SupName) of
                {ok, NChildren} ->
                    {ok, State#state{children = NChildren}};
                {error, NChildren} ->
                    terminate_children(NChildren, SupName),
                    {stop, shutdown}
            end;
        Error ->
            {stop, {start_spec, Error}}
    end.

init_dynamic(State, [StartSpec]) ->
    case check_startspec([StartSpec]) of
        {ok, Children} ->
	    {ok, State#state{children = Children}};
        Error ->
            {stop, {start_spec, Error}}
    end;
init_dynamic(_State, StartSpec) ->
    {stop, {bad_start_spec, StartSpec}}.

%%-----------------------------------------------------------------
%% Func: start_children/2
%% Args: Children = [#child] in start order
%%       SupName = {local, atom()} | {global, atom()} | {pid(),Mod}
%% Purpose: Start all children.  The new list contains #child's
%%          with pids.
%% Returns: {ok, NChildren} | {error, NChildren}
%%          NChildren = [#child] in termination order (reversed
%%                        start order)
%%-----------------------------------------------------------------
start_children(Children, SupName) -> start_children(Children, [], SupName).

start_children([Child|Chs], NChildren, SupName) ->
    case do_start_child(SupName, Child) of
	{ok, Pid} ->
	    start_children(Chs, [Child#child{pid = Pid}|NChildren], SupName);
	{ok, Pid, _Extra} ->
	    start_children(Chs, [Child#child{pid = Pid}|NChildren], SupName);
	{error, Reason} ->
	    report_error(start_error, Reason, Child, SupName),
	    {error, lists:reverse(Chs) ++ [Child | NChildren]}
    end;
start_children([], NChildren, _SupName) ->
    {ok, NChildren}.

do_start_child(SupName, Child) ->
    #child{mfa = {M, F, A}} = Child,
    case catch apply(M, F, A) of
	{ok, Pid} when is_pid(Pid) ->
	    NChild = Child#child{pid = Pid},
	    report_progress(NChild, SupName),
	    {ok, Pid};
	{ok, Pid, Extra} when is_pid(Pid) ->
	    NChild = Child#child{pid = Pid},
	    report_progress(NChild, SupName),
	    {ok, Pid, Extra};
	ignore ->
	    {ok, undefined};
	{error, What} -> {error, What};
	What -> {error, What}
    end.

do_start_child_i(M, F, A) ->
    case catch apply(M, F, A) of
	{ok, Pid} when is_pid(Pid) ->
	    {ok, Pid};
	{ok, Pid, Extra} when is_pid(Pid) ->
	    {ok, Pid, Extra};
	ignore ->
	    {ok, undefined};
	{error, Error} ->
	    {error, Error};
	What ->
	    {error, What}
    end.


%%% ---------------------------------------------------
%%%
%%% Callback functions.
%%%
%%% ---------------------------------------------------
handle_call({start_child, EArgs}, _From, State) when ?is_simple(State) ->
    #child{mfa = {M, F, A}} = hd(State#state.children),
    Args = A ++ EArgs,
    case do_start_child_i(M, F, Args) of
        {ok, undefined} ->
            {reply, {ok, undefined}, State};
	{ok, Pid} ->
	    NState = State#state{dynamics =
				 ?DICT:store(Pid, Args, State#state.dynamics)},
	    {reply, {ok, Pid}, NState};
	{ok, Pid, Extra} ->
	    NState = State#state{dynamics =
				 ?DICT:store(Pid, Args, State#state.dynamics)},
	    {reply, {ok, Pid, Extra}, NState};
	What ->
	    {reply, What, State}
    end;

%%% The requests terminate_child, delete_child and restart_child are
%%% invalid for simple_one_for_one and simple_one_for_one_terminate
%%% supervisors.
handle_call({_Req, _Data}, _From, State) when ?is_simple(State) ->
    {reply, {error, State#state.strategy}, State};

handle_call({start_child, ChildSpec}, _From, State) ->
    case check_childspec(ChildSpec) of
	{ok, Child} ->
	    {Resp, NState} = handle_start_child(Child, State),
	    {reply, Resp, NState};
	What ->
	    {reply, {error, What}, State}
    end;

handle_call({restart_child, Name}, _From, State) ->
    case get_child(Name, State) of
	{value, Child} when Child#child.pid =:= undefined ->
	    case do_start_child(State#state.name, Child) of
		{ok, Pid} ->
		    NState = replace_child(Child#child{pid = Pid}, State),
		    {reply, {ok, Pid}, NState};
		{ok, Pid, Extra} ->
		    NState = replace_child(Child#child{pid = Pid}, State),
		    {reply, {ok, Pid, Extra}, NState};
		Error ->
		    {reply, Error, State}
	    end;
	{value, _} ->
	    {reply, {error, running}, State};
	_ ->
	    {reply, {error, not_found}, State}
    end;

handle_call({delete_child, Name}, _From, State) ->
    case get_child(Name, State) of
	{value, Child} when Child#child.pid =:= undefined ->
	    NState = remove_child(Child, State),
	    {reply, ok, NState};
	{value, _} ->
	    {reply, {error, running}, State};
	_ ->
	    {reply, {error, not_found}, State}
    end;

handle_call({terminate_child, Name}, _From, State) ->
    case get_child(Name, State) of
	{value, Child} ->
	    NChild = do_terminate(Child, State#state.name),
	    {reply, ok, replace_child(NChild, State)};
	_ ->
	    {reply, {error, not_found}, State}
    end;

handle_call(which_children, _From, State) when ?is_simple(State) ->
    [#child{child_type = CT, modules = Mods}] = State#state.children,
    Reply = lists:map(fun ({Pid, _}) -> {undefined, Pid, CT, Mods} end,
		      ?DICT:to_list(State#state.dynamics)),
    {reply, Reply, State};

handle_call(which_children, _From, State) ->
    Resp =
	lists:map(fun (#child{pid = Pid, name = Name,
			     child_type = ChildType, modules = Mods}) ->
		    {Name, Pid, ChildType, Mods}
		  end,
		  State#state.children),
    {reply, Resp, State}.

%%% Hopefully cause a function-clause as there is no API function
%%% that utilizes cast.
handle_cast(null, State) ->
    error_logger:error_msg("ERROR: Supervisor received cast-message 'null'~n",
			   []),

    {noreply, State}.

handle_info({delayed_restart, {RestartType, Reason, Child}}, State)
  when ?is_simple(State) ->
    {ok, NState} = do_restart(RestartType, Reason, Child, State),
    {noreply, NState};
handle_info({delayed_restart, {RestartType, Reason, Child}}, State) ->
    case get_child(Child#child.name, State) of
        {value, Child1} ->
            {ok, NState} = do_restart(RestartType, Reason, Child1, State),
            {noreply, NState};
        _ ->
            {noreply, State}
    end;

%%
%% Take care of terminated children.
%%
handle_info({'EXIT', Pid, Reason}, State) ->
    case restart_child(Pid, Reason, State) of
	{ok, State1} ->
	    {noreply, State1};
	{shutdown, State1} ->
	    {stop, shutdown, State1}
    end;

handle_info(Msg, State) ->
    error_logger:error_msg("Supervisor received unexpected message: ~p~n",
			   [Msg]),
    {noreply, State}.
%%
%% Terminate this server.
%%
terminate(_Reason, State) when ?is_terminate_simple(State) ->
    terminate_simple_children(
      hd(State#state.children), State#state.dynamics, State#state.name),
    ok;
terminate(_Reason, State) ->
    terminate_children(State#state.children, State#state.name),
    ok.

%%
%% Change code for the supervisor.
%% Call the new call-back module and fetch the new start specification.
%% Combine the new spec. with the old. If the new start spec. is
%% not valid the code change will not succeed.
%% Use the old Args as argument to Module:init/1.
%% NOTE: This requires that the init function of the call-back module
%%       does not have any side effects.
%%
code_change(_, State, _) ->
    case (State#state.module):init(State#state.args) of
	{ok, {SupFlags, StartSpec}} ->
	    case catch check_flags(SupFlags) of
		ok ->
		    {Strategy, MaxIntensity, Period} = SupFlags,
                    update_childspec(State#state{strategy = Strategy,
                                                 intensity = MaxIntensity,
                                                 period = Period},
                                     StartSpec);
		Error ->
		    {error, Error}
	    end;
	ignore ->
	    {ok, State};
	Error ->
	    Error
    end.

check_flags({Strategy, MaxIntensity, Period}) ->
    validStrategy(Strategy),
    validIntensity(MaxIntensity),
    validPeriod(Period),
    ok;
check_flags(What) ->
    {bad_flags, What}.

update_childspec(State, StartSpec)  when ?is_simple(State) ->
    case check_startspec(StartSpec) of
        {ok, [Child]} ->
            {ok, State#state{children = [Child]}};
        Error ->
            {error, Error}
    end;

update_childspec(State, StartSpec) ->
    case check_startspec(StartSpec) of
	{ok, Children} ->
	    OldC = State#state.children, % In reverse start order !
	    NewC = update_childspec1(OldC, Children, []),
	    {ok, State#state{children = NewC}};
        Error ->
	    {error, Error}
    end.

update_childspec1([Child|OldC], Children, KeepOld) ->
    case update_chsp(Child, Children) of
	{ok,NewChildren} ->
	    update_childspec1(OldC, NewChildren, KeepOld);
	false ->
	    update_childspec1(OldC, Children, [Child|KeepOld])
    end;
update_childspec1([], Children, KeepOld) ->
    % Return them in (keeped) reverse start order.
    lists:reverse(Children ++ KeepOld).

update_chsp(OldCh, Children) ->
    case lists:map(fun (Ch) when OldCh#child.name =:= Ch#child.name ->
			   Ch#child{pid = OldCh#child.pid};
		      (Ch) ->
			   Ch
		   end,
		   Children) of
	Children ->
	    false;  % OldCh not found in new spec.
	NewC ->
	    {ok, NewC}
    end.

%%% ---------------------------------------------------
%%% Start a new child.
%%% ---------------------------------------------------

handle_start_child(Child, State) ->
    case get_child(Child#child.name, State) of
	false ->
	    case do_start_child(State#state.name, Child) of
		{ok, Pid} ->
		    Children = State#state.children,
		    {{ok, Pid},
		     State#state{children =
				 [Child#child{pid = Pid}|Children]}};
		{ok, Pid, Extra} ->
		    Children = State#state.children,
		    {{ok, Pid, Extra},
		     State#state{children =
				 [Child#child{pid = Pid}|Children]}};
		{error, What} ->
		    {{error, {What, Child}}, State}
	    end;
	{value, OldChild} when OldChild#child.pid =/= undefined ->
	    {{error, {already_started, OldChild#child.pid}}, State};
	{value, _OldChild} ->
	    {{error, already_present}, State}
    end.

%%% ---------------------------------------------------
%%% Restart. A process has terminated.
%%% Returns: {ok, #state} | {shutdown, #state}
%%% ---------------------------------------------------

restart_child(Pid, Reason, State) when ?is_simple(State) ->
    case ?DICT:find(Pid, State#state.dynamics) of
	{ok, Args} ->
	    [Child] = State#state.children,
	    RestartType = Child#child.restart_type,
	    {M, F, _} = Child#child.mfa,
	    NChild = Child#child{pid = Pid, mfa = {M, F, Args}},
	    do_restart(RestartType, Reason, NChild, State);
	error ->
	    {ok, State}
    end;
restart_child(Pid, Reason, State) ->
    Children = State#state.children,
    case lists:keysearch(Pid, #child.pid, Children) of
	{value, Child} ->
	    RestartType = Child#child.restart_type,
	    do_restart(RestartType, Reason, Child, State);
	_ ->
	    {ok, State}
    end.

do_restart({permanent = RestartType, Delay}, Reason, Child, State) ->
    do_restart_delay({RestartType, Delay}, Reason, Child, State);
do_restart(permanent, Reason, Child, State) ->
    report_error(child_terminated, Reason, Child, State#state.name),
    restart(Child, State);
do_restart(Type, normal, Child, State) ->
    del_child_and_maybe_shutdown(Type, Child, State);
do_restart({RestartType, Delay}, {shutdown, restart} = Reason, Child, State)
  when RestartType =:= transient orelse RestartType =:= intrinsic ->
    do_restart_delay({RestartType, Delay}, Reason, Child, State);
do_restart(Type, {shutdown, _}, Child, State) ->
    del_child_and_maybe_shutdown(Type, Child, State);
do_restart(Type, shutdown, Child = #child{child_type = supervisor}, State) ->
    del_child_and_maybe_shutdown(Type, Child, State);
do_restart({RestartType, Delay}, Reason, Child, State)
  when RestartType =:= transient orelse RestartType =:= intrinsic ->
    do_restart_delay({RestartType, Delay}, Reason, Child, State);
do_restart(Type, Reason, Child, State) when Type =:= transient orelse
                                            Type =:= intrinsic ->
    report_error(child_terminated, Reason, Child, State#state.name),
    restart(Child, State);
do_restart(temporary, Reason, Child, State) ->
    report_error(child_terminated, Reason, Child, State#state.name),
    NState = state_del_child(Child, State),
    {ok, NState}.

do_restart_delay({RestartType, Delay}, Reason, Child, State) ->
    case restart1(Child, State) of
        {ok, NState} ->
            {ok, NState};
        {terminate, NState} ->
            _TRef = erlang:send_after(trunc(Delay*1000), self(),
                                      {delayed_restart,
                                       {{RestartType, Delay}, Reason, Child}}),
            {ok, state_del_child(Child, NState)}
    end.

del_child_and_maybe_shutdown(intrinsic, Child, State) ->
    {shutdown, state_del_child(Child, State)};
del_child_and_maybe_shutdown({intrinsic, _Delay}, Child, State) ->
    {shutdown, state_del_child(Child, State)};
del_child_and_maybe_shutdown(_, Child, State) ->
    {ok, state_del_child(Child, State)}.

restart(Child, State) ->
    case add_restart(State) of
	{ok, NState} ->
	    restart(NState#state.strategy, Child, NState, fun restart/2);
	{terminate, NState} ->
	    report_error(shutdown, reached_max_restart_intensity,
			 Child, State#state.name),
	    {shutdown, state_del_child(Child, NState)}
    end.

restart1(Child, State) ->
    case add_restart(State) of
	{ok, NState} ->
	    restart(NState#state.strategy, Child, NState, fun restart1/2);
	{terminate, _NState} ->
            %% we've reached the max restart intensity, but the
            %% add_restart will have added to the restarts
            %% field. Given we don't want to die here, we need to go
            %% back to the old restarts field otherwise we'll never
            %% attempt to restart later.
            {terminate, State}
    end.

restart(Strategy, Child, State, Restart)
  when Strategy =:= simple_one_for_one orelse
       Strategy =:= simple_one_for_one_terminate ->
    #child{mfa = {M, F, A}} = Child,
    Dynamics = ?DICT:erase(Child#child.pid, State#state.dynamics),
    case do_start_child_i(M, F, A) of
        {ok, undefined} ->
            {ok, State};
	{ok, Pid} ->
	    NState = State#state{dynamics = ?DICT:store(Pid, A, Dynamics)},
	    {ok, NState};
	{ok, Pid, _Extra} ->
	    NState = State#state{dynamics = ?DICT:store(Pid, A, Dynamics)},
	    {ok, NState};
	{error, Error} ->
	    report_error(start_error, Error, Child, State#state.name),
	    Restart(Child, State)
    end;
restart(one_for_one, Child, State, Restart) ->
    case do_start_child(State#state.name, Child) of
	{ok, Pid} ->
	    NState = replace_child(Child#child{pid = Pid}, State),
	    {ok, NState};
	{ok, Pid, _Extra} ->
	    NState = replace_child(Child#child{pid = Pid}, State),
	    {ok, NState};
	{error, Reason} ->
	    report_error(start_error, Reason, Child, State#state.name),
	    Restart(Child, State)
    end;
restart(rest_for_one, Child, State, Restart) ->
    {ChAfter, ChBefore} = split_child(Child#child.pid, State#state.children),
    ChAfter2 = terminate_children(ChAfter, State#state.name),
    case start_children(ChAfter2, State#state.name) of
	{ok, ChAfter3} ->
	    {ok, State#state{children = ChAfter3 ++ ChBefore}};
	{error, ChAfter3} ->
	    Restart(Child, State#state{children = ChAfter3 ++ ChBefore})
    end;
restart(one_for_all, Child, State, Restart) ->
    Children1 = del_child(Child#child.pid, State#state.children),
    Children2 = terminate_children(Children1, State#state.name),
    case start_children(Children2, State#state.name) of
	{ok, NChs} ->
	    {ok, State#state{children = NChs}};
	{error, NChs} ->
	    Restart(Child, State#state{children = NChs})
    end.

%%-----------------------------------------------------------------
%% Func: terminate_children/2
%% Args: Children = [#child] in termination order
%%       SupName = {local, atom()} | {global, atom()} | {pid(),Mod}
%% Returns: NChildren = [#child] in
%%          startup order (reversed termination order)
%%-----------------------------------------------------------------
terminate_children(Children, SupName) ->
    terminate_children(Children, SupName, []).

terminate_children([Child | Children], SupName, Res) ->
    NChild = do_terminate(Child, SupName),
    terminate_children(Children, SupName, [NChild | Res]);
terminate_children([], _SupName, Res) ->
    Res.

terminate_simple_children(Child, Dynamics, SupName) ->
    Pids = dict:fold(fun (Pid, _Args, Pids) ->
                         erlang:monitor(process, Pid),
                         unlink(Pid),
                         exit(Pid, child_exit_reason(Child)),
                         [Pid | Pids]
                     end, [], Dynamics),
    TimeoutMsg = {timeout, make_ref()},
    TRef = timeout_start(Child, TimeoutMsg),
    {Replies, Timedout} =
        lists:foldl(
          fun (_Pid, {Replies, Timedout}) ->
                  {Pid1, Reason1, Timedout1} =
                      receive
                          TimeoutMsg ->
                              Remaining = Pids -- [P || {P, _} <- Replies],
                              [exit(P, kill) || P <- Remaining],
                              receive
                                  {'DOWN', _MRef, process, Pid, Reason} ->
                                      {Pid, Reason, true}
                              end;
                          {'DOWN', _MRef, process, Pid, Reason} ->
                              {Pid, Reason, Timedout}
                      end,
                  {[{Pid1, child_res(Child, Reason1, Timedout1)} | Replies],
                   Timedout1}
          end, {[], false}, Pids),
    timeout_stop(Child, TRef, TimeoutMsg, Timedout),
    ReportError = shutdown_error_reporter(SupName),
    Report = fun(_, ok)           -> ok;
                (Pid, {error, R}) -> ReportError(R, Child#child{pid = Pid})
             end,
    [receive
         {'EXIT', Pid, Reason} ->
             Report(Pid, child_res(Child, Reason, Timedout))
     after
         0 -> Report(Pid, Reply)
     end || {Pid, Reply} <- Replies],
    ok.

child_exit_reason(#child{shutdown = brutal_kill}) -> kill;
child_exit_reason(#child{})                       -> shutdown.

child_res(#child{shutdown=brutal_kill},   killed,    false) -> ok;
child_res(#child{},                       shutdown,  false) -> ok;
child_res(#child{restart_type=permanent}, normal,    false) -> {error, normal};
child_res(#child{restart_type={permanent,_}},normal, false) -> {error, normal};
child_res(#child{},                       normal,    false) -> ok;
child_res(#child{},                       R,         _)     -> {error, R}.

timeout_start(#child{shutdown = Time}, Msg) when is_integer(Time) ->
    erlang:send_after(Time, self(), Msg);
timeout_start(#child{}, _Msg) ->
    ok.

timeout_stop(#child{shutdown = Time}, TRef, Msg, false) when is_integer(Time) ->
    erlang:cancel_timer(TRef),
    receive
        Msg -> ok
    after
        0 -> ok
    end;
timeout_stop(#child{}, _TRef, _Msg, _Timedout) ->
    ok.

do_terminate(Child, SupName) when Child#child.pid =/= undefined ->
    ReportError = shutdown_error_reporter(SupName),
    case shutdown(Child#child.pid, Child#child.shutdown) of
        ok ->
            ok;
        {error, normal} ->
            case Child#child.restart_type of
                permanent           -> ReportError(normal, Child);
                {permanent, _Delay} -> ReportError(normal, Child);
                _                   -> ok
            end;
        {error, OtherReason} ->
            ReportError(OtherReason, Child)
    end,
    Child#child{pid = undefined};
do_terminate(Child, _SupName) ->
    Child.

%%-----------------------------------------------------------------
%% Shutdowns a child. We must check the EXIT value
%% of the child, because it might have died with another reason than
%% the wanted. In that case we want to report the error. We put a
%% monitor on the child an check for the 'DOWN' message instead of
%% checking for the 'EXIT' message, because if we check the 'EXIT'
%% message a "naughty" child, who does unlink(Sup), could hang the
%% supervisor.
%% Returns: ok | {error, OtherReason}  (this should be reported)
%%-----------------------------------------------------------------
shutdown(Pid, brutal_kill) ->

    case monitor_child(Pid) of
	ok ->
	    exit(Pid, kill),
	    receive
		{'DOWN', _MRef, process, Pid, killed} ->
		    ok;
		{'DOWN', _MRef, process, Pid, OtherReason} ->
		    {error, OtherReason}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end;

shutdown(Pid, Time) ->

    case monitor_child(Pid) of
	ok ->
	    exit(Pid, shutdown), %% Try to shutdown gracefully
	    receive
		{'DOWN', _MRef, process, Pid, shutdown} ->
		    ok;
		{'DOWN', _MRef, process, Pid, OtherReason} ->
		    {error, OtherReason}
	    after Time ->
		    exit(Pid, kill),  %% Force termination.
		    receive
			{'DOWN', _MRef, process, Pid, OtherReason} ->
			    {error, OtherReason}
		    end
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%% Help function to shutdown/2 switches from link to monitor approach
monitor_child(Pid) ->

    %% Do the monitor operation first so that if the child dies
    %% before the monitoring is done causing a 'DOWN'-message with
    %% reason noproc, we will get the real reason in the 'EXIT'-message
    %% unless a naughty child has already done unlink...
    erlang:monitor(process, Pid),
    unlink(Pid),

    receive
	%% If the child dies before the unlik we must empty
	%% the mail-box of the 'EXIT'-message and the 'DOWN'-message.
	{'EXIT', Pid, Reason} ->
	    receive
		{'DOWN', _, process, Pid, _} ->
		    {error, Reason}
	    end
    after 0 ->
	    %% If a naughty child did unlink and the child dies before
	    %% monitor the result will be that shutdown/2 receives a
	    %% 'DOWN'-message with reason noproc.
	    %% If the child should die after the unlink there
	    %% will be a 'DOWN'-message with a correct reason
	    %% that will be handled in shutdown/2.
	    ok
    end.


%%-----------------------------------------------------------------
%% Child/State manipulating functions.
%%-----------------------------------------------------------------
state_del_child(#child{pid = Pid}, State) when ?is_simple(State) ->
    NDynamics = ?DICT:erase(Pid, State#state.dynamics),
    State#state{dynamics = NDynamics};
state_del_child(Child, State) ->
    NChildren = del_child(Child#child.name, State#state.children),
    State#state{children = NChildren}.

del_child(Name, [Ch|Chs]) when Ch#child.name =:= Name ->
    [Ch#child{pid = undefined} | Chs];
del_child(Pid, [Ch|Chs]) when Ch#child.pid =:= Pid ->
    [Ch#child{pid = undefined} | Chs];
del_child(Name, [Ch|Chs]) ->
    [Ch|del_child(Name, Chs)];
del_child(_, []) ->
    [].

%% Chs = [S4, S3, Ch, S1, S0]
%% Ret: {[S4, S3, Ch], [S1, S0]}
split_child(Name, Chs) ->
    split_child(Name, Chs, []).

split_child(Name, [Ch|Chs], After) when Ch#child.name =:= Name ->
    {lists:reverse([Ch#child{pid = undefined} | After]), Chs};
split_child(Pid, [Ch|Chs], After) when Ch#child.pid =:= Pid ->
    {lists:reverse([Ch#child{pid = undefined} | After]), Chs};
split_child(Name, [Ch|Chs], After) ->
    split_child(Name, Chs, [Ch | After]);
split_child(_, [], After) ->
    {lists:reverse(After), []}.

get_child(Name, State) ->
    lists:keysearch(Name, #child.name, State#state.children).
replace_child(Child, State) ->
    Chs = do_replace_child(Child, State#state.children),
    State#state{children = Chs}.

do_replace_child(Child, [Ch|Chs]) when Ch#child.name =:= Child#child.name ->
    [Child | Chs];
do_replace_child(Child, [Ch|Chs]) ->
    [Ch|do_replace_child(Child, Chs)].

remove_child(Child, State) ->
    Chs = lists:keydelete(Child#child.name, #child.name, State#state.children),
    State#state{children = Chs}.

%%-----------------------------------------------------------------
%% Func: init_state/4
%% Args: SupName = {local, atom()} | {global, atom()} | self
%%       Type = {Strategy, MaxIntensity, Period}
%%         Strategy = one_for_one | one_for_all | simple_one_for_one |
%%                    rest_for_one
%%         MaxIntensity = integer()
%%         Period = integer()
%%       Mod :== atom()
%%       Arsg :== term()
%% Purpose: Check that Type is of correct type (!)
%% Returns: {ok, #state} | Error
%%-----------------------------------------------------------------
init_state(SupName, Type, Mod, Args) ->
    case catch init_state1(SupName, Type, Mod, Args) of
	{ok, State} ->
	    {ok, State};
	Error ->
	    Error
    end.

init_state1(SupName, {Strategy, MaxIntensity, Period}, Mod, Args) ->
    validStrategy(Strategy),
    validIntensity(MaxIntensity),
    validPeriod(Period),
    {ok, #state{name = supname(SupName,Mod),
	       strategy = Strategy,
	       intensity = MaxIntensity,
	       period = Period,
	       module = Mod,
	       args = Args}};
init_state1(_SupName, Type, _, _) ->
    {invalid_type, Type}.

validStrategy(simple_one_for_one_terminate) -> true;
validStrategy(simple_one_for_one)           -> true;
validStrategy(one_for_one)                  -> true;
validStrategy(one_for_all)                  -> true;
validStrategy(rest_for_one)                 -> true;
validStrategy(What)                         -> throw({invalid_strategy, What}).

validIntensity(Max) when is_integer(Max),
                         Max >=  0 -> true;
validIntensity(What)              -> throw({invalid_intensity, What}).

validPeriod(Period) when is_integer(Period),
                         Period > 0 -> true;
validPeriod(What)                   -> throw({invalid_period, What}).

supname(self,Mod) -> {self(),Mod};
supname(N,_)      -> N.

%%% ------------------------------------------------------
%%% Check that the children start specification is valid.
%%% Shall be a six (6) tuple
%%%    {Name, Func, RestartType, Shutdown, ChildType, Modules}
%%% where Name is an atom
%%%       Func is {Mod, Fun, Args} == {atom, atom, list}
%%%       RestartType is permanent | temporary | transient |
%%%                      intrinsic | {permanent, Delay} |
%%%                      {transient, Delay} | {intrinsic, Delay}
%%                       where Delay >= 0
%%%       Shutdown = integer() | infinity | brutal_kill
%%%       ChildType = supervisor | worker
%%%       Modules = [atom()] | dynamic
%%% Returns: {ok, [#child]} | Error
%%% ------------------------------------------------------

check_startspec(Children) -> check_startspec(Children, []).

check_startspec([ChildSpec|T], Res) ->
    case check_childspec(ChildSpec) of
	{ok, Child} ->
	    case lists:keymember(Child#child.name, #child.name, Res) of
		true -> {duplicate_child_name, Child#child.name};
		false -> check_startspec(T, [Child | Res])
	    end;
	Error -> Error
    end;
check_startspec([], Res) ->
    {ok, lists:reverse(Res)}.

check_childspec({Name, Func, RestartType, Shutdown, ChildType, Mods}) ->
    catch check_childspec(Name, Func, RestartType, Shutdown, ChildType, Mods);
check_childspec(X) -> {invalid_child_spec, X}.

check_childspec(Name, Func, RestartType, Shutdown, ChildType, Mods) ->
    validName(Name),
    validFunc(Func),
    validRestartType(RestartType),
    validChildType(ChildType),
    validShutdown(Shutdown, ChildType),
    validMods(Mods),
    {ok, #child{name = Name, mfa = Func, restart_type = RestartType,
		shutdown = Shutdown, child_type = ChildType, modules = Mods}}.

validChildType(supervisor) -> true;
validChildType(worker) -> true;
validChildType(What) -> throw({invalid_child_type, What}).

validName(_Name) -> true.

validFunc({M, F, A}) when is_atom(M),
                          is_atom(F),
                          is_list(A) -> true;
validFunc(Func)                      -> throw({invalid_mfa, Func}).

validRestartType(permanent)          -> true;
validRestartType(temporary)          -> true;
validRestartType(transient)          -> true;
validRestartType(intrinsic)          -> true;
validRestartType({permanent, Delay}) -> validDelay(Delay);
validRestartType({intrinsic, Delay}) -> validDelay(Delay);
validRestartType({transient, Delay}) -> validDelay(Delay);
validRestartType(RestartType)        -> throw({invalid_restart_type,
                                               RestartType}).

validDelay(Delay) when is_number(Delay),
                       Delay >= 0 -> true;
validDelay(What)                  -> throw({invalid_delay, What}).

validShutdown(Shutdown, _)
  when is_integer(Shutdown), Shutdown > 0 -> true;
validShutdown(infinity, supervisor)    -> true;
validShutdown(brutal_kill, _)          -> true;
validShutdown(Shutdown, _)             -> throw({invalid_shutdown, Shutdown}).

validMods(dynamic) -> true;
validMods(Mods) when is_list(Mods) ->
    lists:foreach(fun (Mod) ->
		    if
			is_atom(Mod) -> ok;
			true -> throw({invalid_module, Mod})
		    end
		  end,
		  Mods);
validMods(Mods) -> throw({invalid_modules, Mods}).

%%% ------------------------------------------------------
%%% Add a new restart and calculate if the max restart
%%% intensity has been reached (in that case the supervisor
%%% shall terminate).
%%% All restarts accured inside the period amount of seconds
%%% are kept in the #state.restarts list.
%%% Returns: {ok, State'} | {terminate, State'}
%%% ------------------------------------------------------

add_restart(State) ->
    I = State#state.intensity,
    P = State#state.period,
    R = State#state.restarts,
    Now = erlang:now(),
    R1 = add_restart([Now|R], Now, P),
    State1 = State#state{restarts = R1},
    case length(R1) of
	CurI when CurI  =< I ->
	    {ok, State1};
	_ ->
	    {terminate, State1}
    end.

add_restart([R|Restarts], Now, Period) ->
    case inPeriod(R, Now, Period) of
	true ->
	    [R|add_restart(Restarts, Now, Period)];
	_ ->
	    []
    end;
add_restart([], _, _) ->
    [].

inPeriod(Time, Now, Period) ->
    case difference(Time, Now) of
	T when T > Period ->
	    false;
	_ ->
	    true
    end.

%%
%% Time = {MegaSecs, Secs, MicroSecs} (NOTE: MicroSecs is ignored)
%% Calculate the time elapsed in seconds between two timestamps.
%% If MegaSecs is equal just subtract Secs.
%% Else calculate the Mega difference and add the Secs difference,
%% note that Secs difference can be negative, e.g.
%%      {827, 999999, 676} diff {828, 1, 653753} == > 2 secs.
%%
difference({TimeM, TimeS, _}, {CurM, CurS, _}) when CurM > TimeM ->
    ((CurM - TimeM) * 1000000) + (CurS - TimeS);
difference({_, TimeS, _}, {_, CurS, _}) ->
    CurS - TimeS.

%%% ------------------------------------------------------
%%% Error and progress reporting.
%%% ------------------------------------------------------

report_error(Error, Reason, Child, SupName) ->
    ErrorMsg = [{supervisor, SupName},
		{errorContext, Error},
		{reason, Reason},
		{offender, extract_child(Child)}],
    error_logger:error_report(supervisor_report, ErrorMsg).

shutdown_error_reporter(SupName) ->
    fun(Reason, Child) ->
        report_error(shutdown_error, Reason, Child, SupName)
    end.

extract_child(Child) ->
    [{pid, Child#child.pid},
     {name, Child#child.name},
     {mfa, Child#child.mfa},
     {restart_type, Child#child.restart_type},
     {shutdown, Child#child.shutdown},
     {child_type, Child#child.child_type}].

report_progress(Child, SupName) ->
    Progress = [{supervisor, SupName},
		{started, extract_child(Child)}],
    error_logger:info_report(progress, Progress).
