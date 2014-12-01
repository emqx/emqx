-module(emqtt_monitor).

-include_lib("elog/include/elog.hrl").

-behavior(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {ok}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    erlang:system_monitor(self(), [{long_gc, 5000}, {large_heap, 10000}, busy_port]),
    ?INFO("monitor is started...[ok]", []),
    {ok, #state{}}.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    ?ERROR("unexpected request: ~p", [Request]),
    {reply, {error, unexpected_request}, State}.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    ?ERROR("unexpected msg: ~p", [Msg]),
    {noreply, State}.
%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({monitor, GcPid, long_gc, Info}, State) ->
    ?ERROR("long_gc: gcpid = ~p, ~p ~n ~p", [GcPid, process_info(GcPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), Info]),
    {noreply, State};

handle_info({monitor, GcPid, large_heap, Info}, State) ->
	[{messages,Mess}] = process_info(GcPid, [messages]),
    ?ERROR("large_heap: gcpid = ~p,~p ~n ~p, ~p", [GcPid, process_info(GcPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), 
			lists:nth(1, Mess),Info]),
    {noreply, State};

handle_info({monitor, SusPid, busy_port, Port}, State) ->
    ?ERROR("busy_port: suspid = ~p, port = ~p", [process_info(SusPid, 
		[registered_name, memory, message_queue_len,heap_size,total_heap_size]), Port]),
    {noreply, State};

handle_info(Info, State) ->
    ?ERROR("unexpected info: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


