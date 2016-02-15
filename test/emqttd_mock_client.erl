
-module(emqttd_mock_client).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, start_session/1, stop/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {clientid, session}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(ClientId) ->
    gen_server:start_link(?MODULE, [ClientId], []).

start_session(CPid) ->
    gen_server:call(CPid, start_session).

stop(CPid) ->
    gen_server:call(CPid, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([ClientId]) ->
    {ok, #state{clientid = ClientId}}.

handle_call(start_session, _From, State = #state{clientid = ClientId}) ->
    {ok, SessPid, _} = emqttd_sm:start_session(true, ClientId),
    {reply, {ok, SessPid}, State#state{session = SessPid}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


