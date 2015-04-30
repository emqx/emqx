%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd session manager.
%%%
%%% The Session state in the Server consists of:
%%% The existence of a Session, even if the rest of the Session state is empty.
%%% The Client’s subscriptions.
%%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not 
%%% been completely acknowledged.
%%% QoS 1 and QoS 2 messages pending transmission to the Client.
%%% QoS 2 messages which have been received from the Client, but have not been
%%% completely acknowledged.
%%% Optionally, QoS 0 messages pending transmission to the Client.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_sm).

-author("Feng Lee <feng@emqtt.io>").

%%cleanSess: true | false

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0]).

-export([lookup_session/1, start_session/2, destroy_session/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tabid, statsfun}).

-define(SESSION_TAB, mqtt_session).

%%%=============================================================================
%%% API
%%%=============================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc Lookup Session Pid
%% @end
%%------------------------------------------------------------------------------
-spec lookup_session(binary()) -> pid() | undefined.
lookup_session(ClientId) ->
    case ets:lookup(?SESSION_TAB, ClientId) of
        [{_, SessPid, _}] ->  SessPid;
        [] -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Start a session
%% @end
%%------------------------------------------------------------------------------
-spec start_session(binary(), pid()) -> {ok, pid()} | {error, any()}.
start_session(ClientId, ClientPid) ->
    gen_server:call(?SERVER, {start_session, ClientId, ClientPid}).

%%------------------------------------------------------------------------------
%% @doc Destroy a session
%% @end
%%------------------------------------------------------------------------------
-spec destroy_session(binary()) -> ok.
destroy_session(ClientId) ->
    gen_server:call(?SERVER, {destroy_session, ClientId}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    process_flag(trap_exit, true),
    TabId = ets:new(?SESSION_TAB, [set, protected, named_table]),
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    {ok, #state{tabid = TabId, statsfun = StatsFun}}.

handle_call({start_session, ClientId, ClientPid}, _From, State = #state{tabid = Tab}) ->
    Reply =
    case ets:lookup(Tab, ClientId) of
        [{_, SessPid, _MRef}] ->
            emqttd_session:resume(SessPid, ClientId, ClientPid), 
            {ok, SessPid};
        [] ->
            case emqttd_session_sup:start_session(ClientId, ClientPid) of
            {ok, SessPid} -> 
                ets:insert(Tab, {ClientId, SessPid,
                                 erlang:monitor(process, SessPid)}),
                {ok, SessPid};
            {error, Error} ->
                {error, Error}
            end
    end,
    {reply, Reply, setstats(State)};

handle_call({destroy_session, ClientId}, _From, State = #state{tabid = Tab}) ->
    case ets:lookup(Tab, ClientId) of
        [{_, SessPid, MRef}] ->
            emqttd_session:destroy(SessPid, ClientId),
            erlang:demonitor(MRef, [flush]),
            ets:delete(Tab, ClientId);
        [] ->
            ignore
    end,
    {reply, ok, setstats(State)};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State = #state{tabid = Tab}) ->
	ets:match_delete(Tab, {'_', DownPid, MRef}),
    {noreply, setstats(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(?SESSION_TAB, size)), State.

