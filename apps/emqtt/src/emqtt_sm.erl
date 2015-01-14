%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------


%%------------------------------------------------------------------------------
%%
%% The Session state in the Server consists of:
%% The existence of a Session, even if the rest of the Session state is empty.
%% The Client’s subscriptions.
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely
%% acknowledged.
%% QoS 1 and QoS 2 messages pending transmission to the Client.
%% QoS 2 messages which have been received from the Client, but have not been completely
%% acknowledged.
%% Optionally, QoS 0 messages pending transmission to the Client.
%%
%%------------------------------------------------------------------------------

-module(emqtt_sm).

%%emqtt session manager...

%%cleanSess: true | false

-include("emqtt.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TABLE, emqtt_session).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([lookup_session/1, start_session/2, destory_session/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {ok, pid()}).

-spec(lookup_session/1 :: (binary()) -> pid() | undefined).

-spec(start_session/2 :: (binary(), pid()) -> {ok, pid()} | {error, any()}).

-spec(destory_session/1 :: (binary()) -> ok).

-endif.

%%----------------------------------------------------------------------------

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

lookup_session(ClientId) ->
    case ets:lookup(?TABLE, ClientId) of
        [{_, SessPid, _}] ->  SessPid;
        [] -> undefined
    end.

start_session(ClientId, ClientPid) ->
    gen_server:call(?SERVER, {start_session, ClientId, ClientPid}).

destory_session(ClientId) ->
    gen_server:call(?SERVER, {destory_session, ClientId}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ets:new(?TABLE, [set, protected, named_table]),
    {ok, #state{}}.

handle_call({start_session, ClientId, ClientPid}, _From, State) ->
    Reply =
    case ets:lookup(?TABLE, ClientId) of
        [{_, SessPid, _MRef}] ->
            emqtt_session:resume(SessPid, ClientId, ClientPid), 
            {ok, SessPid};
        [] ->
            case emqtt_session_sup:start_session(ClientId, ClientPid) of
            {ok, SessPid} -> 
                MRef = erlang:monitor(process, SessPid),
                ets:insert(?TABLE, {ClientId, SessPid, MRef}),
                {ok, SessPid};
            {error, Error} ->
                {error, Error}
            end
    end,
    {reply, Reply, State};

handle_call({destory_session, ClientId}, _From, State) ->
    case ets:lookup(?TABLE, ClientId) of
        [{_, SessPid, MRef}] ->
            erlang:demonitor(MRef),
            emqtt_session:destory(SessPid),
            ets:delete(?TABLE, ClientId);
        [] ->
            ignore
    end,
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(emqtt_client, {{'_', DownPid, MRef}}),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


