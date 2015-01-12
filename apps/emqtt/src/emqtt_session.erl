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

-module(emqtt_session).

-record(session_state, { 
        client_id,
        client_pid,
		packet_id = 1,
        subscriptions = [],
        messages = [], %% do not receive rel
		awaiting_ack,
        awaiting_rel }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([start/1, resume/1, publish/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start({true = CleanSess, ClientId, ClientPid}) ->
    %%destroy old session
    %%TODO: emqtt_sm:destory_session(ClientId),
    {ok, initial_state(ClientId)};

start({false = CleanSess, ClientId, ClientPid}) ->
    %%TODO: emqtt_sm:start_session({ClientId, ClientPid})
    gen_server:start_link(?MODULE, [ClientId, ClientPid], []).

resume(#session_state {}) -> 'TODO';
resume(SessPid) when is_pid(SessPid) -> 'TODO'.

publish(_, {?QOS_0, Message}) ->
    emqtt_router:route(Message);

%%TODO:
publish(_, {?QOS_1, Message}) ->
	emqtt_router:route(Message),

%%TODO:
publish(Session = #session_state{awaiting_rel = Awaiting}, {?QOS_2, Message}) ->
    %% store gb_tree:
    Session#session_state{awaiting_rel = Awaiting};

publish(_, {?QOS_2, Message}) ->
    %TODO:
	put({msg, PacketId}, pubrec),
	emqtt_router:route(Message),

initial_state(ClientId) ->
    #session_state { client_id = ClientId,
                     packet_id = 1, 
                     subscriptions = [], 
                     awaiting_ack = gb_trees:empty(),
                     awaiting_rel = gb_trees:empty() }.

initial_state(ClientId, ClientPid) ->
    State = initial_state(ClientId),
    State#session_state{client_pid = ClientPid}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([ClientId, ClientPid]) ->
    process_flag(trap_exit, true),
    State = initial_state(ClientId, ClientPid),
    {ok, State}.

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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------



