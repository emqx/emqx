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

-module(emqtt_server).

-author('feng@slimpp.io').

-include("emqtt.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(RETAINED_TAB, mqtt_retained).

-define(STORE_LIMIT, 100000).

-record(mqtt_retained, {topic, qos, payload}).

-record(state, {store_limit}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

%%TODO: subscribe
-export([start_link/1, retain/1, subscribe/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(RetainOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [RetainOpts], []).

retain(#mqtt_message{ retain = false }) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{ retain = true, topic = Topic, payload = <<>> }) -> 
    mnesia:dirty_delete(?RETAINED_TAB, Topic);

retain(Msg = #mqtt_message{retain = true}) -> 
    gen_server:cast(?SERVER, {retain, Msg}).

%% 
subscribe(Topics, CPid) when is_pid(CPid) ->
    RetainedMsgs = lists:flatten([mnesia:dirty_read(?RETAINED_TAB, Topic) || Topic <- match(Topics)]),
    lists:foreach(fun(Msg) -> 
                CPid ! {dispatch, {self(), retained_msg(Msg)}}
        end, RetainedMsgs).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([RetainOpts]) ->
	mnesia:create_table(mqtt_retained, [
        {type, ordered_set},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, mqtt_retained)}]),
	mnesia:add_table_copy(mqtt_retained, node(), ram_copies),
    Limit = proplists:get_value(store_limit, RetainOpts, ?STORE_LIMIT),
    {ok, #state{store_limit = Limit}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({retain, Msg = #mqtt_message{ qos = Qos, 
                                          topic = Topic, 
                                          payload = Payload }}, State = #state{store_limit = Limit}) ->
    case mnesia:table_info(?RETAINED_TAB, size) of
        Size when Size >= Limit -> 
            lager:error("Server dropped message(retain) for table is full: ~p", [Msg]);
        _ -> 
            lager:info("Server retained message: ~p", [Msg]),
            mnesia:dirty_write(#mqtt_retained{ topic = Topic, 
                                               qos = Qos, 
                                               payload = Payload })
    end,
    {noreply, State};

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
match(Topics) ->
    %%TODO: dirty_all_keys....
    Topics.

retained_msg(#mqtt_retained{topic = Topic, qos = Qos, payload = Payload}) ->
    #mqtt_message { qos = Qos, retain = true, topic = Topic, payload = Payload }.

