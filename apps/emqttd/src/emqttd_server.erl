%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% emqttd server. retain messages???
%%% TODO: redesign...
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_server).

-author('feng@slimpp.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-include("emqttd.hrl").

-include("emqttd_topic.hrl").

-include("emqttd_packet.hrl").

-record(state, {store_limit}).

-define(RETAINED_TAB, mqtt_retained).

-define(STORE_LIMIT, 1000000).

%% API Function Exports
-export([start_link/1, retain/1, subscribe/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%%=============================================================================
%%% API
%%%=============================================================================

-spec start_link([tuple()]) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) -> 
    mnesia:dirty_delete(?RETAINED_TAB, Topic);

retain(Msg = #mqtt_message{retain = true}) -> 
    gen_server:cast(?SERVER, {retain, Msg}).

%% TODO: this is not right???
subscribe(Topics, CPid) when is_pid(CPid) ->
    RetainedMsgs = lists:flatten([mnesia:dirty_read(?RETAINED_TAB, Topic) || Topic <- match(Topics)]),
    lists:foreach(fun(Msg) -> 
                CPid ! {dispatch, {self(), retained_msg(Msg)}}
        end, RetainedMsgs).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Opts]) ->
	mnesia:create_table(?RETAINED_TAB, [
            {type, ordered_set},
            {ram_copies, [node()]},
            {attributes, record_info(fields, mqtt_retained)}]),
	mnesia:add_table_copy(?RETAINED_TAB, node(), ram_copies),
    Limit = proplists:get_value(store_limit, Opts, ?STORE_LIMIT),
    {ok, #state{store_limit = Limit}}.

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast({retain, Msg = #mqtt_message{topic = Topic,
                                         qos = Qos, 
                                         payload = Payload}},
            State = #state{store_limit = Limit}) ->
    case mnesia:table_info(?RETAINED_TAB, size) of
        Size when Size >= Limit -> 
            lager:error("Dropped message(retain) for table is full: ~p", [Msg]);
        _ -> 
            lager:debug("Retained message: ~p", [Msg]),
            mnesia:dirty_write(#mqtt_retained{topic = Topic,
                                               qos = Qos,
                                               payload = Payload}),
            emqttd_metrics:set('messages/retained/count',
                              mnesia:table_info(?RETAINED_TAB, size))
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

match(Topics) ->
    RetainedTopics = mnesia:dirty_all_keys(?RETAINED_TAB),
    lists:flatten([match(Topic, RetainedTopics) || Topic <- Topics]).

match(Topic, RetainedTopics) ->
    case emqttd_topic:type(#topic{name=Topic}) of
        direct -> %% FIXME
            [Topic];
        wildcard ->
            [T || T <- RetainedTopics, emqttd_topic:match(T, Topic)]
    end.

retained_msg(#mqtt_retained{topic = Topic, qos = Qos, payload = Payload}) ->
    #mqtt_message{qos = Qos, retain = true, topic = Topic, payload = Payload}.


