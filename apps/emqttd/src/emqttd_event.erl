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
%%% emqttd event manager.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_event).

-include_lib("emqtt/include/emqtt.hrl").

%% API Function Exports
-export([start_link/0,
         add_handler/2,
         notify/1]).

%% gen_event Function Exports
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {systop}).

%%------------------------------------------------------------------------------
%% @doc
%% Start emqttd event manager.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    case gen_event:start_link({local, ?MODULE}) of
        {ok, Pid} -> 
            add_handler(?MODULE, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

add_handler(Handler, Args) ->
    gen_event:add_handler(?MODULE, Handler, Args).

notify(Event) ->
    gen_event:notify(?MODULE, Event).
%%%=============================================================================
%%% gen_event callbacks
%%%=============================================================================

init([]) ->
    SysTop = list_to_binary(lists:concat(["$SYS/brokers/", node(), "/"])),
    {ok, #state{systop = SysTop}}.

handle_event({connected, ClientId, Params}, State = #state{systop = SysTop}) ->
    Topic = <<SysTop/binary, "clients/", ClientId/binary, "/connected">>,
    Msg = #mqtt_message{topic = Topic, payload = payload(connected, Params)},
    emqttd_pubsub:publish(event, Msg),
    {ok, State};

handle_event({disconnectd, ClientId, Reason}, State = #state{systop = SysTop}) ->
    Topic = <<SysTop/binary, "clients/", ClientId/binary, "/disconnected">>,
    Msg = #mqtt_message{topic = Topic, payload = payload(disconnected, Reason)},
    emqttd_pubsub:publish(event, Msg),
    {ok, State};

handle_event({subscribed, ClientId, TopicTable}, State) ->
    lager:error("TODO: subscribed ~s, ~p", [ClientId, TopicTable]),
    {ok, State};

handle_event({unsubscribed, ClientId, Topics}, State) ->
    lager:error("TODO: unsubscribed ~s, ~p", [ClientId, Topics]),
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

payload(connected, Params) ->
    From = proplists:get_value(from, Params),
    Proto = proplists:get_value(protocol, Params),
    Sess = proplists:get_value(session, Params),
    iolist_to_binary(io_lib:format("from: ~s~nprotocol: ~p~nsession: ~s", [From, Proto, Sess]));

payload(disconnected, Reason) ->
    list_to_binary(io_lib:format(["reason: ~p", Reason])).

