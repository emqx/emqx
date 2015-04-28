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
%%% emqttd websocket client.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_websocket).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link/1, ws_loop/3]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(wsocket_state, {client, parser_state}).

-record(client_state, {request, reply_channel, proto_state, keepalive}).

-define(PACKET_OPTS, [{max_clientid_len, 1024},
                      {max_packet_size,  4096}]).

start_link(Req) ->
    {ReentryWs, ReplyChannel} = mochiweb_websocket:upgrade_connection(Req, fun ?MODULE:ws_loop/3),
    {ok, Client} = gen_server:start_link(?MODULE, [Req, ReplyChannel], []),
    ReentryWs(#wsocket_state{client = Client,
                             parser_state = emqtt_parser:init(?PACKET_OPTS)}).

ws_loop(<<>>, State, _ReplyChannel) ->
    State;
ws_loop(Payload, State = #wsocket_state{client = Client, parser_state = ParserState}, ReplyChannel) ->
    io:format("Received data: ~p~n", [Payload]),
    case catch emqtt_parser:parse(iolist_to_binary(Payload), ParserState) of
    {more, ParserState1} ->
        State#wsocket_state{parser_state = ParserState1};
    {ok, Packet, Rest} ->
        Client ! {received, Packet},
        ws_loop(Rest, State#wsocket_state{parser_state = emqtt_parser:init(?PACKET_OPTS)}, ReplyChannel);
    {error, Error} ->
        lager:error("MQTT detected framing error ~p~n", [Error]),
        exit({shutdown, Error});
    Exit ->
        lager:error("MQTT detected error ~p~n", [Exit])
    end.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Req, ReplyChannel]) ->
    %%TODO: Redesign later...
    Socket = Req:get(socket),
    {ok, Peername} = emqttd_net:peername(Socket),
    SendFun = fun(Payload) -> ReplyChannel({binary, Payload}) end,
    ProtoState = emqttd_protocol:init({SendFun, Socket, Peername}, ?PACKET_OPTS),
    {ok, #client_state{request = Req, reply_channel = ReplyChannel,
                       proto_state = ProtoState}}.

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({received, Packet}, State = #client_state{proto_state = ProtoState}) ->
    io:format("Packet Received: ~p~n", [Packet]),
    case emqttd_protocol:received(Packet, ProtoState) of
    {ok, ProtoState1} ->
        {noreply, State#client_state{proto_state = ProtoState1}};
    {error, Error} ->
        lager:error("MQTT protocol error ~p", [Error]),
        stop({shutdown, Error}, State);
    {error, Error, ProtoState1} ->
        stop({shutdown, Error}, State#client_state{proto_state = ProtoState1});
    {stop, Reason, ProtoState1} ->
        stop(Reason, State#client_state{proto_state = ProtoState1})
    end;

%%TODO: ok??
handle_info({dispatch, {From, Messages}}, #client_state{proto_state = ProtoState} = State) when is_list(Messages) ->
    ProtoState1 =
    lists:foldl(fun(Message, PState) ->
            {ok, PState1} = emqttd_protocol:send({From, Message}, PState), PState1
        end, ProtoState, Messages),
    {noreply, State#client_state{proto_state = ProtoState1}};

handle_info({dispatch, {From, Message}}, #client_state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqttd_protocol:send({From, Message}, ProtoState),
    {noreply, State#client_state{proto_state = ProtoState1}};

handle_info({redeliver, {?PUBREL, PacketId}}, #client_state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    {noreply, State#client_state{proto_state = ProtoState1}};

handle_info({stop, duplicate_id, _NewPid}, State=#client_state{proto_state = ProtoState}) ->
    %% TODO: to...
    %% need transfer data???
    %% emqttd_client:transfer(NewPid, Data),
    lager:error("Shutdown for duplicate clientid: ~s",
                [emqttd_protocol:clientid(ProtoState)]), 
    stop({shutdown, duplicate_id}, State);

handle_info({keepalive, start, _TimeoutSec}, State = #client_state{}) ->
    %lager:debug("Client ~s: Start KeepAlive with ~p seconds", [emqttd_net:format(Peername), TimeoutSec]),
    KeepAlive = undefined, %emqttd_keepalive:new({Transport, Socket}, TimeoutSec, {keepalive, timeout}),
    {noreply, State#client_state{ keepalive = KeepAlive }};

handle_info({keepalive, timeout}, State = #client_state{keepalive = KeepAlive}) ->
    case emqttd_keepalive:resume(KeepAlive) of
    timeout ->
        %lager:debug("Client ~s: Keepalive Timeout!", [emqttd_net:format(Peername)]),
        stop({shutdown, keepalive_timeout}, State#client_state{keepalive = undefined});
    {resumed, KeepAlive1} ->
        %lager:debug("Client ~s: Keepalive Resumed", [emqttd_net:format(Peername)]),
        {noreply, State#client_state{keepalive = KeepAlive1}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #client_state{keepalive = KeepAlive, proto_state = ProtoState}) ->
    lager:debug("~p terminated, reason: ~p~n", [self(), Reason]),
    emqttd_keepalive:cancel(KeepAlive),
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} -> 
            emqttd_protocol:shutdown(Error, ProtoState);
        {_, _} -> 
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

stop(Reason, State ) ->
    {stop, Reason, State}.

