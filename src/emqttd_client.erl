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
%%% MQTT Client Connection.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_client).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API Function Exports
-export([start_link/2, session/1, info/1, kick/1]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%% Client State...
-record(state, {transport,
                socket,
                peername,
                conn_name,
                await_recv,
                conn_state,
                rate_limiter,
                parser,
                proto_state,
                packet_opts,
                keepalive}).

-define(DEBUG(Format, Args, State),
            lager:debug("Client(~s): " ++ Format,
                         [emqttd_net:format(State#state.peername) | Args])).
-define(ERROR(Format, Args, State),
            lager:error("Client(~s): " ++ Format,
                          [emqttd_net:format(State#state.peername) | Args])).

start_link(SockArgs, MqttEnv) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[SockArgs, MqttEnv]])}.

session(CPid) ->
    gen_server:call(CPid, session, infinity).

info(CPid) ->
    gen_server:call(CPid, info, infinity).

kick(CPid) ->
    gen_server:call(CPid, kick).

subscribe(CPid, TopicTable) ->
    gen_server:cast(CPid, {subscribe, TopicTable}).

unsubscribe(CPid, Topics) ->
    gen_server:cast(CPid, {unsubscribe, Topics}).

init([SockArgs = {Transport, Sock, _SockFun}, MqttEnv]) ->
    % Transform if ssl.
    {ok, NewSock}  = esockd_connection:accept(SockArgs),
    %%TODO:...
    {ok, BufSizes} = inet:getopts(Sock, [sndbuf, recbuf, buffer]),
    io:format("~p~n", [BufSizes]),
    {ok, Peername} = emqttd_net:peername(Sock),
    {ok, ConnStr}  = emqttd_net:connection_string(Sock, inbound),
    SendFun    = send_fun(Transport, NewSock),
    PktOpts    = proplists:get_value(packet, MqttEnv),
    ProtoState = emqttd_protocol:init(Peername, SendFun, PktOpts),
    Limiter    = proplists:get_value(rate_limiter, MqttEnv),
    State      = run_socket(#state{transport    = Transport,
                                   socket       = NewSock,
                                   peername     = Peername,
                                   conn_name    = ConnStr,
                                   await_recv   = false,
                                   conn_state   = running,
                                   rate_limiter = Limiter,
                                   packet_opts  = PktOpts,
                                   parser       = emqttd_parser:new(PktOpts),
                                   proto_state  = ProtoState}),
    ClientOpts = proplists:get_value(client, MqttEnv),
    IdleTimout = proplists:get_value(idle_timeout, ClientOpts, 10),
    gen_server:enter_loop(?MODULE, [], State, timer:seconds(IdleTimout)).

handle_call(session, _From, State = #state{proto_state = ProtoState}) ->
    {reply, emqttd_protocol:session(ProtoState), State};

handle_call(info, _From, State = #state{conn_name = ConnName,
                                        proto_state = ProtoState}) ->
    {reply, [{conn_name, ConnName} | emqttd_protocol:info(ProtoState)], State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(Req, _From, State) ->
    ?ERROR("Unexpected request: ~p", [Req], State),
    {reply, {error, unsupported_request}, State}.    

handle_cast({subscribe, TopicTable}, State) ->
    with_session(fun(SessPid) -> emqttd_session:subscribe(SessPid, TopicTable) end, State);

handle_cast({unsubscribe, Topics}, State) ->
    with_session(fun(SessPid) -> emqttd_session:unsubscribe(SessPid, Topics) end, State);

handle_cast(Msg, State) ->
    ?ERROR("Unexpected msg: ~p",[Msg], State),
    {noreply, State}.

handle_info(timeout, State) ->
    stop({shutdown, timeout}, State);
    
handle_info({stop, duplicate_id, _NewPid}, State=#state{proto_state = ProtoState,
                                                        conn_name   = ConnName}) ->
    lager:warning("Shutdown for duplicate clientid: ~s, conn:~s",
                  [emqttd_protocol:clientid(ProtoState), ConnName]),
    stop({shutdown, duplicate_id}, State);

handle_info({deliver, Message}, State = #state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:send(Message, ProtoState),
    noreply(State#state{proto_state = ProtoState1});

handle_info({redeliver, {?PUBREL, PacketId}},  State = #state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    noreply(State#state{proto_state = ProtoState1});

handle_info(activate_sock, State) ->
    noreply(run_socket(State#state{conn_state = running}));

handle_info({inet_async, Sock, _Ref, {ok, Data}}, State = #state{peername = Peername, socket = Sock}) ->
    Size = size(Data),
    lager:debug("RECV from ~s: ~p", [emqttd_net:format(Peername), Data]),
    emqttd_metrics:inc('bytes/received', Size),
    received(Data, rate_limit(Size, State#state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    %%TODO: ...
    network_error(Reason, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    %%TODO: ok...
    io:format("inet_reply ok~n"),
    noreply(State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({keepalive, start, TimeoutSec}, State = #state{transport = Transport, socket = Socket}) ->
    ?DEBUG("Start KeepAlive with ~p seconds", [TimeoutSec], State),
    StatFun = fun() ->
            case Transport:getstat(Socket, [recv_oct]) of
                {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                {error, Error}              -> {error, Error}
            end
    end,
    KeepAlive = emqttd_keepalive:start(StatFun, TimeoutSec, {keepalive, check}),
    noreply(State#state{keepalive = KeepAlive});

handle_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            noreply(State#state{keepalive = KeepAlive1});
        {error, timeout} ->
            ?DEBUG("Keepalive Timeout!", [], State),
            stop({shutdown, keepalive_timeout}, State#state{keepalive = undefined});
        {error, Error} ->
            ?DEBUG("Keepalive Error - ~p", [Error], State),
            stop({shutdown, keepalive_error}, State#state{keepalive = undefined})
    end;

handle_info(Info, State) ->
    ?ERROR("Unexpected info: ~p", [Info], State),
    {noreply, State}.

terminate(Reason, #state{transport   = Transport,
                         socket      = Socket,
                         keepalive   = KeepAlive,
                         proto_state = ProtoState}) ->
    emqttd_keepalive:cancel(KeepAlive),
    if
        Reason == {shutdown, conn_closed} -> ok;
        true -> Transport:fast_close(Socket)
    end,
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqttd_protocol:shutdown(Error, ProtoState);
        {_,  Reason} ->
            emqttd_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

noreply(State) ->
    {noreply, State, hibernate}.

stop(Reason, State) ->
    {stop, Reason, State}.

with_session(Fun, State = #state{proto_state = ProtoState}) ->
    Fun(emqttd_protocol:session(ProtoState)), noreply(State).

%% receive and parse tcp data
received(<<>>, State) ->
    noreply(State);

received(Bytes, State = #state{parser      = Parser,
                               packet_opts = PacketOpts,
                               proto_state = ProtoState}) ->
    case catch Parser(Bytes) of
        {more, NewParser} ->
            noreply(run_socket(State#state{parser = NewParser}));
        {ok, Packet, Rest} ->
            emqttd_metrics:received(Packet),
            case emqttd_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    received(Rest, State#state{parser = emqttd_parser:new(PacketOpts),
                                               proto_state = ProtoState1});
                {error, Error} ->
                    ?ERROR("Protocol error - ~p", [Error], State),
                    stop({shutdown, Error}, State);
                {error, Error, ProtoState1} ->
                    stop({shutdown, Error}, State#state{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#state{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?ERROR("Framing error - ~p", [Error], State),
            stop({shutdown, Error}, State);
        {'EXIT', Reason} ->
            ?ERROR("Parser failed for ~p~nError Frame: ~p", [Reason, Bytes], State),
            {stop, {shutdown, frame_error}, State}
    end.

network_error(Reason, State = #state{peername = Peername}) ->
    lager:warning("Client(~s): network error - ~p",
                      [emqttd_net:format(Peername), Reason]),
    stop({shutdown, conn_closed}, State).

rate_limit(_Size, State = #state{rate_limiter = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #state{socket = Sock, rate_limiter = Limiter}) ->
    {ok, BufSizes} = inet:getopts(Sock, [sndbuf, recbuf, buffer]),
    io:format("~p~n", [BufSizes]),
    case esockd_rate_limiter:check(Limiter, Size) of
        {0, Limiter1} ->
            run_socket(State#state{conn_state = running, rate_limiter = Limiter1});
        {Pause, Limiter1} ->
            ?ERROR("~p Received, Rate Limiter Pause for ~w", [Size, Pause], State),
            erlang:send_after(Pause, self(), activate_sock),
            State#state{conn_state = blocked, rate_limiter = Limiter1}    
    end.

run_socket(State = #state{conn_state = blocked}) ->
    State;
run_socket(State = #state{await_recv = true}) ->
    State;
run_socket(State = #state{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#state{await_recv = true}.

send_fun(Transport, Sock) ->
    fun(Data) ->
        try Transport:port_command(Sock, Data) of
            true -> ok
        catch
            error:Error -> exit({socket_error, Error})
        end
    end.
