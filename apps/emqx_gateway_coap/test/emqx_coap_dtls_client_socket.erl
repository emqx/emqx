%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_dtls_client_socket).
-behaviour(gen_server).

-export([connect/2, connect/3, close/1, get_channel/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {sock, channel}).

connect(Host, Port) ->
    connect(Host, Port, []).

connect(Host, Port, ConnectOpts) ->
    {ok, Socket} = gen_server:start_link(?MODULE, [connect, Host, Port, ConnectOpts], []),
    {ok, Channel} = get_channel(Socket),
    {ok, Socket, Channel}.

close(Pid) ->
    gen_server:cast(Pid, shutdown).

get_channel(Pid) ->
    gen_server:call(Pid, get_channel).

init([connect, Host, Port, ConnectOpts]) ->
    {ok, Sock} = ssl:connect(Host, Port, [binary, {protocol, dtls} | ConnectOpts]),
    ChId = {Host, Port},
    {ok, Pid} = er_coap_channel:start_link(self(), ChId),
    {ok, #state{sock = Sock, channel = Pid}}.

handle_call(get_channel, _From, State = #state{channel = Chan}) ->
    {reply, {ok, Chan}, State};
handle_call(_Unknown, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(shutdown, State) ->
    {stop, normal, State}.

handle_info({ssl, _Socket, Data}, State = #state{channel = Chan}) ->
    Chan ! {datagram, Data},
    {noreply, State};
handle_info({ssl_closed, _Socket}, State) ->
    {stop, normal, State};
handle_info({datagram, _ChId, Data}, State = #state{sock = Socket}) ->
    ok = ssl:send(Socket, Data),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.
