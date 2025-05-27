%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_client).

-include("emqx_nats.hrl").

-behaviour(gen_server).

-export([
    start_link/0,
    start_link/1,
    stop/1,
    connect/1,
    ping/1,
    subscribe/3,
    subscribe/4,
    unsubscribe/2,
    unsubscribe/3,
    publish/3,
    publish/4,
    receive_message/1,
    receive_message/2,
    receive_message/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    send_invalid_frame/2
]).

-type client() :: pid().
-type options() :: #{
    host => string() | inet:ip_address(),
    port => inet:port_number(),
    user => binary() | undefined,
    pass => binary() | undefined,
    verbose => boolean(),
    pedantic => boolean(),
    tls_required => boolean(),
    %% Client's own options
    auto_respond_ping => boolean()
}.
-type state() :: #{
    socket => inet:socket(),
    parse_state => map(),
    message_queue => [map()],
    waiting => {pid(), pos_integer(), timeout()} | undefined,
    options => options(),
    connected_server_info => nats_message_info() | undefined
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, client()}.
start_link() ->
    start_link(#{}).

-spec start_link(options()) -> {ok, client()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

-spec stop(client()) -> ok.
stop(Client) ->
    gen_server:stop(Client).

-spec connect(client()) -> ok | {error, term()}.
connect(Client) ->
    gen_server:call(Client, connect).

-spec ping(client()) -> ok | {error, term()}.
ping(Client) ->
    gen_server:call(Client, ping).

-spec subscribe(client(), binary(), binary()) -> ok.
subscribe(Client, Subject, Sid) ->
    gen_server:call(Client, {subscribe, Subject, Sid}).

-spec subscribe(client(), binary(), binary(), binary()) -> ok.
subscribe(Client, Subject, Sid, Queue) ->
    gen_server:call(Client, {subscribe, Subject, Sid, Queue}).

-spec unsubscribe(client(), binary()) -> ok.
unsubscribe(Client, Sid) ->
    gen_server:call(Client, {unsubscribe, Sid}).

-spec unsubscribe(client(), binary(), pos_integer()) -> ok.
unsubscribe(Client, Sid, MaxMsgs) when is_integer(MaxMsgs) andalso MaxMsgs > 0 ->
    gen_server:call(Client, {unsubscribe, Sid, MaxMsgs}).

-spec publish(client(), binary(), binary()) -> ok.
publish(Client, Subject, Payload) ->
    publish(Client, Subject, undefined, Payload).

-spec publish(client(), binary(), binary() | undefined, binary()) -> ok.
publish(Client, Subject, ReplyTo, Payload) ->
    gen_server:call(Client, {publish, Subject, ReplyTo, Payload}).

-spec receive_message(client()) -> {ok, [map()]}.
receive_message(Client) ->
    receive_message(Client, 1, 5000).

-spec receive_message(client(), pos_integer()) -> {ok, [map()]}.
receive_message(Client, Count) ->
    receive_message(Client, Count, 5000).

-spec receive_message(client(), pos_integer(), timeout()) -> {ok, [map()]} | {error, busy}.
receive_message(Client, Count, Timeout) ->
    try
        gen_server:call(Client, {receive_message, Count, Timeout})
    catch
        _:_ ->
            {ok, []}
    end.

-spec send_invalid_frame(client(), binary()) -> ok.
send_invalid_frame(Client, Data) ->
    gen_server:call(Client, {send_invalid_frame, Data}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(options()) -> {ok, state()}.
init([Options]) ->
    Host = maps:get(host, Options, "tcp://localhost"),
    Port = maps:get(port, Options, 4222),
    {ok, Socket} = connect(Host, Port),
    ParseState = emqx_nats_frame:initial_parse_state(undefined),
    {ok, #{
        socket => Socket,
        parse_state => ParseState,
        message_queue => [],
        waiting => undefined,
        options => Options,
        connected_server_info => undefined
    }}.

handle_call(connect, _From, #{socket := Socket} = State) ->
    ConnectFrame = #nats_frame{operation = ?OP_CONNECT, message = connect_opts(State)},
    send_msg(Socket, ConnectFrame),
    {reply, ok, State};
handle_call(ping, _From, #{socket := Socket} = State) ->
    PingFrame = #nats_frame{operation = ?OP_PING},
    send_msg(Socket, PingFrame),
    {reply, ok, State};
handle_call({subscribe, Subject, Sid}, _From, #{socket := Socket} = State) ->
    SubFrame = #nats_frame{operation = ?OP_SUB, message = #{subject => Subject, sid => Sid}},
    send_msg(Socket, SubFrame),
    {reply, ok, State};
handle_call({subscribe, Subject, Sid, Queue}, _From, #{socket := Socket} = State) ->
    SubFrame = #nats_frame{
        operation = ?OP_SUB, message = #{subject => Subject, sid => Sid, queue_group => Queue}
    },
    send_msg(Socket, SubFrame),
    {reply, ok, State};
handle_call({unsubscribe, Sid}, _From, #{socket := Socket} = State) ->
    UnsubFrame = #nats_frame{operation = ?OP_UNSUB, message = #{sid => Sid}},
    send_msg(Socket, UnsubFrame),
    {reply, ok, State};
handle_call({unsubscribe, Sid, MaxMsgs}, _From, #{socket := Socket} = State) ->
    UnsubFrame = #nats_frame{operation = ?OP_UNSUB, message = #{sid => Sid, max_msgs => MaxMsgs}},
    send_msg(Socket, UnsubFrame),
    {reply, ok, State};
handle_call({publish, Subject, ReplyTo, Payload}, _From, #{socket := Socket} = State) ->
    PubFrame = #nats_frame{
        operation = ?OP_PUB,
        message = #{subject => Subject, reply_to => ReplyTo, payload => Payload}
    },
    send_msg(Socket, PubFrame),
    {reply, ok, State};
handle_call({receive_message, Count, Timeout}, From, #{message_queue := Queue} = State) ->
    case length(Queue) >= Count of
        true ->
            {Messages, Remaining} = lists:split(Count, Queue),
            {reply, {ok, Messages}, State#{message_queue => Remaining}};
        false ->
            case State of
                #{waiting := undefined} ->
                    {noreply, State#{
                        waiting => {From, Count, Timeout},
                        message_queue => Queue
                    }};
                _ ->
                    {reply, {error, busy}, State}
            end
    end;
handle_call({send_invalid_frame, Data}, _From, #{socket := Socket} = State) ->
    send_data(Socket, Data),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tcp, _Socket, Data}, State) ->
    handle_incoming_data(Data, State);
handle_info({tcp_closed, _}, State = #{message_queue := Queue}) ->
    {noreply, State#{message_queue => Queue ++ [tcp_closed]}};
handle_info({tcp_error, _Socket, _Reason}, State) ->
    {stop, normal, State};
handle_info({gun_ws, _ConnPid, _StreamRef, {_Type, Msg}}, State) ->
    handle_incoming_data(Msg, State);
handle_info({gun_down, _ConnPid, ws, _, _}, State = #{message_queue := Queue}) ->
    {noreply, State#{message_queue => Queue ++ [tcp_closed]}};
handle_info(Info, State) ->
    ct:pal("[nats-client] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_incoming_data(Data, #{parse_state := ParseState, message_queue := Queue} = State) ->
    case emqx_nats_frame:parse(Data, ParseState) of
        {ok, Frame, Rest, NewParseState} ->
            State1 = process_parsed_frame(Frame, State#{
                parse_state => NewParseState, message_queue => Queue
            }),
            State2 = handle_message_with_state(Frame, State1),
            handle_incoming_data(Rest, State2);
        {more, NewParseState} ->
            {noreply, State#{parse_state => NewParseState}}
    end.

process_parsed_frame(?PACKET(?OP_INFO, ServerInfo), State) ->
    State#{connected_server_info => ServerInfo};
process_parsed_frame(?PACKET(?OP_PING), State = #{socket := Socket, options := Options}) ->
    case maps:get(auto_respond_ping, Options, true) of
        true ->
            PongFrame = #nats_frame{operation = ?OP_PONG},
            send_msg(Socket, PongFrame),
            State;
        false ->
            State
    end;
process_parsed_frame(_Frame, State) ->
    State.

handle_message_with_state(Frame, #{message_queue := Queue} = State) ->
    NewQueue = Queue ++ [Frame],
    case State of
        #{waiting := {From, Count, _}} when length(NewQueue) >= Count ->
            {Messages, Remaining} = lists:split(Count, NewQueue),
            gen_server:reply(From, {ok, Messages}),
            State#{
                message_queue => Remaining,
                waiting => undefined
            };
        _ ->
            State#{message_queue => NewQueue}
    end.

connect_opts(#{options := Options}) ->
    Opts = #{
        verbose => maps:get(verbose, Options, false),
        pedantic => maps:get(pedantic, Options, false),
        tls_required => maps:get(tls_required, Options, false),
        user => maps:get(user, Options, undefined),
        pass => maps:get(pass, Options, undefined),
        no_responders => maps:get(no_responders, Options, undefined),
        headers => maps:get(headers, Options, undefined)
    },
    maps:filter(fun(_, V) -> V =/= undefined end, Opts).

serialize_pkt(Frame) ->
    emqx_nats_frame:serialize_pkt(
        Frame,
        emqx_nats_frame:serialize_opts()
    ).

%%--------------------------------------------------------------------
%% Socket and Websocket
%%--------------------------------------------------------------------

connect("tcp://" ++ Host, Port) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, true}]),
    {ok, {tcp, Socket}};
connect("ws://" ++ Host, Port) ->
    Timeout = 5000,
    ConnOpts = #{connect_timeout => 5000},
    case gun:open(Host, Port, ConnOpts) of
        {ok, ConnPid} ->
            {ok, _} = gun:await_up(ConnPid, Timeout),
            case upgrade(ConnPid, Timeout) of
                {ok, StreamRef} -> {ok, {ws, {ConnPid, StreamRef}}};
                Error -> Error
            end;
        Error ->
            Error
    end.

upgrade(ConnPid, Timeout) ->
    Path = binary_to_list(<<"/">>),
    WsHeaders = [{<<"cache-control">>, <<"no-cache">>}],
    StreamRef = gun:ws_upgrade(ConnPid, Path, WsHeaders, #{
        protocols => [{<<"NATS">>, gun_ws_h}]
    }),
    receive
        {gun_upgrade, ConnPid, StreamRef, [<<"websocket">>], _Headers} ->
            {ok, StreamRef};
        {gun_response, ConnPid, _, _, Status, Headers} ->
            {error, {ws_upgrade_failed, Status, Headers}};
        {gun_error, ConnPid, StreamRef, Reason} ->
            {error, {ws_upgrade_failed, Reason}}
    after Timeout ->
        {error, timeout}
    end.

send_msg({tcp, Socket}, Frame) ->
    Bin = serialize_pkt(Frame),
    ok = gen_tcp:send(Socket, Bin);
send_msg({ws, {ConnPid, StreamRef}}, Frame) ->
    Bin = serialize_pkt(Frame),
    gun:ws_send(ConnPid, StreamRef, {text, Bin}).

send_data({tcp, Socket}, Data) ->
    ok = gen_tcp:send(Socket, Data);
send_data({ws, {ConnPid, StreamRef}}, Data) ->
    gun:ws_send(ConnPid, StreamRef, {text, Data}).

close({tcp, Socket}) ->
    gen_tcp:close(Socket);
close({ws, {ConnPid, _StreamRef}}) ->
    gun:shutdown(ConnPid).
