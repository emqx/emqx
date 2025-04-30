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
    unsubscribe/2,
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

-type client() :: pid().
-type options() :: #{
    host => string() | inet:ip_address(),
    port => inet:port_number(),
    user => binary() | undefined,
    pass => binary() | undefined,
    verbose => boolean(),
    pedantic => boolean(),
    tls_required => boolean()
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

-spec unsubscribe(client(), binary()) -> ok.
unsubscribe(Client, Sid) ->
    gen_server:call(Client, {unsubscribe, Sid}).

-spec publish(client(), binary(), binary()) -> ok.
publish(Client, Subject, Payload) ->
    publish(Client, Subject, undefined, Payload).

-spec publish(client(), binary(), binary() | undefined, binary()) -> ok.
publish(Client, Subject, ReplyTo, Payload) ->
    gen_server:call(Client, {publish, Subject, ReplyTo, Payload}).

-spec receive_message(client()) -> {ok, [map()]}.
receive_message(Client) ->
    receive_message(Client, 1, 1000).

-spec receive_message(client(), pos_integer()) -> {ok, [map()]}.
receive_message(Client, Count) ->
    receive_message(Client, Count, 1000).

-spec receive_message(client(), pos_integer(), timeout()) -> {ok, [map()]}.
receive_message(Client, Count, Timeout) ->
    gen_server:call(Client, {receive_message, Count, Timeout}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(options()) -> {ok, state()}.
init([Options]) ->
    Host = maps:get(host, Options, "localhost"),
    Port = maps:get(port, Options, 4222),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, true}]),
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
    case is_server_ready(State) of
        false ->
            {reply, {error, not_ready}, State};
        true ->
            ConnectFrame = #nats_frame{operation = ?OP_CONNECT, message = connect_opts(State)},
            Bin = serialize_pkt(ConnectFrame),
            ok = gen_tcp:send(Socket, Bin),
            {reply, ok, State}
    end;
handle_call(ping, _From, #{socket := Socket} = State) ->
    PingFrame = #nats_frame{operation = ?OP_PING},
    Bin = serialize_pkt(PingFrame),
    ok = gen_tcp:send(Socket, Bin),
    {reply, ok, State};
handle_call({subscribe, Subject, Sid}, _From, #{socket := Socket} = State) ->
    SubFrame = #nats_frame{operation = ?OP_SUB, message = #{subject => Subject, sid => Sid}},
    Bin = serialize_pkt(SubFrame),
    ok = gen_tcp:send(Socket, Bin),
    {reply, ok, State};
handle_call({unsubscribe, Sid}, _From, #{socket := Socket} = State) ->
    UnsubFrame = #nats_frame{operation = ?OP_UNSUB, message = #{sid => Sid}},
    Bin = serialize_pkt(UnsubFrame),
    ok = gen_tcp:send(Socket, Bin),
    {reply, ok, State};
handle_call({publish, Subject, ReplyTo, Payload}, _From, #{socket := Socket} = State) ->
    PubFrame = #nats_frame{
        operation = ?OP_PUB,
        message = #{subject => Subject, reply_to => ReplyTo, payload => Payload}
    },
    Bin = serialize_pkt(PubFrame),
    ok = gen_tcp:send(Socket, Bin),
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
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #{socket := Socket} = State) ->
    handle_tcp_data(Data, State);
handle_info({tcp_closed, Socket}, #{socket := Socket} = State) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #{socket := Socket} = State) ->
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
    gen_tcp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_tcp_data(Data, #{parse_state := ParseState, message_queue := Queue} = State) ->
    case emqx_nats_frame:parse(Data, ParseState) of
        {ok, Frame, Rest, NewParseState} ->
            State1 = process_parsed_frame(Frame, State#{
                parse_state => NewParseState, message_queue => Queue
            }),
            State2 = handle_message_with_state(Frame, State1),
            handle_tcp_data(Rest, State2);
        {more, NewParseState} ->
            {noreply, State#{parse_state => NewParseState}}
    end.

process_parsed_frame(?PACKET(?OP_INFO, ServerInfo), State) ->
    State#{connected_server_info => ServerInfo};
process_parsed_frame(?PACKET(?OP_PING), State = #{socket := Socket}) ->
    PongFrame = #nats_frame{operation = ?OP_PONG},
    Bin = serialize_pkt(PongFrame),
    ok = gen_tcp:send(Socket, Bin),
    State;
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

is_server_ready(#{socket := Socket, connected_server_info := ServerInfo}) ->
    case ServerInfo of
        undefined ->
            false;
        _ ->
            case inet:peername(Socket) of
                {ok, {_, _}} ->
                    true;
                _ ->
                    false
            end
    end.

connect_opts(#{options := Options, connected_server_info := ServerInfo}) ->
    ClientVerbose = maps:get(verbose, Options, false),
    ServerVerbose = maps:get(verbose, ServerInfo, false),
    Verbose = ClientVerbose and ServerVerbose,
    #{
        verbose => Verbose,
        pedantic => maps:get(pedantic, Options, false),
        tls_required => maps:get(tls_required, Options, false),
        user => maps:get(user, Options, undefined),
        pass => maps:get(pass, Options, undefined)
    }.

serialize_pkt(Frame) ->
    emqx_nats_frame:serialize_pkt(
        Frame,
        emqx_nats_frame:serialize_opts()
    ).
