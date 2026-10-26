%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% A minimal Kafka broker used to observe the requests a Kafka client sends.
%%
%% It listens on two addresses: the bootstrap address (given to the connector
%% as `bootstrap_hosts`) and the leader address (advertised in metadata
%% responses). This makes it possible to tell metadata/bootstrap connections
%% apart from the connections used to produce messages.
%%
%% Every request is reported to the owner process as:
%%   {kafka_request, bootstrap | leader, ConnId, ApiKey, ApiVersion, ClientId}
%%
%% Requests are answered with minimal protocol responses so that connectors can
%% become connected and messages can actually be produced.

-module(emqx_bridge_kafka_fake_broker).

-behaviour(gen_server).

-export([
    start_link/1, start_link/2, stop/1, close_connections/1, bootstrap_address/1, leader_address/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(API_PRODUCE, 0).
-define(API_METADATA, 3).
-define(API_API_VERSIONS, 18).

-record(state, {
    owner :: pid(),
    options = #{} :: map(),
    listeners = #{} :: #{bootstrap | leader => {gen_tcp:socket(), inet:port_number()}},
    conns = #{} :: #{reference() => {bootstrap | leader, gen_tcp:socket()}},
    offset = 0 :: non_neg_integer()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Owner) ->
    start_link(Owner, #{}).

start_link(Owner, Options) ->
    gen_server:start_link(?MODULE, {Owner, maps:merge(#{jmq => false}, Options)}, []).

stop(Broker) ->
    gen_server:stop(Broker).

close_connections(Broker) ->
    gen_server:call(Broker, close_connections).

bootstrap_address(Broker) ->
    gen_server:call(Broker, {address, bootstrap}).

leader_address(Broker) ->
    gen_server:call(Broker, {address, leader}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init({Owner, Options}) ->
    Listeners = maps:from_list([
        {Role, listen(Role, self())}
     || Role <- [bootstrap, leader]
    ]),
    {ok, #state{owner = Owner, listeners = Listeners, options = Options}}.

listen(Role, Server) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary,
        {packet, 4},
        {active, false},
        {reuseaddr, true},
        {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(LSock),
    _ = spawn_link(fun() -> accept_loop(Server, Role, LSock) end),
    {LSock, Port}.

accept_loop(Server, Role, LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            gen_server:cast(Server, {accepted, Role, Sock}),
            accept_loop(Server, Role, LSock);
        {error, _} ->
            ok
    end.

handle_call({address, Role}, _From, State) ->
    {_LSock, Port} = maps:get(Role, State#state.listeners),
    {reply, {"127.0.0.1", Port}, State};
handle_call(options, _From, State) ->
    {reply, State#state.options, State};
handle_call(next_offset, _From, State = #state{offset = Offset}) ->
    {reply, Offset, State#state{offset = Offset + 1}};
handle_call(close_connections, _From, State = #state{conns = Conns}) ->
    _ = [gen_tcp:close(Sock) || {_Role, Sock} <- maps:values(Conns)],
    {reply, ok, State#state{conns = #{}}};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({accepted, Role, Sock}, State = #state{conns = Conns, owner = Owner}) ->
    Server = self(),
    ConnId = make_ref(),
    _ = spawn_link(fun() -> connection_loop(Server, Owner, Role, ConnId, Sock) end),
    {noreply, State#state{conns = Conns#{ConnId => {Role, Sock}}}};
handle_cast({closed, ConnId}, State = #state{conns = Conns}) ->
    {noreply, State#state{conns = maps:remove(ConnId, Conns)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{listeners = Listeners, conns = Conns}) ->
    _ = [gen_tcp:close(Sock) || {_Role, Sock} <- maps:values(Conns)],
    _ = [gen_tcp:close(LSock) || {LSock, _Port} <- maps:values(Listeners)],
    ok.

%%--------------------------------------------------------------------
%% Connection handling
%%--------------------------------------------------------------------

connection_loop(Server, Owner, Role, ConnId, Sock) ->
    case gen_tcp:recv(Sock, 0, infinity) of
        {ok, Packet} ->
            case handle_request(Server, Owner, Role, ConnId, Sock, Packet) of
                ok ->
                    connection_loop(Server, Owner, Role, ConnId, Sock);
                {error, _} ->
                    gen_tcp:close(Sock),
                    gen_server:cast(Server, {closed, ConnId})
            end;
        {error, _} ->
            gen_tcp:close(Sock),
            gen_server:cast(Server, {closed, ConnId})
    end.

handle_request(Server, Owner, Role, ConnId, Sock, Packet) ->
    case parse_header(Packet) of
        {ApiKey, ApiVersion, CorrId, ClientId, Body} ->
            Owner ! {kafka_request, Role, ConnId, ApiKey, ApiVersion, ClientId},
            case response(Server, ApiKey, ApiVersion, CorrId, Body) of
                undefined ->
                    ok;
                Response ->
                    gen_tcp:send(Sock, Response)
            end;
        error ->
            ok
    end.

parse_header(
    <<ApiKey:16/signed, ApiVersion:16/signed, CorrId:32/signed, ClientIdLen:16/signed, Rest/binary>>
) ->
    case ClientIdLen of
        -1 ->
            {ApiKey, ApiVersion, CorrId, undefined, Rest};
        Len when Len >= 0, byte_size(Rest) >= Len ->
            <<ClientId:Len/binary, Body/binary>> = Rest,
            {ApiKey, ApiVersion, CorrId, ClientId, Body};
        _ ->
            error
    end;
parse_header(_) ->
    error.

response(Server, ?API_API_VERSIONS, 0, CorrId, _Body) ->
    #{jmq := Jmq} = gen_server:call(Server, options),
    Base = <<?API_METADATA:16, 0:16, 0:16, ?API_PRODUCE:16, 0:16, 0:16>>,
    {Count, Entries, Tail} =
        case Jmq of
            false -> {2, Base, <<>>};
            true -> {4, <<Base/binary, 17:16, 0:16, 1:16, 36:16, 0:16, 1:16>>, <<0:32>>}
        end,
    <<CorrId:32, 0:16, Count:32, Entries/binary, Tail/binary>>;
response(_Server, 17, 1, CorrId, <<5:16, "PLAIN">>) ->
    <<CorrId:32, 0:16, 1:32, 5:16, "PLAIN">>;
response(_Server, 36, 1, CorrId, <<Len:32, Token:Len/binary>>) ->
    <<0, "test-user", 0, "test-password">> = Token,
    %% The captured service returns a non-empty final PLAIN token and no lifetime.
    <<CorrId:32, 0:16, -1:16/signed, Len:32, Token/binary, 0:64>>;
response(Server, ?API_METADATA, _ApiVersion, CorrId, Body) ->
    {_Host, Port} = gen_server:call(Server, {address, leader}),
    Topics = parse_topics(Body),
    EncodedTopics = [encode_topic(Topic) || Topic <- Topics],
    <<CorrId:32/signed, 1:32/signed, 0:32/signed, (byte_size(<<"127.0.0.1">>)):16/signed,
        <<"127.0.0.1">>/binary, Port:32/signed, (length(EncodedTopics)):32/signed,
        (iolist_to_binary(EncodedTopics))/binary>>;
response(Server, ?API_PRODUCE, _ApiVersion, CorrId, Body) ->
    case parse_produce(Body) of
        {Topic, Partition} ->
            Offset = gen_server:call(Server, next_offset),
            <<CorrId:32/signed, 1:32/signed, (byte_size(Topic)):16/signed, Topic/binary,
                1:32/signed, Partition:32/signed, 0:16/signed, Offset:64/signed>>;
        error ->
            undefined
    end;
response(_Server, _ApiKey, _ApiVersion, _CorrId, _Body) ->
    undefined.

parse_topics(<<Count:32/signed, Rest/binary>>) ->
    parse_topics(Count, Rest, []);
parse_topics(_) ->
    [].

parse_topics(0, _Rest, Acc) ->
    lists:reverse(Acc);
parse_topics(N, <<Len:16/signed, Topic:Len/binary, Rest/binary>>, Acc) when N > 0 ->
    parse_topics(N - 1, Rest, [Topic | Acc]);
parse_topics(_N, _Rest, Acc) ->
    lists:reverse(Acc).

encode_topic(Topic) ->
    <<0:16/signed, (byte_size(Topic)):16/signed, Topic/binary, 1:32/signed, 0:16/signed,
        0:32/signed, 0:32/signed, 1:32/signed, 0:32/signed, 1:32/signed, 0:32/signed>>.

parse_produce(<<_Acks:16/signed, _Timeout:32/signed, _Count:32/signed, Rest/binary>>) ->
    case Rest of
        <<Len:16/signed, Topic:Len/binary, _DataCount:32/signed, Partition:32/signed, _/binary>> ->
            {Topic, Partition};
        _ ->
            error
    end;
parse_produce(_) ->
    error.
