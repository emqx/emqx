%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_client).

-behaviour(gen_statem).

-include("emqx_mqtt.hrl").
-include("emqx_client.hrl").

-export([start_link/0, start_link/1]).

-export([subscribe/2, subscribe/3, unsubscribe/2]).
-export([publish/2, publish/3, publish/4, publish/5]).
-export([ping/1]).
-export([disconnect/1, disconnect/2]).
-export([puback/2, pubrec/2, pubrel/2, pubcomp/2]).
-export([subscriptions/1]).

-export([initialized/3, waiting_for_connack/3, connected/3]).
-export([init/1, callback_mode/0, terminate/3, code_change/4]).

-type(host() :: inet:ip_address() | inet:hostname()).

-type(option() :: {name, atom()}
                | {owner, pid()}
                | {host, host()}
                | {hosts, [{host(), inet:port_number()}]}
                | {port, inet:port_number()}
                | {tcp_opts, [gen_tcp:option()]}
                | {ssl, boolean()}
                | {ssl_opts, [ssl:ssl_option()]}
                | {client_id, iodata()}
                | {clean_start, boolean()}
                | {username, iodata()}
                | {password, iodata()}
                | {proto_ver, v3 | v4 | v5}
                | {keepalive, non_neg_integer()}
                | {max_inflight, pos_integer()}
                | {retry_interval, timeout()}
                | {will_topic, iodata()}
                | {will_payload, iodata()}
                | {will_retain, boolean()}
                | {will_qos, mqtt_qos() | mqtt_qos_name()}
                | {connect_timeout, pos_integer()}
                | {ack_timeout, pos_integer()}
                | {force_ping, boolean()}
                | {debug_mode, boolean()}).

-export_type([option/0]).

-record(state, {name            :: atom(),
                owner           :: pid(),
                host            :: host(),
                port            :: inet:port_number(),
                hosts           :: [{host(), inet:port_number()}],
                socket          :: inet:socket(),
                sock_opts       :: [emqx_client_sock:option()],
                receiver        :: pid(),
                client_id       :: binary(),
                clean_start     :: boolean(),
                username        :: binary() | undefined,
                password        :: binary() | undefined,
                proto_ver       :: mqtt_vsn(),
                proto_name      :: iodata(),
                keepalive       :: non_neg_integer(),
                keepalive_timer :: reference() | undefined,
                force_ping      :: boolean(),
                will_flag       :: boolean(),
                will_msg        :: mqtt_message(),
                pending_calls   :: list(),
                subscribers     :: list(),
                subscriptions   :: map(),
                max_inflight    :: infinity | pos_integer(),
                inflight        :: emqx_inflight:inflight(),
                awaiting_rel    :: map(),
                properties      :: list(),
                auto_ack        :: boolean(),
                ack_timeout     :: pos_integer(),
                ack_timer       :: reference(),
                retry_interval  :: pos_integer(),
                retry_timer     :: reference(),
                connect_timeout :: pos_integer(),
                last_packet_id  :: mqtt_packet_id(),
                debug_mode      :: boolean()}).

-record(call, {id, from, req, ts}).

-type(client() :: pid() | atom()).

-type(msgid() :: mqtt_packet_id()).

-type(pubopt() :: {retain, boolean()}
                | {qos, mqtt_qos()}).

-type(subopt() :: mqtt_subopt()).

-export_type([client/0, host/0, msgid/0, pubopt/0, subopt/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> gen_statem:start_ret()).
start_link() -> start_link([]).

-spec(start_link(map() | [option()]) -> gen_statem:start_ret()).
start_link(Options) when is_map(Options) ->
    start_link(maps:to_list(Options));
start_link(Options) when is_list(Options) ->
    case start_client(with_owner(Options)) of
        {ok, Client} ->
            connect(Client);
        Error -> Error
    end.

start_client(Options) ->
    case proplists:get_value(name, Options) of
        undefined ->
            gen_statem:start_link(?MODULE, [Options], []);
        Name when is_atom(Name) ->
            gen_statem:start_link({local, Name}, ?MODULE, [Options], [])
    end.

with_owner(Options) ->
    case proplists:get_value(owner, Options) of
        Owner when is_pid(Owner) -> Options;
        undefined -> [{owner, self()} | Options]
    end.

%% @private
%% @doc should be called with start_link
-spec(connect(client()) -> {ok, client()} | {error, term()}).
connect(Client) ->
    gen_statem:call(Client, connect, infinity).

%% @doc Publish QoS0 message to broker.
-spec(publish(client(), iolist(), iodata()) -> ok | {error, term()}).
publish(Client, Topic, Payload) ->
    publish(Client, #mqtt_message{topic   = iolist_to_binary(Topic),
                                  payload = iolist_to_binary(Payload)}).

%% @doc Publish message to broker with qos, retain options.
-spec(publish(client(), iolist(), iodata(),
              mqtt_qos() | mqtt_qos_name() | [pubopt()])
      -> ok | {ok, msgid()} | {error, term()}).
publish(Client, Topic, Payload, QoS) when is_atom(QoS) ->
    publish(Client, Topic, Payload, ?QOS_I(QoS));
publish(Client, Topic, Payload, QoS) when ?IS_QOS(QoS) ->
    publish(Client, Topic, Payload, [{qos, QoS}]);
publish(Client, Topic, Payload, Options) when is_list(Options) ->
    publish(Client, Topic, [], Payload, Options).
publish(Client, Topic, Properties, Payload, Options) ->
    ok = emqx_mqtt_properties:validate(Properties),
    publish(Client, #mqtt_message{qos        = pubopt(qos, Options),
                                  retain     = pubopt(retain, Options),
                                  topic      = iolist_to_binary(Topic),
                                  properties = Properties,
                                  payload    = iolist_to_binary(Payload)}).

%% @doc Publish MQTT Message.
-spec(publish(client(), mqtt_message())
      -> ok | {ok, msgid()} | {error, term()}).
publish(Client, Msg) when is_record(Msg, mqtt_message) ->
    gen_statem:call(Client, {publish, Msg}).

pubopt(qos, Opts) ->
    proplists:get_value(qos, Opts, ?QOS0);
pubopt(retain, Opts) ->
    lists:member(retain, Opts) orelse proplists:get_bool(retain, Opts).

-spec(subscribe(client(), binary()
                        | {binary(), mqtt_qos_name() | mqtt_qos()})
      -> {ok, mqtt_qos()} | {error, term()}).
subscribe(Client, Topic) when is_binary(Topic) ->
    subscribe(Client, Topic, ?QOS_0);
subscribe(Client, {Topic, QoS}) when ?IS_QOS(QoS); is_atom(QoS) ->
    subscribe(Client, Topic, ?QOS_I(QoS));
subscribe(Client, Topics) when is_list(Topics) ->
    case io_lib:printable_unicode_list(Topics) of
        true  -> subscribe(Client, [{Topics, ?QOS_0}]);
        false -> Topics1 = [{iolist_to_binary(Topic), [{qos, ?QOS_I(QoS)}]}
                            || {Topic, QoS} <- Topics],
                 gen_statem:call(Client, {subscribe, Topics1})
    end.

-spec(subscribe(client(), string() | binary(),
                mqtt_qos_name() | mqtt_qos() | [subopt()])
      -> {ok, mqtt_qos()} | {error, term()}).
subscribe(Client, Topic, QoS) when is_atom(QoS) ->
    subscribe(Client, Topic, ?QOS_I(QoS));
subscribe(Client, Topic, QoS) when ?IS_QOS(QoS) ->
    subscribe(Client, Topic, [{qos, QoS}]);
subscribe(Client, Topic, Options) ->
    gen_statem:call(Client, {subscribe, iolist_to_binary(Topic), Options}).

-spec(unsubscribe(client(), iolist()) -> ok | {error, term()}).
unsubscribe(Client, Topic) when is_binary(Topic) ->
    unsubscribe(Client, [Topic]);
unsubscribe(Client, Topics) when is_list(Topics) ->
    case io_lib:printable_unicode_list(Topics) of
        true  -> unsubscribe(Client, [Topics]);
        false ->
            Topics1 = [iolist_to_binary(Topic) || Topic <- Topics],
            gen_statem:call(Client, {unsubscribe, Topics1})
    end.

-spec(ping(client()) -> pong).
ping(Client) ->
    gen_statem:call(Client, ping).

-spec(disconnect(client()) -> ok).
disconnect(C) ->
    disconnect(C, []).

disconnect(Client, Props) ->
    gen_statem:call(Client, {disconnect, Props}).

%%--------------------------------------------------------------------
%% APIs for broker test cases.
%%--------------------------------------------------------------------

puback(Client, PacketId) when is_integer(PacketId) ->
    gen_statem:cast(Client, {puback, PacketId}).

pubrec(Client, PacketId) when is_integer(PacketId) ->
    gen_statem:cast(Client, {pubrec, PacketId}).

pubrel(Client, PacketId) when is_integer(PacketId) ->
    gen_statem:cast(Client, {pubrel, PacketId}).

pubcomp(Client, PacketId) when is_integer(PacketId) ->
    gen_statem:cast(Client, {pubcomp, PacketId}).

subscriptions(C) -> gen_statem:call(C, subscriptions).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init([Options]) ->
    process_flag(trap_exit, true),
    ClientId = case {proplists:get_value(proto_ver, Options, v4),
                     proplists:get_value(client_id, Options)} of
                   {v5, undefined}   -> undefined;
                   {_ver, undefined} -> random_client_id();
                   {_ver, Id}        -> iolist_to_binary(Id)
               end,
    State = init(Options, #state{host            = {127,0,0,1},
                                 port            = 1883,
                                 hosts           = [],
                                 sock_opts       = [],
                                 client_id       = ClientId,
                                 clean_start     = true,
                                 proto_ver       = ?MQTT_PROTO_V4,
                                 proto_name      = <<"MQTT">>,
                                 keepalive       = ?DEFAULT_KEEPALIVE,
                                 will_flag       = false,
                                 will_msg        = #mqtt_message{},
                                 ack_timeout     = ?DEFAULT_ACK_TIMEOUT,
                                 connect_timeout = ?DEFAULT_CONNECT_TIMEOUT,
                                 force_ping      = false,
                                 pending_calls   = [],
                                 subscribers     = [],
                                 subscriptions   = #{},
                                 max_inflight    = infinity,
                                 inflight        = emqx_inflight:new(0),
                                 awaiting_rel    = #{},
                                 auto_ack        = true,
                                 retry_interval  = 0,
                                 last_packet_id  = 1,
                                 debug_mode      = false}),
    %% Connect and Send ConnAck
    {ok, initialized, State}.

random_client_id() ->
    rand:seed(exsplus, erlang:timestamp()),
    I1 = rand:uniform(round(math:pow(2, 48))) - 1,
    I2 = rand:uniform(round(math:pow(2, 32))) - 1,
    {ok, Host} = inet:gethostname(),
    iolist_to_binary(["emqx-client-", Host, "-", io_lib:format("~12.16.0b~8.16.0b", [I1, I2])]).

init([], State) ->
    State;
init([{name, Name} | Opts], State) ->
    init(Opts, State#state{name = Name});
init([{owner, Owner} | Opts], State) when is_pid(Owner) ->
    link(Owner),
    init(Opts, State#state{owner = Owner});
init([{host, Host} | Opts], State) ->
    init(Opts, State#state{host = Host});
init([{port, Port} | Opts], State) ->
    init(Opts, State#state{port = Port});
init([{hosts, Hosts} | Opts], State) ->
    Hosts1 =
    lists:foldl(fun({Host, Port}, Acc) ->
                    [{Host, Port}|Acc];
                   (Host, Acc) ->
                    [{Host, 1883}|Acc]
                end, [], Hosts),
    init(Opts, State#state{hosts = Hosts1});
init([{tcp_opts, TcpOpts} | Opts], State = #state{sock_opts = SockOpts}) ->
    init(Opts, State#state{sock_opts = emqx_misc:merge_opts(SockOpts, TcpOpts)});
init([ssl | Opts], State = #state{sock_opts = SockOpts}) ->
    ok = ssl:start(),
    SockOpts1 = emqx_misc:merge_opts([{ssl_opts, []}], SockOpts),
    init(Opts, State#state{sock_opts = SockOpts1});
init([{ssl_opts, SslOpts} | Opts], State = #state{sock_opts = SockOpts}) ->
    ok = ssl:start(),
    SockOpts1 = emqx_misc:merge_opts(SockOpts, [{ssl_opts, SslOpts}]),
    init(Opts, State#state{sock_opts = SockOpts1});
init([{client_id, ClientId} | Opts], State) ->
    init(Opts, State#state{client_id = iolist_to_binary(ClientId)});
init([{clean_start, CleanStart} | Opts], State) when is_boolean(CleanStart) ->
    init(Opts, State#state{clean_start = CleanStart});
init([{useranme, Username} | Opts], State) ->
    init(Opts, State#state{username = iolist_to_binary(Username)});
init([{passwrod, Password} | Opts], State) ->
    init(Opts, State#state{password = iolist_to_binary(Password)});
init([{keepalive, Secs} | Opts], State) ->
    init(Opts, State#state{keepalive = timer:seconds(Secs)});
init([{proto_ver, v3} | Opts], State) ->
    init(Opts, State#state{proto_ver  = ?MQTT_PROTO_V3,
                           proto_name = <<"MQIsdp">>});
init([{proto_ver, v4} | Opts], State) ->
    init(Opts, State#state{proto_ver  = ?MQTT_PROTO_V4,
                           proto_name = <<"MQTT">>});
init([{proto_ver, v5} | Opts], State) ->
    init(Opts, State#state{proto_ver  = ?MQTT_PROTO_V5,
                           proto_name = <<"MQTT">>});
init([{will_topic, Topic} | Opts], State = #state{will_msg = WillMsg}) ->
    WillMsg1 = init_will_msg({topic, Topic}, WillMsg),
    init(Opts, State#state{will_flag = true, will_msg = WillMsg1});
init([{will_payload, Payload} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg  = init_will_msg({payload, Payload}, WillMsg)});
init([{will_retain, Retain} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg  = init_will_msg({retain, Retain}, WillMsg)});
init([{will_qos, QoS} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg  = init_will_msg({qos, QoS}, WillMsg)});
init([{connect_timeout, Timeout}| Opts], State) ->
    init(Opts, State#state{connect_timeout = timer:seconds(Timeout)});
init([{ack_timeout, Timeout}| Opts], State) ->
    init(Opts, State#state{ack_timeout = timer:seconds(Timeout)});
init([force_ping | Opts], State) ->
    init(Opts, State#state{force_ping = true});
init([{force_ping, ForcePing} | Opts], State) when is_boolean(ForcePing) ->
    init(Opts, State#state{force_ping = ForcePing});
init([{max_inflight, infinity} | Opts], State) ->
    init(Opts, State#state{max_inflight = infinity,
                           inflight = emqx_inflight:new(0)});
init([{max_inflight, I} | Opts], State) when is_integer(I) ->
    init(Opts, State#state{max_inflight = I,
                           inflight = emqx_inflight:new(I)});
init([auto_ack | Opts], State) ->
    init(Opts, State#state{auto_ack = true});
init([{auto_ack, AutoAck} | Opts], State) when is_boolean(AutoAck) ->
    init(Opts, State#state{auto_ack = AutoAck});
init([{retry_interval, I} | Opts], State) ->
    init(Opts, State#state{retry_interval = timer:seconds(I)});
init([{debug_mode, Mode} | Opts], State) when is_boolean(Mode) ->
    init(Opts, State#state{debug_mode = Mode});
init([_Opt | Opts], State) ->
    init(Opts, State).

init_will_msg({topic, Topic}, WillMsg) ->
    WillMsg#mqtt_message{topic = iolist_to_binary(Topic)};
init_will_msg({payload, Payload}, WillMsg) ->
    WillMsg#mqtt_message{payload = iolist_to_binary(Payload)};
init_will_msg({retain, Retain}, WillMsg) when is_boolean(Retain) ->
    WillMsg#mqtt_message{retain = Retain};
init_will_msg({qos, QoS}, WillMsg) ->
    WillMsg#mqtt_message{qos = ?QOS_I(QoS)}.

callback_mode() -> state_functions.

initialized({call, From}, connect, State = #state{connect_timeout = Timeout}) ->
    case sock_connect(State) of
        {ok, State1} ->
            case mqtt_connect(State1) of
                {ok, State2} ->
                    {next_state, waiting_for_connack,
                     add_call(new_call(connect, From), State2), [Timeout]};
                Err = {error, Reason} ->
                    {stop_and_reply, Reason, [{reply, From, Err}]}
            end;
        Err = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Err}]}
    end;

initialized(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, StateData).

sock_connect(State) ->
    sock_connect(get_hosts(State), {error, no_hosts}, State).

get_hosts(#state{hosts = [], host = Host, port = Port}) ->
    [{Host, Port}];
get_hosts(#state{hosts = Hosts}) -> Hosts.

sock_connect([], Err, _State) ->
    Err;
sock_connect([{Host, Port} | Hosts], _Err, State = #state{sock_opts = SockOpts}) ->
    case emqx_client_sock:connect(self(), Host, Port, SockOpts) of
        {ok, Socket, Receiver} ->
            {ok, State#state{socket = Socket, receiver = Receiver}};
        Err = {error, _Reason} ->
            sock_connect(Hosts, Err, State)
    end.

mqtt_connect(State = #state{client_id   = ClientId,
                            clean_start = CleanStart,
                            username    = Username,
                            password    = Password,
                            proto_ver   = ProtoVer,
                            proto_name  = ProtoName,
                            keepalive   = KeepAlive,
                            will_flag   = WillFlag,
                            will_msg    = WillMsg}) ->
    #mqtt_message{qos     = WillQos,
                  retain  = WillRetain,
                  topic   = WillTopic,
                  payload = WillPayload} = WillMsg,
    send(?CONNECT_PACKET(
            #mqtt_packet_connect{client_id   = ClientId,
                                 clean_start = CleanStart,
                                 proto_ver   = ProtoVer,
                                 proto_name  = ProtoName,
                                 will_flag   = WillFlag,
                                 will_retain = WillRetain,
                                 will_qos    = WillQos,
                                 keep_alive  = KeepAlive,
                                 will_topic  = WillTopic,
                                 will_msg    = WillPayload,
                                 username    = Username,
                                 password    = Password}), State).

waiting_for_connack(cast, ?CONNACK_PACKET(?CONNACK_ACCEPT,
                                          _SessPresent,
                                          _Properties), State) ->
    case take_call(connect, State) of
        {value, #call{from = From}, State1} ->
            {next_state, connected,
             ensure_keepalive_timer(ensure_ack_timer(State1)),
             [{reply, From, {ok, self()}}]};
        false ->
            io:format("Cannot find call: ~p~n", [State#state.pending_calls]),
            {stop, {error, unexpected_connack}}
    end;

waiting_for_connack(cast, ?CONNACK_PACKET(ReasonCode,
                                          _SessPresent,
                                          _Properties), State) ->
    reply_connack_error(emqx_packet:connack_error(ReasonCode), State);

waiting_for_connack(timeout, _Timeout, State) ->
    reply_connack_error(connack_timeout, State);

waiting_for_connack(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, StateData).

reply_connack_error(Reason, State) ->
    Error = {error, Reason},
    case take_call(connect, State) of
        {value, #call{from = From}, State1} ->
            {stop_and_reply, Error, [{reply, From, Error}], State1};
        false -> {stop, Error}
    end.

connected({call, From}, subscriptions, State = #state{subscriptions = Subscriptions}) ->
    {keep_state, State, [{reply, From, maps:to_list(Subscriptions)}]};

connected({call, From}, {publish, Msg = #mqtt_message{qos = ?QOS_0}}, State) ->
    case send(Msg, State) of
        {ok, NewState} ->
            {keep_state, NewState, [{reply, From, ok}]};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, {publish, Msg = #mqtt_message{qos = Qos}},
          State = #state{inflight = Inflight, last_packet_id = PacketId})
    when (Qos =:= ?QOS_1); (Qos =:= ?QOS_2) ->
    case Inflight:is_full() of
        true ->
            {keep_state, State, [{reply, From, {error, inflight_full}}]};
        false ->
            Msg1 = Msg#mqtt_message{packet_id = PacketId},
            case send(Msg1, State) of
                {ok, NewState} ->
                    Inflight1 = Inflight:insert(PacketId, {publish, Msg1, os:timestamp()}),
                    {keep_state, ensure_retry_timer(NewState#state{inflight = Inflight1}),
                     [{reply, From, {ok, PacketId}}]};
                Error = {error, Reason} ->
                    {stop_and_reply, Reason, [{reply, From, Error}]}
            end
    end;

connected({call, From}, SubReq = {subscribe, Topic, SubOpts},
          State= #state{last_packet_id = PacketId, subscriptions = Subscriptions}) ->
    %%TODO: handle qos...
    QoS = proplists:get_value(qos, SubOpts, ?QOS_0),
    case send(?SUBSCRIBE_PACKET(PacketId, [{Topic, QoS}]), State) of
        {ok, NewState} ->
            Call = new_call({subscribe, PacketId}, From, SubReq),
            Subscriptions1 = maps:put(Topic, SubOpts, Subscriptions),
            {keep_state, ensure_ack_timer(add_call(Call, NewState#state{subscriptions = Subscriptions1}))};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, SubReq = {subscribe, Topics},
          State= #state{last_packet_id = PacketId, subscriptions = Subscriptions}) ->
    case send(?SUBSCRIBE_PACKET(PacketId, Topics), State) of
        {ok, NewState} ->
            Call = new_call({subscribe, PacketId}, From, SubReq),
            Subscriptions1 =
                lists:fold(fun({Topic, SubOpts}, Acc) ->
                               maps:put(Topic, SubOpts, Acc)
                           end, Subscriptions, Topics),
            {keep_state, ensure_ack_timer(add_call(Call, NewState#state{subscriptions = Subscriptions1}))};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, UnsubReq = {unsubscribe, Topics},
          State = #state{last_packet_id = PacketId}) ->
    case send(?UNSUBSCRIBE_PACKET(PacketId, Topics), State) of
        {ok, NewState} ->
            Call = new_call({unsubscribe, PacketId}, From, UnsubReq),
            {keep_state, ensure_ack_timer(add_call(Call, NewState))};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, ping, State) ->
    case send(?PACKET(?PINGREQ), State) of
        {ok, NewState} ->
            Call = new_call(ping, From),
            {keep_state, ensure_ack_timer(add_call(Call, NewState))};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, disconnect, State) ->
    case send(?PACKET(?DISCONNECT), State) of
        {ok, NewState} ->
            {stop_and_reply, normal, [{reply, From, ok}], NewState};
        Error = {error, _Reason} ->
            {stop_and_reply, disconnected, [{reply, From, Error}]}
    end;

connected(cast, {puback, PacketId}, State) ->
    send_puback(?PUBACK_PACKET(?PUBACK, PacketId), State);

connected(cast, {pubrec, PacketId}, State) ->
    send_puback(?PUBACK_PACKET(?PUBREC, PacketId), State);

connected(cast, {pubrel, PacketId}, State) ->
    send_puback(?PUBREL_PACKET(PacketId), State);

connected(cast, {pubcomp, PacketId}, State) ->
    send_puback(?PUBCOMP_PACKET(PacketId), State);

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_0, _PacketId), State) ->
    {keep_state, deliver_msg(packet_to_msg(Packet), State)};

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_1, PacketId),
          State = #state{auto_ack = AutoAck}) ->
    _ = deliver_msg(packet_to_msg(Packet), State),
    case AutoAck of
        true  -> send_puback(?PUBACK_PACKET(PacketId), State);
        false -> {keep_state, State}
    end;

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_2, PacketId),
          State = #state{awaiting_rel = AwaitingRel}) ->
    case send_puback(?PUBREC_PACKET(PacketId), State) of
        {keep_state, NewState} ->
            AwaitingRel1 = maps:put(PacketId, Packet, AwaitingRel),
            {keep_state, NewState#state{awaiting_rel = AwaitingRel1}};
        Stop -> Stop
    end;

connected(cast, ?PUBACK_PACKET(PacketId),
          State = #state{owner = Owner, inflight = Inflight}) ->
    case Inflight:lookup(PacketId) of
        {value, {publish, #mqtt_message{packet_id = PacketId}, _Ts}} ->
            Owner ! {puback, PacketId},
            {keep_state, State#state{inflight = Inflight:delete(PacketId)}};
        none ->
            ?LOG(warning, "Unexpected PUBACK: ~p", [PacketId]),
            {keep_state, State}
    end;

connected(cast, ?PUBREC_PACKET(PacketId), State = #state{inflight = Inflight}) ->
    send_puback(?PUBREL_PACKET(PacketId),
                case Inflight:lookup(PacketId) of
                    {value, {publish, _Msg, _Ts}} ->
                        Inflight1 = Inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}),
                        State#state{inflight = Inflight1};
                    {value, {pubrel, _Ref, _Ts}} ->
                        ?LOG(warning, "Duplicated PUBREC Packet: ~p", [PacketId]),
                        State;
                    none ->
                        ?LOG(warning, "Unexpected PUBREC Packet: ~p", [PacketId]),
                        State
                end);

%%TODO::... if auto_ack is false, should we take PacketId from the map?
connected(cast, ?PUBREL_PACKET(PacketId), State = #state{awaiting_rel = AwaitingRel,
                                                         auto_ack = AutoAck}) ->
     case maps:take(PacketId, AwaitingRel) of
         {Packet, AwaitingRel1} ->
             NewState = deliver_msg(packet_to_msg(Packet),
                                    State#state{awaiting_rel = AwaitingRel1}),
             case AutoAck of
                 true  -> send_puback(?PUBCOMP_PACKET(PacketId), NewState);
                 false -> {keep_state, NewState}
             end;
         error ->
             ?LOG(warning, "Unexpected PUBREL: ~p", [PacketId]),
             {keep_state, State}
     end;

connected(cast, ?PUBCOMP_PACKET(PacketId), State = #state{owner = Owner, inflight = Inflight}) ->
    case Inflight:lookup(PacketId) of
        {value, {pubrel, _PacketId, _Ts}} ->
            Owner ! {pubcomp, PacketId},
            {keep_state, State#state{inflight = Inflight:delete(PacketId)}};
        none ->
            ?LOG(warning, "Unexpected PUBCOMP Packet: ~p", [PacketId]),
            {keep_state, State}
     end;

%%TODO: handle suback...
connected(cast, ?SUBACK_PACKET(PacketId, QosTable),
          State = #state{subscriptions = Subscriptions}) ->
    ?LOG(info, "SUBACK(~p) Received", [PacketId]),
    case take_call({subscribe, PacketId}, State) of
        {value, #call{from = From}, State1} ->
            {keep_state, State1, [{reply, From, ok}]};
        false -> {keep_state, State}
    end;

%%TODO: handle unsuback...
connected(cast, ?UNSUBACK_PACKET(PacketId),
          State = #state{subscriptions = Subscriptions}) ->
    ?LOG(info, "UNSUBACK(~p) received", [PacketId]),
    case take_call({unsubscribe, PacketId}, State) of
        {value, #call{from = From, req = {_, Topics}}, State1} ->
            {keep_state, State1#state{subscriptions =
                                      lists:foldl(fun(Topic, Subs) ->
                                                      maps:remove(Topic, Subs)
                                                  end, Subscriptions, Topics)},
             [{reply, From, ok}]};
        false -> {keep_state, State}
    end;

%%TODO: handle PINGRESP...
connected(cast, ?PACKET(?PINGRESP), State = #state{pending_calls = []}) ->
    {keep_state, State};
connected(cast, ?PACKET(?PINGRESP), State) ->
    case take_call(ping, State) of
        {value, #call{from = From}, State1} ->
            {keep_state, State1, [{reply, From, pong}]};
        false -> {keep_state, State}
    end;

connected(info, {timeout, _TRef, keepalive}, State = #state{force_ping = true}) ->
    case send(?PACKET(?PINGREQ), State) of
        {ok, NewState} ->
            {keep_state, ensure_keepalive_timer(NewState)};
        Error -> {stop, Error}
    end;

connected(info, {timeout, TRef, keepalive},
          State = #state{socket = Socket, keepalive_timer = TRef}) ->
    case should_ping(Socket) of
        true ->
            case send(?PACKET(?PINGREQ), State) of
                {ok, NewState} ->
                    {keep_state, ensure_keepalive_timer(NewState)};
                Error -> {stop, Error}
            end;
        false ->
            {keep_state, ensure_keepalive_timer(State)};
        {error, Reason} ->
            {stop, {error, Reason}}
    end;

connected(info, {timeout, TRef, ack}, State = #state{ack_timer = TRef,
                                                     ack_timeout = Timeout,
                                                     pending_calls = Calls}) ->
    NewState = State#state{ack_timer = undefined,
                           pending_calls = timeout_calls(Timeout, Calls)},
    {keep_state, ensure_ack_timer(NewState)};

connected(info, {timeout, TRef, retry}, State = #state{retry_timer = TRef,
                                                       inflight = Inflight}) ->
    case Inflight:is_empty() of
        true  -> {keep_state, State#state{retry_timer = undefined}};
        false -> retry_send(State)
    end;

connected(EventType, EventContent, StateData) ->
    handle_event(EventType, EventContent, StateData).

should_ping(Sock) ->
    case emqx_client_sock:getstat(Sock, [send_oct]) of
        {ok, [{send_oct, Val}]} ->
            OldVal = get(send_oct), put(send_oct, Val),
            OldVal == undefined orelse OldVal == Val;
        Err = {error, _Reason} ->
            Err
    end.

handle_event(info, {'EXIT', Owner, Reason}, #state{owner = Owner}) ->
    {stop, Reason};

handle_event(info, {'EXIT', Receiver, Reason}, #state{receiver = Receiver}) ->
    {stop, Reason};

handle_event(info, {inet_reply, _Sock, ok}, State) ->
    {keep_state, State};

handle_event(info, {inet_reply, _Sock, {error, Reason}}, State) ->
    {stop, Reason, State};

handle_event(EventType, EventContent, State) ->
    ?LOG(error, "Unexpected Event: (~p, ~p)", [EventType, EventContent]),
    {keep_state, State}.

%% Mandatory callback functions
terminate(_Reason, _State, #state{socket = undefined}) ->
    ok;
terminate(_Reason, _State, #state{socket   = Socket,
                                  receiver = Receiver}) ->
    emqx_client_sock:stop(Receiver),
    emqx_client_sock:close(Socket).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_keepalive_timer(State = #state{keepalive = 0}) ->
    State;
ensure_keepalive_timer(State = #state{keepalive = Keepalive}) ->
    TRef = erlang:start_timer(Keepalive, self(), keepalive),
    State#state{keepalive_timer = TRef}.

new_call(Id, From) ->
    new_call(Id, From, undefined).
new_call(Id, From, Req) ->
    #call{id = Id, from = From, req = Req, ts = os:timestamp()}.

add_call(Call, State = #state{pending_calls = Calls}) ->
    State#state{pending_calls = [Call | Calls]}.

take_call(Id, State = #state{pending_calls = Calls}) ->
    case lists:keytake(Id, #call.id, Calls) of
        {value, Call, Left} ->
            {value, Call, State#state{pending_calls = Left}};
        false -> false
    end.

timeout_calls(Timeout, Calls) ->
    timeout_calls(os:timestamp(), Timeout, Calls).
timeout_calls(Now, Timeout, Calls) ->
    lists:foldl(fun(C = #call{from = From, ts = Ts}, Acc) ->
                    case (timer:now_diff(Now, Ts) div 1000) >= Timeout of
                        true  -> gen_statem:reply(From, {error, ack_timeout}),
                                 Acc;
                        false -> [C | Acc]
                    end
                end, [], Calls).

ensure_ack_timer(State = #state{ack_timer = undefined,
                                ack_timeout = Timeout,
                                pending_calls = Calls}) when length(Calls) > 0 ->
    State#state{ack_timer = erlang:start_timer(Timeout, self(), ack)};
ensure_ack_timer(State) -> State.

ensure_retry_timer(State = #state{retry_interval = Interval}) ->
    ensure_retry_timer(Interval, State).
ensure_retry_timer(Interval, State = #state{retry_timer = undefined}) when Interval > 0 ->
    State#state{retry_timer = erlang:start_timer(Interval, self(), retry)};
ensure_retry_timer(_Interval, State) ->
    State.

retry_send(State = #state{inflight = Inflight}) ->
    SortFun = fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end,
    Msgs = lists:sort(SortFun, Inflight:values()),
    retry_send(Msgs, os:timestamp(), State).

retry_send([], _Now, State) ->
    {keep_state, ensure_retry_timer(State)};
retry_send([{Type, Msg, Ts} | Msgs], Now, State = #state{retry_interval = Interval}) ->
    Diff = timer:now_diff(Now, Ts) div 1000, %% micro -> ms
    case (Diff >= Interval) of
        true  -> case retry_send(Type, Msg, Now, State) of
                     {ok, NewState} -> retry_send(Msgs, Now, NewState);
                     {error, Error} -> {stop, Error}
                 end;
        false -> {keep_state, ensure_retry_timer(Interval - Diff, State)}
    end.

retry_send(publish, Msg = #mqtt_message{qos = QoS, packet_id = PacketId},
           Now, State = #state{inflight = Inflight}) ->
    Msg1 = Msg#mqtt_message{dup = (QoS =:= ?QOS1)},
    case send(Msg1, State) of
        {ok, NewState} ->
            Inflight1 = Inflight:update(PacketId, {publish, Msg1, Now}),
            {ok, NewState#state{inflight = Inflight1}};
        Error = {error, _Reason} ->
            Error
    end;
retry_send(pubrel, PacketId, Now, State = #state{inflight = Inflight}) ->
    case send(?PUBREL_PACKET(PacketId), State) of
        {ok, NewState} ->
            Inflight1 = Inflight:update(PacketId, {pubrel, PacketId, Now}),
            {ok, NewState#state{inflight = Inflight1}};
        Error = {error, _Reason} ->
            Error
    end.

deliver_msg(#mqtt_message{packet_id  = PacketId,
                          qos        = QoS,
                          retain     = Retain,
                          dup        = Dup,
                          topic      = Topic,
                          properties = Props,
                          payload    = Payload},
            State = #state{owner = Owner}) ->
    Metadata = #{mid => PacketId, qos => QoS, dup => Dup,
                 retain => Retain, properties => Props},
    Owner ! {publish, Topic, Metadata, Payload}, State.

packet_to_msg(?PUBLISH_PACKET(QoS, Topic, PacketId, Payload)) ->
    #mqtt_message{qos = QoS, packet_id = PacketId,
                  topic = Topic, payload = Payload}.

msg_to_packet(#mqtt_message{packet_id = PacketId,
                            qos       = Qos,
                            retain    = Retain,
                            dup       = Dup,
                            topic     = Topic,
                            payload   = Payload}) ->
    #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                qos    = Qos,
                                                retain = Retain,
                                                dup    = Dup},
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = PacketId},
                 payload  = Payload}.

send_puback(Packet, State) ->
    case send(Packet, State) of
        {ok, NewState}  -> {keep_state, NewState};
        {error, Reason} -> {stop, Reason}
    end.

send(Msg, State) when is_record(Msg, mqtt_message) ->
    send(msg_to_packet(Msg), State);

send(Packet, StateData = #state{socket = Socket})
    when is_record(Packet, mqtt_packet) ->
    Data = emqx_serializer:serialize(Packet),
    case emqx_client_sock:send(Socket, Data) of
        ok -> {ok, next_msg_id(StateData)};
        {error, Reason} -> {error, Reason}
    end.

next_msg_id(State = #state{last_packet_id = 16#ffff}) ->
    State#state{last_packet_id = 1};

next_msg_id(State = #state{last_packet_id = Id}) ->
    State#state{last_packet_id = Id + 1}.

