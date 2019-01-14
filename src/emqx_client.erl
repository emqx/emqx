%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_client).

-behaviour(gen_statem).

-include("emqx_mqtt.hrl").
-include("emqx_client.hrl").

-export([start_link/0, start_link/1]).
-export([request/5, request/6, request_async/7, receive_response/3]).
-export([set_request_handler/2, sub_request_topic/3, sub_request_topic/4]).
-export([connect/1]).
-export([subscribe/2, subscribe/3, subscribe/4]).
-export([publish/2, publish/3, publish/4, publish/5]).
-export([unsubscribe/2, unsubscribe/3]).
-export([ping/1]).
-export([disconnect/1, disconnect/2, disconnect/3]).
-export([puback/2, puback/3, puback/4]).
-export([pubrec/2, pubrec/3, pubrec/4]).
-export([pubrel/2, pubrel/3, pubrel/4]).
-export([pubcomp/2, pubcomp/3, pubcomp/4]).
-export([subscriptions/1]).
-export([info/1, stop/1]).
%% For test cases
-export([pause/1, resume/1]).

-export([initialized/3, waiting_for_connack/3, connected/3]).
-export([init/1, callback_mode/0, handle_event/4, terminate/3, code_change/4]).

-export_type([client/0, properties/0, payload/0, pubopt/0, subopt/0,
              request_input/0, response_payload/0, request_handler/0,
              corr_data/0, mqtt_msg/0]).

-export_type([host/0, option/0]).

%% Default timeout
-define(DEFAULT_KEEPALIVE,       60000).
-define(DEFAULT_ACK_TIMEOUT,     30000).
-define(DEFAULT_CONNECT_TIMEOUT, 60000).

-define(PROPERTY(Name, Val), #state{properties = #{Name := Val}}).

-define(WILL_MSG(QoS, Retain, Topic, Props, Payload),
        #mqtt_msg{qos = QoS, retain = Retain, topic = Topic, props = Props, payload = Payload}).

-define(RESPONSE_TIMEOUT_SECONDS, timer:seconds(5)).

-define(NO_HANDLER, undefined).

-define(NO_GROUP, <<>>).

-define(NO_CLIENT_ID, <<>>).

-type(host() :: inet:ip_address() | inet:hostname()).

-type corr_data() :: binary().

-type(option() :: {name, atom()}
                | {owner, pid()}
                | {host, host()}
                | {hosts, [{host(), inet:port_number()}]}
                | {port, inet:port_number()}
                | {tcp_opts, [gen_tcp:option()]}
                | {ssl, boolean()}
                | {ssl_opts, [ssl:ssl_option()]}
                | {connect_timeout, pos_integer()}
                | {bridge_mode, boolean()}
                | {client_id, iodata()}
                | {clean_start, boolean()}
                | {username, iodata()}
                | {password, iodata()}
                | {proto_ver, v3 | v4 | v5}
                | {keepalive, non_neg_integer()}
                | {max_inflight, pos_integer()}
                | {retry_interval, timeout()}
                | {request_handler, request_handler()}
                | {will_topic, iodata()}
                | {will_payload, iodata()}
                | {will_retain, boolean()}
                | {will_qos, qos()}
                | {will_props, properties()}
                | {auto_ack, boolean()}
                | {ack_timeout, pos_integer()}
                | {force_ping, boolean()}
                | {properties, properties()}).

-type(mqtt_msg() :: #mqtt_msg{}).

-record(state, {name            :: atom(),
                owner           :: pid(),
                host            :: host(),
                port            :: inet:port_number(),
                hosts           :: [{host(), inet:port_number()}],
                socket          :: inet:socket(),
                sock_opts       :: [emqx_client_sock:option()],
                connect_timeout :: pos_integer(),
                bridge_mode     :: boolean(),
                client_id       :: binary(),
                clean_start     :: boolean(),
                username        :: binary() | undefined,
                password        :: binary() | undefined,
                proto_ver       :: emqx_mqtt_types:version(),
                proto_name      :: iodata(),
                keepalive       :: non_neg_integer(),
                keepalive_timer :: reference() | undefined,
                force_ping      :: boolean(),
                paused          :: boolean(),
                will_flag       :: boolean(),
                will_msg        :: mqtt_msg(),
                properties      :: properties(),
                pending_calls   :: list(),
                subscriptions   :: map(),
                max_inflight    :: infinity | pos_integer(),
                inflight        :: emqx_inflight:inflight(),
                awaiting_rel    :: map(),
                auto_ack        :: boolean(),
                ack_timeout     :: pos_integer(),
                ack_timer       :: reference(),
                retry_interval  :: pos_integer(),
                retry_timer     :: reference(),
                request_handler :: request_handler(),
                session_present :: boolean(),
                last_packet_id  :: packet_id(),
                parse_state     :: emqx_frame:state()}).

-record(call, {id, from, req, ts}).

-type(client() :: pid() | atom()).

-type(topic() :: emqx_topic:topic()).

-type(payload() :: iodata()).

-type(packet_id() :: emqx_mqtt_types:packet_id()).

-type(properties() :: emqx_mqtt_types:properties()).

-type(qos() :: emqx_mqtt_types:qos_name() | emqx_mqtt_types:qos()).

-type(pubopt() :: {retain, boolean()} | {qos, qos()} | {timeout, timeout()}).

-type(subopt() :: {rh, 0 | 1 | 2}
                | {rap, boolean()}
                | {nl,  boolean()}
                | {qos, qos()}).

-type(reason_code() :: emqx_mqtt_types:reason_code()).

-type(subscribe_ret() :: {ok, properties(), [reason_code()]} | {error, term()}).

-type(request_input() :: binary()).

-type(response_payload() :: binary()).

-type(request_handler() :: fun((request_input()) -> response_payload())).

-type(group() :: binary()).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Swap in a new request handler on the fly.
-spec(set_request_handler(client(), request_handler()) -> ok).
set_request_handler(Responser, RequestHandler) ->
    gen_statem:call(Responser, {set_request_handler, RequestHandler}).

%% @doc Subscribe to request topic.
-spec(sub_request_topic(client(), qos(), topic()) -> ok).
sub_request_topic(Client, QoS, Topic) ->
    sub_request_topic(Client, QoS, Topic, ?NO_GROUP).

%% @doc Share-subscribe to request topic.
-spec(sub_request_topic(client(), qos(), topic(), group()) -> ok).
sub_request_topic(Client, QoS, Topic, Group) ->
    Properties = get_properties(Client),
    NewTopic = make_req_rsp_topic(Properties, Topic, Group),
    subscribe_req_rsp_topic(Client, QoS, NewTopic).

-spec(start_link() -> gen_statem:start_ret()).
start_link() -> start_link([]).

-spec(start_link(map() | [option()]) -> gen_statem:start_ret()).
start_link(Options) when is_map(Options) ->
    start_link(maps:to_list(Options));
start_link(Options) when is_list(Options) ->
    ok = emqx_mqtt_props:validate(
            proplists:get_value(properties, Options, #{})),
    case proplists:get_value(name, Options) of
        undefined ->
            gen_statem:start_link(?MODULE, [with_owner(Options)], []);
        Name when is_atom(Name) ->
            gen_statem:start_link({local, Name}, ?MODULE, [with_owner(Options)], [])
    end.

with_owner(Options) ->
    case proplists:get_value(owner, Options) of
        Owner when is_pid(Owner) -> Options;
        undefined -> [{owner, self()} | Options]
    end.

-spec(connect(client()) -> {ok, properties()} | {error, term()}).
connect(Client) ->
    gen_statem:call(Client, connect, infinity).

-spec(subscribe(client(), topic() | {topic(), qos() | [subopt()]} | [{topic(), qos()}])
      -> subscribe_ret()).
subscribe(Client, Topic) when is_binary(Topic) ->
    subscribe(Client, {Topic, ?QOS_0});
subscribe(Client, {Topic, QoS}) when is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, {Topic, ?QOS_I(QoS)});
subscribe(Client, {Topic, QoS}) when is_binary(Topic), ?IS_QOS(QoS) ->
    subscribe(Client, [{Topic, ?QOS_I(QoS)}]);
subscribe(Client, Topics) when is_list(Topics) ->
    subscribe(Client, #{}, lists:map(
                             fun({Topic, QoS}) when is_binary(Topic), is_atom(QoS) ->
                                 {Topic, [{qos, ?QOS_I(QoS)}]};
                                ({Topic, QoS}) when is_binary(Topic), ?IS_QOS(QoS) ->
                                 {Topic, [{qos, ?QOS_I(QoS)}]};
                                ({Topic, Opts}) when is_binary(Topic), is_list(Opts) ->
                                 {Topic, Opts}
                             end, Topics)).

-spec(subscribe(client(), topic(), qos() | [subopt()]) ->
                subscribe_ret();
               (client(), properties(), [{topic(), qos() | [subopt()]}]) ->
                subscribe_ret()).
subscribe(Client, Topic, QoS) when is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, Topic, ?QOS_I(QoS));
subscribe(Client, Topic, QoS) when is_binary(Topic), ?IS_QOS(QoS) ->
    subscribe(Client, Topic, [{qos, QoS}]);
subscribe(Client, Topic, Opts) when is_binary(Topic), is_list(Opts) ->
    subscribe(Client, #{}, [{Topic, Opts}]);
subscribe(Client, Properties, Topics) when is_map(Properties), is_list(Topics) ->
    Topics1 = [{Topic, parse_subopt(Opts)} || {Topic, Opts} <- Topics],
    gen_statem:call(Client, {subscribe, Properties, Topics1}).

-spec(subscribe(client(), properties(), topic(), qos() | [subopt()])
      -> subscribe_ret()).
subscribe(Client, Properties, Topic, QoS)
    when is_map(Properties), is_binary(Topic), is_atom(QoS) ->
    subscribe(Client, Properties, Topic, ?QOS_I(QoS));
subscribe(Client, Properties, Topic, QoS)
    when is_map(Properties), is_binary(Topic), ?IS_QOS(QoS) ->
    subscribe(Client, Properties, Topic, [{qos, QoS}]);
subscribe(Client, Properties, Topic, Opts)
    when is_map(Properties), is_binary(Topic), is_list(Opts) ->
    subscribe(Client, Properties, [{Topic, Opts}]).

parse_subopt(Opts) ->
    parse_subopt(Opts, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_0}).

parse_subopt([], Result) ->
    Result;
parse_subopt([{rh, I} | Opts], Result) when I >= 0, I =< 2 ->
    parse_subopt(Opts, Result#{rh := I});
parse_subopt([{rap, true} | Opts], Result) ->
    parse_subopt(Opts, Result#{rap := 1});
parse_subopt([{rap, false} | Opts], Result) ->
    parse_subopt(Opts, Result#{rap := 0});
parse_subopt([{nl, true} | Opts], Result) ->
    parse_subopt(Opts, Result#{nl := 1});
parse_subopt([{nl, false} | Opts], Result) ->
    parse_subopt(Opts, Result#{nl := 0});
parse_subopt([{qos, QoS} | Opts], Result) ->
    parse_subopt(Opts, Result#{qos := ?QOS_I(QoS)}).

-spec(request(client(), topic(), topic(), payload(), qos() | [pubopt()])
        -> ok | {ok, packet_id()} | {error, term()}).
request(Client, ResponseTopic, RequestTopic, Payload, QoS) when is_binary(ResponseTopic), is_atom(QoS) ->
    request(Client, ResponseTopic, RequestTopic, Payload, [{qos, ?QOS_I(QoS)}]);
request(Client, ResponseTopic, RequestTopic, Payload, QoS) when is_binary(ResponseTopic), ?IS_QOS(QoS) ->
    request(Client, ResponseTopic, RequestTopic, Payload, [{qos, QoS}]);
request(Client, ResponseTopic, RequestTopic, Payload, Opts) when is_binary(ResponseTopic), is_list(Opts) ->
    request(Client, ResponseTopic, RequestTopic, Payload, Opts, _Properties = #{}).

%% @doc Send a request to request topic and wait for response.
-spec(request(client(), topic(), topic(), payload(), [pubopt()], properties())
        -> {ok, response_payload()} | {error, term()}).
request(Client, ResponseTopic, RequestTopic, Payload, Opts, Properties) ->
    CorrData = make_corr_data(),
    case request_async(Client, ResponseTopic, RequestTopic,
                       Payload, Opts, Properties, CorrData) of
        ok -> receive_response(Client, CorrData, Opts);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Get client properties.
-spec(get_properties(client()) -> properties()).
get_properties(Client) -> gen_statem:call(Client, get_properties, infinity).

%% @doc Send a request, but do not wait for response.
%% The caller should expect a `{publish, Response}' message,
%% or call `receive_response/3' to receive the message.
-spec(request_async(client(), topic(), topic(), payload(),
                    [pubopt()], properties(), corr_data()) -> ok | {error, any()}).
request_async(Client, ResponseTopic, RequestTopic, Payload, Opts, Properties, CorrData)
    when is_binary(ResponseTopic),
         is_binary(RequestTopic),
         is_map(Properties),
         is_list(Opts) ->
    ok = emqx_mqtt_props:validate(Properties),
    Retain = proplists:get_bool(retain, Opts),
    QoS = ?QOS_I(proplists:get_value(qos, Opts, ?QOS_0)),
    ClientProperties = get_properties(Client),
    NewResponseTopic = make_req_rsp_topic(ClientProperties, ResponseTopic),
    NewRequestTopic = make_req_rsp_topic(ClientProperties, RequestTopic),
    %% This is perhaps not optimal to subscribe the response topic for
    %% each and every request even though the response topic is always the same
    ok = sub_response_topic(Client, QoS, NewResponseTopic),
    NewProperties = maps:merge(Properties, #{'Response-Topic' => NewResponseTopic,
                                             'Correlation-Data' => CorrData}),
    case publish(Client, #mqtt_msg{qos  = QoS,
                                   retain = Retain,
                                   topic = NewRequestTopic,
                                   props = NewProperties,
                                   payload = iolist_to_binary(Payload)}) of
        ok -> ok;
        {ok, _PacketId} -> ok; %% assume auto_ack
        {error, Reason} -> {error, Reason}
    end.

%% @doc Block wait the response for a request sent earlier.
-spec(receive_response(client(), corr_data(), [pubopt()])
      -> {ok, response_payload()} | {error, any()}).
receive_response(Client, CorrData, Opts) ->
    TimeOut = proplists:get_value(timeout, Opts, ?RESPONSE_TIMEOUT_SECONDS),
    MRef = erlang:monitor(process, Client),
    TRef = erlang:start_timer(TimeOut, self(), response),
    try
        receive_response(Client, CorrData, TRef, MRef)
    after
        erlang:cancel_timer(TRef),
        receive {timeout, TRef, _} -> ok after 0 -> ok end,
        erlang:demonitor(MRef, [flush])
    end.

-spec(publish(client(), topic(), payload()) -> ok | {error, term()}).
publish(Client, Topic, Payload) when is_binary(Topic) ->
    publish(Client, #mqtt_msg{topic = Topic, qos = ?QOS_0, payload = iolist_to_binary(Payload)}).

-spec(publish(client(), topic(), payload(), qos() | [pubopt()])
        -> ok | {ok, packet_id()} | {error, term()}).
publish(Client, Topic, Payload, QoS) when is_binary(Topic), is_atom(QoS) ->
    publish(Client, Topic, Payload, [{qos, ?QOS_I(QoS)}]);
publish(Client, Topic, Payload, QoS) when is_binary(Topic), ?IS_QOS(QoS) ->
    publish(Client, Topic, Payload, [{qos, QoS}]);
publish(Client, Topic, Payload, Opts) when is_binary(Topic), is_list(Opts) ->
    publish(Client, Topic, #{}, Payload, Opts).

-spec(publish(client(), topic(), properties(), payload(), [pubopt()])
      -> ok | {ok, packet_id()} | {error, term()}).
publish(Client, Topic, Properties, Payload, Opts)
    when is_binary(Topic), is_map(Properties), is_list(Opts) ->
    ok = emqx_mqtt_props:validate(Properties),
    Retain = proplists:get_bool(retain, Opts),
    QoS = ?QOS_I(proplists:get_value(qos, Opts, ?QOS_0)),
    publish(Client, #mqtt_msg{qos     = QoS,
                              retain  = Retain,
                              topic   = Topic,
                              props   = Properties,
                              payload = iolist_to_binary(Payload)}).

-spec(publish(client(), #mqtt_msg{}) -> ok | {ok, packet_id()} | {error, term()}).
publish(Client, Msg) when is_record(Msg, mqtt_msg) ->
    gen_statem:call(Client, {publish, Msg}).

-spec(unsubscribe(client(), topic() | [topic()]) -> subscribe_ret()).
unsubscribe(Client, Topic) when is_binary(Topic) ->
    unsubscribe(Client, [Topic]);
unsubscribe(Client, Topics) when is_list(Topics) ->
    unsubscribe(Client, #{}, Topics).

-spec(unsubscribe(client(), properties(), topic() | [topic()]) -> subscribe_ret()).
unsubscribe(Client, Properties, Topic) when is_map(Properties), is_binary(Topic) ->
    unsubscribe(Client, Properties, [Topic]);
unsubscribe(Client, Properties, Topics) when is_map(Properties), is_list(Topics) ->
    gen_statem:call(Client, {unsubscribe, Properties, Topics}).

-spec(ping(client()) -> pong).
ping(Client) ->
    gen_statem:call(Client, ping).

-spec(disconnect(client()) -> ok).
disconnect(Client) ->
    disconnect(Client, ?RC_SUCCESS).

-spec(disconnect(client(), reason_code()) -> ok).
disconnect(Client, ReasonCode) ->
    disconnect(Client, ReasonCode, #{}).

-spec(disconnect(client(), reason_code(), properties()) -> ok).
disconnect(Client, ReasonCode, Properties) ->
    gen_statem:call(Client, {disconnect, ReasonCode, Properties}).

%%------------------------------------------------------------------------------
%% For test cases
%%------------------------------------------------------------------------------

puback(Client, PacketId) when is_integer(PacketId) ->
    puback(Client, PacketId, ?RC_SUCCESS).
puback(Client, PacketId, ReasonCode)
    when is_integer(PacketId), is_integer(ReasonCode) ->
    puback(Client, PacketId, ReasonCode, #{}).
puback(Client, PacketId, ReasonCode, Properties)
    when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(Client, {puback, PacketId, ReasonCode, Properties}).

pubrec(Client, PacketId) when is_integer(PacketId) ->
    pubrec(Client, PacketId, ?RC_SUCCESS).
pubrec(Client, PacketId, ReasonCode)
    when is_integer(PacketId), is_integer(ReasonCode) ->
    pubrec(Client, PacketId, ReasonCode, #{}).
pubrec(Client, PacketId, ReasonCode, Properties)
    when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(Client, {pubrec, PacketId, ReasonCode, Properties}).

pubrel(Client, PacketId) when is_integer(PacketId) ->
    pubrel(Client, PacketId, ?RC_SUCCESS).
pubrel(Client, PacketId, ReasonCode)
    when is_integer(PacketId), is_integer(ReasonCode) ->
    pubrel(Client, PacketId, ReasonCode, #{}).
pubrel(Client, PacketId, ReasonCode, Properties)
    when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(Client, {pubrel, PacketId, ReasonCode, Properties}).

pubcomp(Client, PacketId) when is_integer(PacketId) ->
    pubcomp(Client, PacketId, ?RC_SUCCESS).
pubcomp(Client, PacketId, ReasonCode)
    when is_integer(PacketId), is_integer(ReasonCode) ->
    pubcomp(Client, PacketId, ReasonCode, #{}).
pubcomp(Client, PacketId, ReasonCode, Properties)
    when is_integer(PacketId), is_integer(ReasonCode), is_map(Properties) ->
    gen_statem:cast(Client, {pubcomp, PacketId, ReasonCode, Properties}).

subscriptions(Client) ->
    gen_statem:call(Client, subscriptions).

info(Client) ->
    gen_statem:call(Client, info).

stop(Client) ->
    gen_statem:call(Client, stop).

pause(Client) ->
    gen_statem:call(Client, pause).

resume(Client) ->
    gen_statem:call(Client, resume).

%%------------------------------------------------------------------------------
%% gen_statem callbacks
%%------------------------------------------------------------------------------

init([Options]) ->
    process_flag(trap_exit, true),
    ClientId = case {proplists:get_value(proto_ver, Options, v4),
                     proplists:get_value(client_id, Options)} of
                   {v5, undefined}   -> ?NO_CLIENT_ID;
                   {_ver, undefined} -> random_client_id();
                   {_ver, Id}        -> iolist_to_binary(Id)
               end,
    State = init(Options, #state{host            = {127,0,0,1},
                                 port            = 1883,
                                 hosts           = [],
                                 sock_opts       = [],
                                 bridge_mode     = false,
                                 client_id       = ClientId,
                                 clean_start     = true,
                                 proto_ver       = ?MQTT_PROTO_V4,
                                 proto_name      = <<"MQTT">>,
                                 keepalive       = ?DEFAULT_KEEPALIVE,
                                 force_ping      = false,
                                 paused          = false,
                                 will_flag       = false,
                                 will_msg        = #mqtt_msg{},
                                 pending_calls   = [],
                                 subscriptions   = #{},
                                 max_inflight    = infinity,
                                 inflight        = emqx_inflight:new(0),
                                 awaiting_rel    = #{},
                                 properties      = #{},
                                 auto_ack        = true,
                                 ack_timeout     = ?DEFAULT_ACK_TIMEOUT,
                                 retry_interval  = 0,
                                 request_handler = ?NO_HANDLER,
                                 connect_timeout = ?DEFAULT_CONNECT_TIMEOUT,
                                 last_packet_id  = 1}),
    {ok, initialized, init_parse_state(State)}.

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
init([{ssl, EnableSsl} | Opts], State) ->
    case lists:keytake(ssl_opts, 1, Opts) of
        {value, SslOpts, WithOutSslOpts} ->
            init([SslOpts, {ssl, EnableSsl}| WithOutSslOpts], State);
        false ->
            init([{ssl_opts, []}, {ssl, EnableSsl}| Opts], State)
    end;
init([{ssl_opts, SslOpts} | Opts], State = #state{sock_opts = SockOpts}) ->
    case lists:keytake(ssl, 1, Opts) of
        {value, {ssl, true}, WithOutEnableSsl} ->
            ok = ssl:start(),
            SockOpts1 = emqx_misc:merge_opts(SockOpts, [{ssl_opts, SslOpts}]),
            init(WithOutEnableSsl, State#state{sock_opts = SockOpts1});
        {value, {ssl, false}, WithOutEnableSsl} ->
            init(WithOutEnableSsl, State);
        false ->
            init(Opts, State)
    end;
init([{client_id, ClientId} | Opts], State) ->
    init(Opts, State#state{client_id = iolist_to_binary(ClientId)});
init([{clean_start, CleanStart} | Opts], State) when is_boolean(CleanStart) ->
    init(Opts, State#state{clean_start = CleanStart});
init([{username, Username} | Opts], State) ->
    init(Opts, State#state{username = iolist_to_binary(Username)});
init([{password, Password} | Opts], State) ->
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
init([{will_props, Properties} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg = init_will_msg({props, Properties}, WillMsg)});
init([{will_payload, Payload} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg = init_will_msg({payload, Payload}, WillMsg)});
init([{will_retain, Retain} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg = init_will_msg({retain, Retain}, WillMsg)});
init([{will_qos, QoS} | Opts], State = #state{will_msg = WillMsg}) ->
    init(Opts, State#state{will_msg = init_will_msg({qos, QoS}, WillMsg)});
init([{connect_timeout, Timeout}| Opts], State) ->
    init(Opts, State#state{connect_timeout = timer:seconds(Timeout)});
init([{ack_timeout, Timeout}| Opts], State) ->
    init(Opts, State#state{ack_timeout = timer:seconds(Timeout)});
init([force_ping | Opts], State) ->
    init(Opts, State#state{force_ping = true});
init([{force_ping, ForcePing} | Opts], State) when is_boolean(ForcePing) ->
    init(Opts, State#state{force_ping = ForcePing});
init([{properties, Properties} | Opts], State = #state{properties = InitProps}) ->
    init(Opts, State#state{properties = maps:merge(InitProps, Properties)});
init([{max_inflight, infinity} | Opts], State) ->
    init(Opts, State#state{max_inflight = infinity,
                           inflight     = emqx_inflight:new(0)});
init([{max_inflight, I} | Opts], State) when is_integer(I) ->
    init(Opts, State#state{max_inflight = I,
                           inflight     = emqx_inflight:new(I)});
init([auto_ack | Opts], State) ->
    init(Opts, State#state{auto_ack = true});
init([{auto_ack, AutoAck} | Opts], State) when is_boolean(AutoAck) ->
    init(Opts, State#state{auto_ack = AutoAck});
init([{retry_interval, I} | Opts], State) ->
    init(Opts, State#state{retry_interval = timer:seconds(I)});
init([{request_handler, Handler} | Opts], State) ->
    init(Opts, State#state{request_handler = Handler});
init([{bridge_mode, Mode} | Opts], State) when is_boolean(Mode) ->
    init(Opts, State#state{bridge_mode = Mode});
init([_Opt | Opts], State) ->
    init(Opts, State).

init_will_msg({topic, Topic}, WillMsg) ->
    WillMsg#mqtt_msg{topic = iolist_to_binary(Topic)};
init_will_msg({props, Props}, WillMsg) ->
    WillMsg#mqtt_msg{props = Props};
init_will_msg({payload, Payload}, WillMsg) ->
    WillMsg#mqtt_msg{payload = iolist_to_binary(Payload)};
init_will_msg({retain, Retain}, WillMsg) when is_boolean(Retain) ->
    WillMsg#mqtt_msg{retain = Retain};
init_will_msg({qos, QoS}, WillMsg) ->
    WillMsg#mqtt_msg{qos = ?QOS_I(QoS)}.

init_parse_state(State = #state{proto_ver = Ver, properties = Properties}) ->
    Size = maps:get('Maximum-Packet-Size', Properties, ?MAX_PACKET_SIZE),
    State#state{parse_state = emqx_frame:initial_state(
                                #{max_packet_size => Size, version => Ver})}.

callback_mode() -> state_functions.

initialized({call, From}, connect, State = #state{sock_opts       = SockOpts,
                                                  connect_timeout = Timeout}) ->
    case sock_connect(hosts(State), SockOpts, Timeout) of
        {ok, Sock} ->
            case mqtt_connect(run_sock(State#state{socket = Sock})) of
                {ok, NewState} ->
                    {next_state, waiting_for_connack,
                     add_call(new_call(connect, From), NewState), [Timeout]};
                Error = {error, Reason} ->
                    {stop_and_reply, Reason, [{reply, From, Error}]}
            end;
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

initialized(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, initialized, State).

mqtt_connect(State = #state{client_id   = ClientId,
                            clean_start = CleanStart,
                            bridge_mode = IsBridge,
                            username    = Username,
                            password    = Password,
                            proto_ver   = ProtoVer,
                            proto_name  = ProtoName,
                            keepalive   = KeepAlive,
                            will_flag   = WillFlag,
                            will_msg    = WillMsg,
                            properties  = Properties}) ->
    ?WILL_MSG(WillQoS, WillRetain, WillTopic, WillProps, WillPayload) = WillMsg,
    ConnProps = emqx_mqtt_props:filter(?CONNECT, Properties),
    send(?CONNECT_PACKET(
            #mqtt_packet_connect{proto_ver    = ProtoVer,
                                 proto_name   = ProtoName,
                                 is_bridge    = IsBridge,
                                 clean_start  = CleanStart,
                                 will_flag    = WillFlag,
                                 will_qos     = WillQoS,
                                 will_retain  = WillRetain,
                                 keepalive    = KeepAlive,
                                 properties   = ConnProps,
                                 client_id    = ClientId,
                                 will_props   = WillProps,
                                 will_topic   = WillTopic,
                                 will_payload = WillPayload,
                                 username     = Username,
                                 password     = Password}), State).

waiting_for_connack(cast, ?CONNACK_PACKET(?RC_SUCCESS,
                                          SessPresent,
                                          Properties),
                    State = #state{properties = AllProps,
                                   client_id = ClientId}) ->
    case take_call(connect, State) of
        {value, #call{from = From}, State1} ->
            AllProps1 = case Properties of
                            undefined -> AllProps;
                            _ -> maps:merge(AllProps, Properties)
                        end,
            Reply = {ok, Properties},
            State2 = State1#state{client_id = assign_id(ClientId, AllProps1),
                                  properties = AllProps1,
                                  session_present = SessPresent},
            {next_state, connected, ensure_keepalive_timer(State2),
             [{reply, From, Reply}]};
        false ->
            {stop, bad_connack}
    end;

waiting_for_connack(cast, ?CONNACK_PACKET(ReasonCode,
                                          _SessPresent,
                                          Properties),
                    State = #state{proto_ver = ProtoVer}) ->
    Reason = emqx_reason_codes:name(ReasonCode, ProtoVer),
    case take_call(connect, State) of
        {value, #call{from = From}, _State} ->
            Reply = {error, {Reason, Properties}},
            {stop_and_reply, {shutdown, Reason}, [{reply, From, Reply}]};
        false -> {stop, connack_error}
    end;

waiting_for_connack(timeout, _Timeout, State) ->
    case take_call(connect, State) of
        {value, #call{from = From}, _State} ->
            Reply = {error, connack_timeout},
            {stop_and_reply, connack_timeout, [{reply, From, Reply}]};
        false -> {stop, connack_timeout}
    end;

waiting_for_connack(EventType, EventContent, State) ->
    case take_call(connect, State) of
        {value, #call{from = From}, _State} ->
            case handle_event(EventType, EventContent, waiting_for_connack, State) of
                {stop, Reason, State} ->
                    Reply = {error, {Reason, EventContent}},
                    {stop_and_reply, Reason, [{reply, From, Reply}]};
                StateCallbackResult ->
                    StateCallbackResult
            end;
        false -> {stop, connack_timeout}
    end.

connected({call, From}, subscriptions, State = #state{subscriptions = Subscriptions}) ->
    {keep_state, State, [{reply, From, maps:to_list(Subscriptions)}]};

connected({call, From}, info, State) ->
    Info = lists:zip(record_info(fields, state), tl(tuple_to_list(State))),
    {keep_state, State, [{reply, From, Info}]};

connected({call, From}, pause, State) ->
    {keep_state, State#state{paused = true}, [{reply, From, ok}]};

connected({call, From}, resume, State) ->
    {keep_state, State#state{paused = false}, [{reply, From, ok}]};

connected({call, From}, stop, _State) ->
    {stop_and_reply, normal, [{reply, From, ok}]};

connected({call, From}, get_properties, State = #state{properties = Properties}) ->
    {keep_state, State, [{reply, From, Properties}]};

connected({call, From}, client_id, State = #state{client_id = ClientId}) ->
    {keep_state, State, [{reply, From, ClientId}]};

connected({call, From}, {set_request_handler, RequestHandler}, State) ->
    {keep_state, State#state{request_handler = RequestHandler}, [{reply, From, ok}]};

connected({call, From}, SubReq = {subscribe, Properties, Topics},
          State = #state{last_packet_id = PacketId, subscriptions = Subscriptions}) ->
    case send(?SUBSCRIBE_PACKET(PacketId, Properties, Topics), State) of
        {ok, NewState} ->
            Call = new_call({subscribe, PacketId}, From, SubReq),
            Subscriptions1 =
                lists:foldl(fun({Topic, Opts}, Acc) ->
                                maps:put(Topic, Opts, Acc)
                            end, Subscriptions, Topics),
            {keep_state, ensure_ack_timer(add_call(Call,NewState#state{subscriptions = Subscriptions1}))};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, {publish, Msg = #mqtt_msg{qos = ?QOS_0}}, State) ->
    case send(Msg, State) of
        {ok, NewState} ->
            {keep_state, NewState, [{reply, From, ok}]};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected({call, From}, {publish, Msg = #mqtt_msg{qos = QoS}},
          State = #state{inflight = Inflight, last_packet_id = PacketId})
    when (QoS =:= ?QOS_1); (QoS =:= ?QOS_2) ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            {keep_state, State, [{reply, From, {error, {PacketId, inflight_full}}}]};
        false ->
            Msg1 = Msg#mqtt_msg{packet_id = PacketId},
            case send(Msg1, State) of
                {ok, NewState} ->
                    Inflight1 = emqx_inflight:insert(PacketId, {publish, Msg1, os:timestamp()}, Inflight),
                    {keep_state, ensure_retry_timer(NewState#state{inflight = Inflight1}),
                     [{reply, From, {ok, PacketId}}]};
                {error, Reason} ->
                    {stop_and_reply, Reason, [{reply, From, {error, {PacketId, Reason}}}]}
            end
    end;

connected({call, From}, UnsubReq = {unsubscribe, Properties, Topics},
          State = #state{last_packet_id = PacketId}) ->
    case send(?UNSUBSCRIBE_PACKET(PacketId, Properties, Topics), State) of
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

connected({call, From}, {disconnect, ReasonCode, Properties}, State) ->
    case send(?DISCONNECT_PACKET(ReasonCode, Properties), State) of
        {ok, NewState} ->
            {stop_and_reply, normal, [{reply, From, ok}], NewState};
        Error = {error, Reason} ->
            {stop_and_reply, Reason, [{reply, From, Error}]}
    end;

connected(cast, {puback, PacketId, ReasonCode, Properties}, State) ->
    send_puback(?PUBACK_PACKET(PacketId, ReasonCode, Properties), State);

connected(cast, {pubrec, PacketId, ReasonCode, Properties}, State) ->
    send_puback(?PUBREC_PACKET(PacketId, ReasonCode, Properties), State);

connected(cast, {pubrel, PacketId, ReasonCode, Properties}, State) ->
    send_puback(?PUBREL_PACKET(PacketId, ReasonCode, Properties), State);

connected(cast, {pubcomp, PacketId, ReasonCode, Properties}, State) ->
    send_puback(?PUBCOMP_PACKET(PacketId, ReasonCode, Properties), State);

connected(cast, ?PUBLISH_PACKET(_QoS, _PacketId), State = #state{paused = true}) ->
    {keep_state, State};

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_0, _Topic, _PacketId, Properties, Payload),
          State) when Properties =/= undefined ->
    NewState = response_publish(Properties, State, ?QOS_0, Payload),
    {keep_state, deliver(packet_to_msg(Packet), NewState)};
connected(cast, Packet = ?PUBLISH_PACKET(?QOS_0, _PacketId), State) ->
     {keep_state, deliver(packet_to_msg(Packet), State)};

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_1, _Topic, _PacketId, Properties, Payload), State)
    when Properties =/= undefined ->
    NewState = response_publish(Properties, State, ?QOS_1, Payload),
    publish_process(?QOS_1, Packet, NewState);

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_1, _PacketId), State) ->
    publish_process(?QOS_1, Packet, State);

connected(cast, Packet = ?PUBLISH_PACKET(?QOS_2, _Topic, _PacketId, Properties, Payload), State)
    when Properties =/= undefined ->
    NewState = response_publish(Properties, State, ?QOS_2, Payload),
    publish_process(?QOS_2, Packet, NewState);
connected(cast, Packet = ?PUBLISH_PACKET(?QOS_2, _PacketId), State) ->
    publish_process(?QOS_2, Packet, State);

connected(cast, ?PUBACK_PACKET(PacketId, ReasonCode, Properties),
          State = #state{owner = Owner, inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, #mqtt_msg{packet_id = PacketId}, _Ts}} ->
            Owner ! {puback, #{packet_id   => PacketId,
                               reason_code => ReasonCode,
                               properties  => Properties}},
            {keep_state, State#state{inflight = emqx_inflight:delete(PacketId, Inflight)}};
        none ->
            emqx_logger:warning("Unexpected PUBACK: ~p", [PacketId]),
            {keep_state, State}
    end;

connected(cast, ?PUBREC_PACKET(PacketId), State = #state{inflight = Inflight}) ->
    send_puback(?PUBREL_PACKET(PacketId),
                case emqx_inflight:lookup(PacketId, Inflight) of
                    {value, {publish, _Msg, _Ts}} ->
                        Inflight1 = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight),
                        State#state{inflight = Inflight1};
                    {value, {pubrel, _Ref, _Ts}} ->
                        emqx_logger:warning("Duplicated PUBREC Packet: ~p", [PacketId]),
                        State;
                    none ->
                        emqx_logger:warning("Unexpected PUBREC Packet: ~p", [PacketId]),
                        State
                end);

%%TODO::... if auto_ack is false, should we take PacketId from the map?
connected(cast, ?PUBREL_PACKET(PacketId),
          State = #state{awaiting_rel = AwaitingRel, auto_ack = AutoAck}) ->
     case maps:take(PacketId, AwaitingRel) of
         {Packet, AwaitingRel1} ->
             NewState = deliver(packet_to_msg(Packet), State#state{awaiting_rel = AwaitingRel1}),
             case AutoAck of
                 true  -> send_puback(?PUBCOMP_PACKET(PacketId), NewState);
                 false -> {keep_state, NewState}
             end;
         error ->
             emqx_logger:warning("Unexpected PUBREL: ~p", [PacketId]),
             {keep_state, State}
     end;

connected(cast, ?PUBCOMP_PACKET(PacketId, ReasonCode, Properties),
          State = #state{owner = Owner, inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {pubrel, _PacketId, _Ts}} ->
            Owner ! {puback, #{packet_id   => PacketId,
                               reason_code => ReasonCode,
                               properties  => Properties}},
            {keep_state, State#state{inflight = emqx_inflight:delete(PacketId, Inflight)}};
        none ->
            emqx_logger:warning("Unexpected PUBCOMP Packet: ~p", [PacketId]),
            {keep_state, State}
     end;

connected(cast, ?SUBACK_PACKET(PacketId, Properties, ReasonCodes),
          State = #state{subscriptions = _Subscriptions}) ->
    case take_call({subscribe, PacketId}, State) of
        {value, #call{from = From}, NewState} ->
            %%TODO: Merge reason codes to subscriptions?
            Reply = {ok, Properties, ReasonCodes},
            {keep_state, NewState, [{reply, From, Reply}]};
        false -> {keep_state, State}
    end;

connected(cast, ?UNSUBACK_PACKET(PacketId, Properties, ReasonCodes),
          State = #state{subscriptions = Subscriptions}) ->
    case take_call({unsubscribe, PacketId}, State) of
        {value, #call{from = From, req = {_, _, Topics}}, NewState} ->
            Subscriptions1 =
              lists:foldl(fun(Topic, Acc) ->
                              maps:remove(Topic, Acc)
                          end, Subscriptions, Topics),
            {keep_state, NewState#state{subscriptions = Subscriptions1},
             [{reply, From, {ok, Properties, ReasonCodes}}]};
        false -> {keep_state, State}
    end;

connected(cast, ?PACKET(?PINGRESP), State = #state{pending_calls = []}) ->
    {keep_state, State};
connected(cast, ?PACKET(?PINGRESP), State) ->
    case take_call(ping, State) of
        {value, #call{from = From}, NewState} ->
            {keep_state, NewState, [{reply, From, pong}]};
        false -> {keep_state, State}
    end;

connected(cast, ?DISCONNECT_PACKET(ReasonCode, Properties),
          State = #state{owner = Owner}) ->
    Owner ! {disconnected, ReasonCode, Properties},
    {stop, disconnected, State};

connected(info, {timeout, _TRef, keepalive}, State = #state{force_ping = true}) ->
    case send(?PACKET(?PINGREQ), State) of
        {ok, NewState} ->
            {keep_state, ensure_keepalive_timer(NewState)};
        Error -> {stop, Error}
    end;

connected(info, {timeout, TRef, keepalive},
          State = #state{socket = Sock, paused = Paused, keepalive_timer = TRef}) ->
    case (not Paused) andalso should_ping(Sock) of
        true ->
            case send(?PACKET(?PINGREQ), State) of
                {ok, NewState} ->
                    {keep_state, ensure_keepalive_timer(NewState), [hibernate]};
                Error -> {stop, Error}
            end;
        false ->
            {keep_state, ensure_keepalive_timer(State), [hibernate]};
        {error, Reason} ->
            {stop, Reason}
    end;

connected(info, {timeout, TRef, ack}, State = #state{ack_timer     = TRef,
                                                     ack_timeout   = Timeout,
                                                     pending_calls = Calls}) ->
    NewState = State#state{ack_timer = undefined,
                           pending_calls = timeout_calls(Timeout, Calls)},
    {keep_state, ensure_ack_timer(NewState)};

connected(info, {timeout, TRef, retry}, State = #state{retry_timer = TRef,
                                                       inflight    = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  -> {keep_state, State#state{retry_timer = undefined}};
        false -> retry_send(State)
    end;

connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, connected, Data).

should_ping(Sock) ->
    case emqx_client_sock:getstat(Sock, [send_oct]) of
        {ok, [{send_oct, Val}]} ->
            OldVal = get(send_oct), put(send_oct, Val),
            OldVal == undefined orelse OldVal == Val;
        Error = {error, _Reason} ->
            Error
    end.

handle_event(info, {TcpOrSsL, _Sock, Data}, _StateName, State)
    when TcpOrSsL =:= tcp; TcpOrSsL =:= ssl ->
    emqx_logger:debug("RECV Data: ~p", [Data]),
    process_incoming(Data, [], run_sock(State));

handle_event(info, {Error, _Sock, Reason}, _StateName, State)
    when Error =:= tcp_error; Error =:= ssl_error ->
    emqx_logger:error("[~p] ~p, Reason: ~p", [?MODULE, Error, Reason]),
    {stop, {shutdown, Reason}, State};

handle_event(info, {Closed, _Sock}, _StateName, State)
    when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    emqx_logger:debug("[~p] ~p", [?MODULE, Closed]),
    {stop, {shutdown, Closed}, State};

handle_event(info, {'EXIT', Owner, Reason}, _, State = #state{owner = Owner}) ->
    emqx_logger:debug("[~p] Got EXIT from owner, Reason: ~p", [?MODULE, Reason]),
    {stop, {shutdown, Reason}, State};

handle_event(info, {inet_reply, _Sock, ok}, _, State) ->
    {keep_state, State};

handle_event(info, {inet_reply, _Sock, {error, Reason}}, _, State) ->
    emqx_logger:error("[~p] got tcp error: ~p", [?MODULE, Reason]),
    {stop, {shutdown, Reason}, State};

handle_event(EventType, EventContent, StateName, StateData) ->
    emqx_logger:error("State: ~s, Unexpected Event: (~p, ~p)",
                      [StateName, EventType, EventContent]),
    {keep_state, StateData}.

%% Mandatory callback functions
terminate(_Reason, _State, #state{socket = undefined}) ->
    ok;
terminate(_Reason, _State, #state{socket = Socket}) ->
    emqx_client_sock:close(Socket).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Subscribe to response topic.
-spec(sub_response_topic(client(), qos(), topic()) -> ok).
sub_response_topic(Client, QoS, Topic) when is_binary(Topic) ->
    subscribe_req_rsp_topic(Client, QoS, Topic).

receive_response(Client, CorrData, TRef, MRef) ->
    receive
        {publish, Response} ->
            {ok, Properties} = maps:find(properties, Response),
            case maps:find('Correlation-Data', Properties) of
                {ok, CorrData} ->
                    maps:find(payload, Response);
                _ ->
                    emqx_logger:debug("Discarded stale response: ~p", [Response]),
                    receive_response(Client, CorrData, TRef, MRef)
            end;
        {timeout, TRef, response} ->
            {error, timeout};
        {'DOWN', MRef, process, _, _} ->
            {error, client_down}
    end.

%% Make a unique correlation data for each request.
%% It has to be unique because stale responses should be discarded.
make_corr_data() -> term_to_binary(make_ref()).

%% Shared function for request and response topic subscription.
subscribe_req_rsp_topic(Client, QoS, Topic) ->
    %% It is a Protocol Error to set the No Local bit to 1 on a Shared Subscription
    {ok, _Props, _QoS} = subscribe(Client, [{Topic, [{rh, 2}, {rap, false},
                                                     {nl, not ?IS_SHARE(Topic)},
                                                     {qos, QoS}]}]),
    emqx_logger:debug("Subscribed to topic ~s", [Topic]),
    ok.

%% Make a request or response topic.
make_req_rsp_topic(Properties, Topic) ->
    make_req_rsp_topic(Properties, Topic, ?NO_GROUP).

%% Same as make_req_rsp_topic/2, but allow shared subscription (for request topics)
make_req_rsp_topic(Properties, Topic, Group) ->
    case maps:find('Response-Information', Properties) of
        {ok, ResponseInformation} when ResponseInformation =/= <<>> ->
            emqx_topic:join([req_rsp_topic_prefix(Group, ResponseInformation), Topic]);
        _ ->
            erlang:error(no_response_information)
    end.

req_rsp_topic_prefix(?NO_GROUP, Prefix) -> Prefix;
req_rsp_topic_prefix(Group, Prefix) -> ?SHARE(Group, Prefix).

assign_id(?NO_CLIENT_ID, Props) ->
    case maps:find('Assigned-Client-Identifier', Props) of
        {ok, Value} ->
            Value;
        _ ->
            error(bad_client_id)
    end;
assign_id(Id, _Props) ->
    Id.

publish_process(?QOS_1, Packet = ?PUBLISH_PACKET(?QOS_1, PacketId), State = #state{auto_ack = AutoAck}) ->
    _ = deliver(packet_to_msg(Packet), State),
    case AutoAck of
        true  -> send_puback(?PUBACK_PACKET(PacketId), State);
        false -> {keep_state, State}
    end;
publish_process(?QOS_2, Packet = ?PUBLISH_PACKET(?QOS_2, PacketId),
    State = #state{awaiting_rel = AwaitingRel}) ->
    case send_puback(?PUBREC_PACKET(PacketId), State) of
        {keep_state, NewState} ->
            AwaitingRel1 = maps:put(PacketId, Packet, AwaitingRel),
            {keep_state, NewState#state{awaiting_rel = AwaitingRel1}};
        Stop -> Stop
    end.

response_publish(undefined, State, _QoS, _Payload) ->
    State;
response_publish(Properties, State = #state{request_handler = RequestHandler}, QoS, Payload) ->
    case maps:find('Response-Topic', Properties) of
        {ok, ResponseTopic} ->
            case RequestHandler of
                ?NO_HANDLER -> State;
                _ -> do_publish(ResponseTopic, Properties, State, QoS, Payload)
            end;
        _ ->
            State
    end.

do_publish(ResponseTopic, Properties, State = #state{request_handler = RequestHandler}, ?QOS_0, Payload) ->
        Msg = #mqtt_msg{qos     = ?QOS_0,
                        retain  = false,
                        topic   = ResponseTopic,
                        props   = Properties,
                        payload = RequestHandler(Payload)
                       },
    case send(Msg, State) of
        {ok, NewState} -> NewState;
        _Error -> State
    end;
do_publish(ResponseTopic, Properties, State = #state{request_handler = RequestHandler,
                                                     inflight = Inflight,
                                                     last_packet_id = PacketId},
           QoS, Payload)
    when (QoS =:= ?QOS_1); (QoS =:= ?QOS_2)->
    case emqx_inflight:is_full(Inflight) of
        true ->
            emqx_logger:error("Inflight is full"),
            State;
        false ->
            Msg = #mqtt_msg{packet_id = PacketId,
                            qos       = QoS,
                            retain    = false,
                            topic     = ResponseTopic,
                            props     = Properties,
                            payload   = RequestHandler(Payload)},
            case send(Msg, State) of
                {ok, NewState} ->
                    Inflight1 = emqx_inflight:insert(PacketId, {publish, Msg, os:timestamp()}, Inflight),
                    ensure_retry_timer(NewState#state{inflight = Inflight1});
                {error, Reason} ->
                    emqx_logger:error("Send failed: ~p", [Reason]),
                    State
            end
    end.

ensure_keepalive_timer(State = ?PROPERTY('Server-Keep-Alive', Secs)) ->
    ensure_keepalive_timer(timer:seconds(Secs), State);
ensure_keepalive_timer(State = #state{keepalive = 0}) ->
    State;
ensure_keepalive_timer(State = #state{keepalive = I}) ->
    ensure_keepalive_timer(I, State).
ensure_keepalive_timer(I, State) when is_integer(I) ->
    State#state{keepalive_timer = erlang:start_timer(I, self(), keepalive)}.

new_call(Id, From) ->
    new_call(Id, From, undefined).
new_call(Id, From, Req) ->
    #call{id = Id, from = From, req = Req, ts = os:timestamp()}.

add_call(Call, Data = #state{pending_calls = Calls}) ->
    Data#state{pending_calls = [Call | Calls]}.

take_call(Id, Data = #state{pending_calls = Calls}) ->
    case lists:keytake(Id, #call.id, Calls) of
        {value, Call, Left} ->
            {value, Call, Data#state{pending_calls = Left}};
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

ensure_ack_timer(State = #state{ack_timer     = undefined,
                                ack_timeout   = Timeout,
                                pending_calls = Calls}) when length(Calls) > 0 ->
    State#state{ack_timer = erlang:start_timer(Timeout, self(), ack)};
ensure_ack_timer(State) -> State.

ensure_retry_timer(State = #state{retry_interval = Interval}) ->
    ensure_retry_timer(Interval, State).
ensure_retry_timer(Interval, State = #state{retry_timer = undefined})
    when Interval > 0 ->
    State#state{retry_timer = erlang:start_timer(Interval, self(), retry)};
ensure_retry_timer(_Interval, State) ->
    State.

retry_send(State = #state{inflight = Inflight}) ->
    SortFun = fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end,
    Msgs = lists:sort(SortFun, emqx_inflight:values(Inflight)),
    retry_send(Msgs, os:timestamp(), State ).

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

retry_send(publish, Msg = #mqtt_msg{qos = QoS, packet_id = PacketId},
           Now, State = #state{inflight = Inflight}) ->
    Msg1 = Msg#mqtt_msg{dup = (QoS =:= ?QOS_1)},
    case send(Msg1, State) of
        {ok, NewState} ->
            Inflight1 = emqx_inflight:update(PacketId, {publish, Msg1, Now}, Inflight),
            {ok, NewState#state{inflight = Inflight1}};
        Error = {error, _Reason} ->
            Error
    end;
retry_send(pubrel, PacketId, Now, State = #state{inflight = Inflight}) ->
    case send(?PUBREL_PACKET(PacketId), State) of
        {ok, NewState} ->
            Inflight1 = emqx_inflight:update(PacketId, {pubrel, PacketId, Now}, Inflight),
            {ok, NewState#state{inflight = Inflight1}};
        Error = {error, _Reason} ->
            Error
    end.

deliver(#mqtt_msg{qos = QoS, dup = Dup, retain = Retain, packet_id = PacketId,
                  topic = Topic, props = Props, payload = Payload},
        State = #state{owner = Owner, request_handler = RequestHandler}) ->
    case RequestHandler of
        ?NO_HANDLER ->
            Owner ! {publish, #{qos => QoS, dup => Dup, retain => Retain, packet_id => PacketId,
                                topic => Topic, properties => Props, payload => Payload,
                                client_pid => self()}};
        _ ->
            ok
    end,
    State.

packet_to_msg(#mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                          dup    = Dup,
                                                          qos    = QoS,
                                                          retain = R},
                           variable = #mqtt_packet_publish{topic_name = Topic,
                                                           packet_id  = PacketId,
                                                           properties = Props},
                           payload  = Payload}) ->
    #mqtt_msg{qos = QoS, retain = R, dup = Dup, packet_id = PacketId,
               topic = Topic, props = Props, payload = Payload}.

msg_to_packet(#mqtt_msg{qos = QoS, dup = Dup, retain = Retain, packet_id = PacketId,
                       topic = Topic, props = Props, payload = Payload}) ->
    #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                qos    = QoS,
                                                retain = Retain,
                                                dup    = Dup},
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = PacketId,
                                                 properties = Props},
                 payload  = Payload}.

%%------------------------------------------------------------------------------
%% Socket Connect/Send

sock_connect(Hosts, SockOpts, Timeout) ->
    sock_connect(Hosts, SockOpts, Timeout, {error, no_hosts}).

sock_connect([], _SockOpts, _Timeout, LastErr) ->
    LastErr;
sock_connect([{Host, Port} | Hosts], SockOpts, Timeout, _LastErr) ->
    case emqx_client_sock:connect(Host, Port, SockOpts, Timeout) of
        {ok, Socket} -> {ok, Socket};
        Err = {error, _Reason} ->
            sock_connect(Hosts, SockOpts, Timeout, Err)
    end.

hosts(#state{hosts = [], host = Host, port = Port}) ->
    [{Host, Port}];
hosts(#state{hosts = Hosts}) -> Hosts.

send_puback(Packet, State) ->
    case send(Packet, State) of
        {ok, NewState}  -> {keep_state, NewState};
        {error, Reason} -> {stop, {shutdown, Reason}}
    end.

send(Msg, State) when is_record(Msg, mqtt_msg) ->
    send(msg_to_packet(Msg), State);

send(Packet, State = #state{socket = Sock, proto_ver = Ver})
    when is_record(Packet, mqtt_packet) ->
    Data = emqx_frame:serialize(Packet, #{version => Ver}),
    emqx_logger:debug("SEND Data: ~p", [Data]),
    case emqx_client_sock:send(Sock, Data) of
        ok  -> {ok, next_packet_id(State)};
        Error -> Error
    end.

run_sock(State = #state{socket = Sock}) ->
    emqx_client_sock:setopts(Sock, [{active, once}]), State.

%%------------------------------------------------------------------------------
%% Process incomming

process_incoming(<<>>, Packets, State) ->
    {keep_state, State, next_events(Packets)};

process_incoming(Bytes, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Bytes, ParseState) of
        {ok, Packet, Rest} ->
            process_incoming(Rest, [Packet|Packets], init_parse_state(State));
        {more, NewParseState} ->
            {keep_state, State#state{parse_state = NewParseState}, next_events(Packets)};
        {error, Reason} ->
            {stop, Reason}
    catch
        error:Error ->
            {stop, Error}
    end.

next_events([]) ->
    [];
next_events([Packet]) ->
    {next_event, cast, Packet};
next_events(Packets) ->
    [{next_event, cast, Packet} || Packet <- lists:reverse(Packets)].

%%------------------------------------------------------------------------------
%% Next packet id

next_packet_id(State = #state{last_packet_id = 16#ffff}) ->
    State#state{last_packet_id = 1};

next_packet_id(State = #state{last_packet_id = Id}) ->
    State#state{last_packet_id = Id + 1}.
