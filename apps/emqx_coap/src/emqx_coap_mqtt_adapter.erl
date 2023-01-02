%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------

-module(emqx_coap_mqtt_adapter).

-behaviour(gen_server).

-include("emqx_coap.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-logger_header("[CoAP-Adpter]").

%% API.
-export([ subscribe/2
        , unsubscribe/2
        , publish/3
        , received_puback/2
        , message_payload/1
        , message_topic/1
        ]).

-export([ client_pid/4
        , stop/1
        ]).

-export([ call/2
        , call/3
        ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {peername, clientid, username, password, sub_topics = [], connected_at}).

-define(ALIVE_INTERVAL, 20000).

-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(SUBOPTS, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_0, is_new => false}).

-define(PROTO_VER, 1).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

client_pid(undefined, _Username, _Password, _Channel) ->
    {error, bad_request};
client_pid(ClientId, Username, Password, Channel) ->
    % check authority
    case start(ClientId, Username, Password, Channel) of
        {ok, Pid1}                       -> {ok, Pid1};
        {error, {already_started, Pid2}} -> {ok, Pid2};
        {error, auth_failure}            -> {error, auth_failure};
        Other                            -> {error, Other}
    end.

start(ClientId, Username, Password, Channel) ->
    % DO NOT use start_link, since multiple coap_reponsder may have relation with one mqtt adapter,
    % one coap_responder crashes should not make mqtt adapter crash too
    % And coap_responder is not a system process
    % it is dangerous to link mqtt adapter to coap_responder
    gen_server:start({via, emqx_coap_registry, {ClientId, Username, Password}},
                     ?MODULE, {ClientId, Username, Password, Channel}, []).

stop(Pid) ->
    gen_server:stop(Pid).

subscribe(Pid, Topic) ->
    gen_server:call(Pid, {subscribe, Topic, self()}).

unsubscribe(Pid, Topic) ->
    gen_server:call(Pid, {unsubscribe, Topic, self()}).

publish(Pid, Topic, Payload) ->
    gen_server:call(Pid, {publish, Topic, Payload}).

received_puback(Pid, Msg) ->
    gen_server:cast(Pid, {received_puback, Msg}).

message_payload(#message{payload = Payload}) ->
    Payload.

message_topic(#message{topic = Topic}) ->
    Topic.

%% For emqx_management plugin
call(Pid, Msg) ->
    call(Pid, Msg, infinity).

call(Pid, Msg, _) ->
    Pid ! Msg, ok.

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init({ClientId, Username, Password, Channel}) ->
    ?LOG(debug, "try to start adapter ClientId=~p, Username=~p, Password=~p, "
                "Channel=~0p", [ClientId, Username, "******", Channel]),
    State0 = #state{peername = Channel,
                    clientid = ClientId,
                    username = Username,
                    password = Password},
    _ = run_hooks('client.connect', [conninfo(State0)], undefined),
    case emqx_access_control:authenticate(clientinfo(State0)) of
        {ok, _AuthResult} ->
            ok = emqx_cm:discard_session(ClientId),

            _ = run_hooks('client.connack', [conninfo(State0), success], undefined),

            State = State0#state{connected_at = erlang:system_time(millisecond)},

            run_hooks('client.connected', [clientinfo(State), conninfo(State)]),

            Self = self(),
            erlang:send_after(?ALIVE_INTERVAL, Self, check_alive),
            _ = emqx_cm_locker:trans(ClientId, fun(_) ->
                emqx_cm:register_channel(ClientId, Self, conninfo(State))
            end),
            emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
            {ok, State};
        {error, Reason} ->
            ?LOG(debug, "authentication faild: ~p", [Reason]),
            _ = run_hooks('client.connack', [conninfo(State0), not_authorized], undefined),
            {stop, {shutdown, Reason}}
    end.

handle_call({subscribe, Topic, CoapPid}, _From, State=#state{sub_topics = TopicList}) ->
    NewTopics = proplists:delete(Topic, TopicList),
    IsWild = emqx_topic:wildcard(Topic),
    {reply, chann_subscribe(Topic, State), State#state{sub_topics =
        [{Topic, {IsWild, CoapPid}} | NewTopics]}, hibernate};

handle_call({unsubscribe, Topic, _CoapPid}, _From, State=#state{sub_topics = TopicList}) ->
    NewTopics = proplists:delete(Topic, TopicList),
    chann_unsubscribe(Topic, State),
    {reply, ok, State#state{sub_topics = NewTopics}, hibernate};

handle_call({publish, Topic, Payload}, _From, State) ->
    {reply, chann_publish(Topic, Payload, State), State};

handle_call(info, _From, State) ->
    {reply, info(State), State};

handle_call(stats, _From, State) ->
    {reply, stats(State), State, hibernate};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call({set_rate_limit, _Rl}, _From, State) ->
    ?LOG(error, "set_rate_limit is not support", []),
    {reply, ok, State};

handle_call(get_rate_limit, _From, State) ->
    ?LOG(error, "get_rate_limit is not support", []),
    {reply, ok, State};

handle_call(Request, _From, State) ->
    ?LOG(error, "adapter unexpected call ~p", [Request]),
    {reply, ignored, State, hibernate}.

handle_cast({received_puback, Msg}, State) ->
    %% NOTE: the counter named 'messages.acked', but the hook named 'message.acked'!
    ok = emqx_metrics:inc('messages.acked'),
    _ = emqx_hooks:run('message.acked', [conninfo(State), Msg]),
    {noreply, State, hibernate};

handle_cast(Msg, State) ->
    ?LOG(error, "broker_api unexpected cast ~p", [Msg]),
    {noreply, State, hibernate}.

handle_info({deliver, _Topic, #message{} = Msg},
            State = #state{sub_topics = Subscribers}) ->
    deliver([Msg], Subscribers),
    {noreply, State, hibernate};

handle_info(check_alive, State = #state{sub_topics = []}) ->
    {stop, {shutdown, check_alive}, State};
handle_info(check_alive, State) ->
    erlang:send_after(?ALIVE_INTERVAL, self(), check_alive),
    {noreply, State, hibernate};

handle_info({shutdown, Error}, State) ->
    {stop, {shutdown, Error}, State};

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid]),
    {stop, {shutdown, conflict}, State};

handle_info(discard, State) ->
    ?LOG(warning, "the connection is discarded. " ++
                  "possibly there is another client with the same clientid", []),
    {stop, {shutdown, discarded}, State};

handle_info(kick, State) ->
    ?LOG(info, "Kicked", []),
    {stop, {shutdown, kick}, State};

handle_info(Info, State) ->
    ?LOG(error, "adapter unexpected info ~p", [Info]),
    {noreply, State, hibernate}.

terminate(Reason, State = #state{clientid = ClientId, sub_topics = SubTopics}) ->
    ?LOG(debug, "unsubscribe ~p while exiting for ~p", [SubTopics, Reason]),
    [chann_unsubscribe(Topic, State) || {Topic, _} <- SubTopics],
    emqx_cm:unregister_channel(ClientId),

    ConnInfo0 = conninfo(State),
    ConnInfo = ConnInfo0#{disconnected_at => erlang:system_time(millisecond)},
    run_hooks('client.disconnected', [clientinfo(State), Reason, ConnInfo]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Channel adapter functions

chann_subscribe(Topic, State = #state{clientid = ClientId}) ->
    ?LOG(debug, "subscribe Topic=~p", [Topic]),
    case emqx_access_control:check_acl(clientinfo(State), subscribe, Topic) of
        allow ->
            emqx_broker:subscribe(Topic, ClientId, ?SUBOPTS),
            emqx_hooks:run('session.subscribed', [clientinfo(State), Topic, ?SUBOPTS]),
            ok;
        deny  ->
            ?LOG(warning, "subscribe to ~p by clientid ~p failed due to acl check.",
                 [Topic, ClientId]),
            {error, forbidden}
    end.

chann_unsubscribe(Topic, State) ->
    ?LOG(debug, "unsubscribe Topic=~p", [Topic]),
    Opts = #{rh => 0, rap => 0, nl => 0, qos => 0},
    emqx_broker:unsubscribe(Topic),
    emqx_hooks:run('session.unsubscribed', [clientinfo(State), Topic, Opts]).

chann_publish(Topic, Payload, State = #state{clientid = ClientId}) ->
    ?LOG(debug, "publish Topic=~p, Payload=~p", [Topic, Payload]),
    case emqx_access_control:check_acl(clientinfo(State), publish, Topic) of
        allow ->
            _ = emqx_broker:publish(
                  packet_to_message(Topic, Payload, State)), ok;
        deny  ->
            ?LOG(warning, "publish to ~p by clientid ~p failed due to acl check.",
                 [Topic, ClientId]),
            {error, forbidden}
    end.

packet_to_message(Topic, Payload,
                  #state{clientid = ClientId,
                         username = Username,
                         peername = {PeerHost, _}}) ->
    Message = emqx_message:set_flag(
                retain, false,
                emqx_message:make(ClientId, ?QOS_0, Topic, Payload)
               ),
    emqx_message:set_headers(
      #{ proto_ver => ?PROTO_VER
       , protocol => coap
       , username => Username
       , peerhost => PeerHost}, Message).

%%--------------------------------------------------------------------
%% Deliver

deliver([], _) -> ok;
deliver([Msg | More], Subscribers) ->
    ok = do_deliver(Msg, Subscribers),
    deliver(More, Subscribers).

do_deliver(Msg, Subscribers) ->
    %% handle PUBLISH packet from broker
    ?LOG(debug, "deliver message from broker, msg: ~p", [Msg]),
    deliver_to_coap(Msg, Subscribers),
    ok.

deliver_to_coap(_Msg, []) ->
    ok;
deliver_to_coap(#message{topic = TopicName} = Msg, [{TopicFilter, {IsWild, CoapPid}} | T]) ->
    Matched =   case IsWild of
                    true  -> emqx_topic:match(TopicName, TopicFilter);
                    false -> TopicName =:= TopicFilter
                end,
    Matched andalso (CoapPid ! {dispatch, Msg}),
    deliver_to_coap(Msg, T).

%%--------------------------------------------------------------------
%% Helper funcs

-compile({inline, [run_hooks/2, run_hooks/3]}).
run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run_fold(Name, Args, Acc).

%%--------------------------------------------------------------------
%% Info & Stats

info(State) ->
    ChannInfo = chann_info(State),
    ChannInfo#{sockinfo => sockinfo(State)}.

%% copies from emqx_connection:info/1
sockinfo(#state{peername = Peername}) ->
    #{socktype => udp,
      peername => Peername,
      sockname => {{127, 0, 0, 1}, 5683},    %% FIXME: Sock?
      sockstate =>  running,
      active_n => 1
     }.

%% copies from emqx_channel:info/1
chann_info(State) ->
    #{conninfo => conninfo(State),
      conn_state => connected,
      clientinfo => clientinfo(State),
      session => maps:from_list(session_info(State)),
      will_msg => undefined
     }.

conninfo(#state{peername = {PeerHost, _} = Peername,
                clientid = ClientId,
                connected_at = ConnectedAt}) ->
    #{socktype => udp,
      sockname => {{127, 0, 0, 1}, 5683},
      peername => Peername,
      peerhost => PeerHost,
      peercert => nossl,        %% TODO: dtls
      conn_mod => ?MODULE,
      proto_name => <<"CoAP">>,
      proto_ver => ?PROTO_VER,
      clean_start => true,
      clientid => ClientId,
      username => undefined,
      conn_props => undefined,
      connected => true,
      connected_at => ConnectedAt,
      keepalive => 0,
      receive_maximum => 0,
      expiry_interval => 0
     }.

%% copies from emqx_session:info/1
session_info(#state{sub_topics = SubTopics, connected_at = ConnectedAt}) ->
    Subs = lists:foldl(
             fun({Topic, _}, Acc) ->
                Acc#{Topic => ?SUBOPTS}
             end, #{}, SubTopics),
    [{subscriptions, Subs},
     {upgrade_qos, false},
     {retry_interval, 0},
     {await_rel_timeout, 0},
     {created_at, ConnectedAt}
    ].

%% The stats keys copied from emqx_connection:stats/1
stats(#state{sub_topics = SubTopics}) ->
    SockStats = [{recv_oct, 0}, {recv_cnt, 0}, {send_oct, 0}, {send_cnt, 0}, {send_pend, 0}],
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = [{subscriptions_cnt, length(SubTopics)},
                 {subscriptions_max, length(SubTopics)},
                 {inflight_cnt, 0},
                 {inflight_max, 0},
                 {mqueue_len, 0},
                 {mqueue_max, 0},
                 {mqueue_dropped, 0},
                 {next_pkt_id, 0},
                 {awaiting_rel_cnt, 0},
                 {awaiting_rel_max, 0}
                ],
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

clientinfo(#state{peername = {PeerHost, _},
                  clientid = ClientId,
                  username = Username,
                  password = Password}) ->
    #{zone => undefined,
      protocol => coap,
      peerhost => PeerHost,
      sockport => 5683,      %% FIXME:
      clientid => ClientId,
      username => Username,
      password => Password,
      peercert => nossl,
      is_bridge => false,
      is_superuser => false,
      mountpoint => undefined,
      ws_cookie  => undefined
     }.
