%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_connector).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/2]).

-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2]).

-define(RECONNECT_DELAY_MS, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(map(), non_neg_integer()) -> emqx_types:startlink_ret().
start_link(BridgeConfig, Index) ->
    gen_server:start_link(?MODULE, {BridgeConfig, Index}, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init({BridgeConfig, Index}) ->
    #{
        name := BridgeName,
        server := Server,
        proto_ver := ProtoVer,
        clientid_prefix := ClientidPrefix,
        username := Username,
        password := Password,
        clean_start := CleanStart,
        keepalive_s := Keepalive,
        ssl := SslConf
    } = BridgeConfig,
    {Host, Port} = parse_server(Server),
    ClientId = make_clientid(ClientidPrefix, Index),
    ConnOpts = #{
        host => Host,
        port => Port,
        clientid => ClientId,
        proto_ver => ProtoVer,
        clean_start => CleanStart,
        keepalive => Keepalive,
        force_ping => true,
        connect_timeout => 10
    },
    ConnOpts1 = maybe_add_credentials(ConnOpts, Username, Password),
    ConnOpts2 = maybe_add_ssl(ConnOpts1, Host, SslConf),
    State = #{
        bridge_name => BridgeName,
        index => Index,
        conn_opts => ConnOpts2,
        bridge_config => BridgeConfig,
        client_pid => undefined,
        client_mon => undefined,
        connected => false
    },
    %% Attempt initial connection
    self() ! reconnect,
    {ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info(
    {publish_batch, Items, From, Ref},
    #{connected := true, client_pid := Pid} = State
) when is_pid(Pid) ->
    Result = do_publish_batch(Items, State),
    From ! {batch_ack, Ref, Result},
    {noreply, State};
handle_info({publish_batch, _Items, From, Ref}, State) ->
    From ! {batch_ack, Ref, {error, 0}},
    {noreply, State};
handle_info(reconnect, State) ->
    State1 = do_connect(State),
    {noreply, State1};
handle_info({'DOWN', Ref, process, Pid, Reason}, #{client_mon := Ref, client_pid := Pid} = State) ->
    #{bridge_name := BridgeName, index := Index} = State,
    ?LOG_WARNING(#{
        msg => "mqtt_dq_connector_down",
        bridge => BridgeName,
        index => Index,
        reason => Reason
    }),
    State1 = State#{
        client_pid := undefined,
        client_mon := undefined,
        connected := false
    },
    schedule_reconnect(),
    {noreply, State1};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    %% Stale monitor, ignore
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{client_pid := Pid}) ->
    maybe_disconnect(Pid),
    ok.

%%--------------------------------------------------------------------
%% Internal: publish
%%--------------------------------------------------------------------

do_publish_batch(Items, State) ->
    do_publish_batch(Items, 0, State).

do_publish_batch([], _NumOk, _State) ->
    ok;
do_publish_batch([Item | Rest], NumOk, State) ->
    case publish_one(Item, State) of
        ok -> do_publish_batch(Rest, NumOk + 1, State);
        {error, rejected} -> do_publish_batch(Rest, NumOk + 1, State);
        {error, network} -> {error, NumOk}
    end.

publish_one(#{topic := OrigTopic, payload := Payload, properties := Props}, State) ->
    #{
        client_pid := ClientPid,
        bridge_name := BridgeName,
        bridge_config := #{
            remote_topic := RemoteTopicTemplate,
            remote_qos := ConfigQoS,
            remote_retain := ConfigRetain
        }
    } = State,
    RenderedTopic = render_topic(RemoteTopicTemplate, OrigTopic),
    PubOpts = [{qos, ConfigQoS}, {retain, ConfigRetain}],
    do_publish(ClientPid, RenderedTopic, Props, Payload, PubOpts, BridgeName).

do_publish(ClientPid, Topic, Props, Payload, PubOpts, BridgeName) ->
    try emqtt:publish(ClientPid, Topic, Props, Payload, PubOpts) of
        Result -> handle_publish_result(Result, Topic, BridgeName)
    catch
        _:_ -> {error, network}
    end.

handle_publish_result(ok, _Topic, _BridgeName) ->
    ok;
handle_publish_result({ok, #{reason_code := RC}}, _Topic, _BridgeName) when
    RC =:= 0; RC =:= 16
->
    ok;
handle_publish_result({ok, #{reason_code := _RC}}, Topic, BridgeName) ->
    ?LOG_WARNING(#{
        msg => "mqtt_dq_publish_rejected",
        bridge => BridgeName,
        topic => Topic
    }),
    {error, rejected};
handle_publish_result({error, Reason}, Topic, BridgeName) ->
    ?LOG_ERROR(#{
        msg => "mqtt_dq_publish_error",
        bridge => BridgeName,
        topic => Topic,
        reason => Reason
    }),
    {error, network}.

render_topic(<<"${topic}">>, OrigTopic) ->
    OrigTopic;
render_topic(Template, OrigTopic) ->
    binary:replace(Template, <<"${topic}">>, OrigTopic, [global]).

%%--------------------------------------------------------------------
%% Internal: connection
%%--------------------------------------------------------------------

do_connect(#{conn_opts := ConnOpts, bridge_name := BridgeName, index := Index} = State) ->
    maybe_disconnect(maps:get(client_pid, State, undefined)),
    _ = safe_demonitor(maps:get(client_mon, State, undefined)),
    case start_and_connect(ConnOpts) of
        {ok, Pid} ->
            Mon = erlang:monitor(process, Pid),
            ?LOG_INFO(#{
                msg => "mqtt_dq_connector_connected",
                bridge => BridgeName,
                index => Index
            }),
            State#{
                client_pid := Pid,
                client_mon := Mon,
                connected := true
            };
        {error, Reason} ->
            ?LOG_WARNING(#{
                msg => "mqtt_dq_connector_connect_failed",
                bridge => BridgeName,
                index => Index,
                reason => Reason
            }),
            schedule_reconnect(),
            State#{
                client_pid := undefined,
                client_mon := undefined,
                connected := false
            }
    end.

start_and_connect(ConnOpts) ->
    try
        do_start_and_connect(ConnOpts)
    catch
        Class:Error -> {error, {Class, Error}}
    end.

do_start_and_connect(ConnOpts) ->
    case emqtt:start_link(ConnOpts) of
        {ok, Pid} -> try_connect(Pid);
        {error, Reason} -> {error, Reason}
    end.

try_connect(Pid) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            {ok, Pid};
        {error, Reason} ->
            catch emqtt:stop(Pid),
            {error, Reason}
    end.

schedule_reconnect() ->
    erlang:send_after(?RECONNECT_DELAY_MS, self(), reconnect).

maybe_disconnect(undefined) ->
    ok;
maybe_disconnect(Pid) when is_pid(Pid) ->
    try
        emqtt:disconnect(Pid),
        emqtt:stop(Pid)
    catch
        _:_ -> ok
    end.

safe_demonitor(undefined) ->
    ok;
safe_demonitor(Ref) ->
    erlang:demonitor(Ref, [flush]),
    ok.

parse_server(Server) when is_list(Server) ->
    parse_server(list_to_binary(Server));
parse_server(Server) when is_binary(Server) ->
    case binary:split(Server, <<":">>) of
        [Host, PortBin] ->
            {binary_to_list(Host), binary_to_integer(PortBin)};
        [Host] ->
            {binary_to_list(Host), 1883}
    end.

make_clientid(Prefix, Index) ->
    iolist_to_binary([Prefix, ":", integer_to_binary(Index)]).

maybe_add_credentials(Opts, <<>>, _) ->
    Opts;
maybe_add_credentials(Opts, Username, Password) ->
    Opts#{username => Username, password => Password}.

maybe_add_ssl(Opts, Host, #{enable := true} = SslConf) ->
    SslOpts = maybe_add_sni([], Host, SslConf),
    Opts#{ssl => true, ssl_opts => SslOpts};
maybe_add_ssl(Opts, _Host, _) ->
    Opts.

maybe_add_sni(SslOpts, _Host, #{server_name_indication := disable}) ->
    [{server_name_indication, disable} | SslOpts];
maybe_add_sni(SslOpts, _Host, #{server_name_indication := SNI}) when is_list(SNI), SNI =/= [] ->
    [{server_name_indication, SNI} | SslOpts];
maybe_add_sni(SslOpts, Host, _) ->
    [{server_name_indication, Host} | SslOpts].
