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

%% @doc This module implements EMQX Bridge transport layer on top of MQTT protocol

-module(emqx_bridge_mqtt_actions).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_rule_engine/include/rule_actions.hrl").

-import(emqx_rule_utils, [str/1]).

-export([ on_resource_create/2
        , on_get_resource_status/2
        , on_resource_destroy/2
        ]).

%% Callbacks of ecpool Worker
-export([connect/1]).

-export([subscriptions/1]).

-export([ on_action_create_data_to_mqtt_broker/2
        , on_action_data_to_mqtt_broker/2
        ]).

-define(RESOURCE_TYPE_MQTT, 'bridge_mqtt').
-define(RESOURCE_TYPE_RPC, 'bridge_rpc').
-define(BAD_TOPIC_WITH_WILDCARD, wildcard_topic_not_allowed_for_publish).

-define(RESOURCE_CONFIG_SPEC_MQTT, #{
        address => #{
            order => 1,
            type => string,
            required => true,
            default => <<"127.0.0.1:1883">>,
            title => #{en => <<" Broker Address">>,
                       zh => <<"远程 broker 地址"/utf8>>},
            description => #{en => <<"The MQTT Remote Address">>,
                             zh => <<"远程 MQTT Broker 的地址"/utf8>>}
        },
        pool_size => #{
            order => 2,
            type => number,
            required => true,
            default => 8,
            title => #{en => <<"Pool Size">>,
                       zh => <<"连接池大小"/utf8>>},
            description => #{en => <<"MQTT Connection Pool Size">>,
                             zh => <<"连接池大小"/utf8>>}
        },
        clientid => #{
            order => 3,
            type => string,
            required => true,
            default => <<"client">>,
            title => #{en => <<"ClientId">>,
                       zh => <<"客户端 Id"/utf8>>},
            description => #{en => <<"ClientId for connecting to remote MQTT broker">>,
                             zh => <<"连接远程 Broker 的 ClientId"/utf8>>}
        },
        append => #{
            order => 4,
            type => boolean,
            required => false,
            default => true,
            title => #{en => <<"Append GUID">>,
                       zh => <<"附加 GUID"/utf8>>},
            description => #{en => <<"Append GUID to MQTT ClientId?">>,
                             zh => <<"是否将GUID附加到 MQTT ClientId 后"/utf8>>}
        },
        username => #{
            order => 5,
            type => string,
            required => false,
            default => <<"">>,
            title => #{en => <<"Username">>, zh => <<"用户名"/utf8>>},
            description => #{en => <<"Username for connecting to remote MQTT Broker">>,
                             zh => <<"连接远程 Broker 的用户名"/utf8>>}
        },
        password => #{
            order => 6,
            type => password,
            required => false,
            default => <<"">>,
            title => #{en => <<"Password">>,
                       zh => <<"密码"/utf8>>},
            description => #{en => <<"Password for connecting to remote MQTT Broker">>,
                             zh => <<"连接远程 Broker 的密码"/utf8>>}
        },
        mountpoint => #{
            order => 7,
            type => string,
            required => false,
            default => <<"bridge/aws/${node}/">>,
            title => #{en => <<"Bridge MountPoint">>,
                       zh => <<"桥接挂载点"/utf8>>},
            description => #{
                en => <<"MountPoint for bridge topic:<br/>"
                        "Example: The topic of messages sent to <code>topic1</code> on local node "
                        "will be transformed to <code>bridge/aws/${node}/topic1</code>">>,
                zh => <<"桥接主题的挂载点:<br/>"
                        "示例: 本地节点向 <code>topic1</code> 发消息，远程桥接节点的主题"
                        "会变换为 <code>bridge/aws/${node}/topic1</code>"/utf8>>
            }
        },
        disk_cache => #{
            order => 8,
            type => string,
            required => false,
            default => <<"off">>,
            enum => [<<"on">>, <<"off">>],
            title => #{en => <<"Disk Cache">>,
                       zh => <<"磁盘缓存"/utf8>>},
            description => #{en => <<"The flag which determines whether messages "
                                     "can be cached on local disk when bridge is "
                                     "disconnected">>,
                             zh => <<"当桥接断开时用于控制是否将消息缓存到本地磁"
                                     "盘队列上"/utf8>>}
        },
        proto_ver => #{
            order => 9,
            type => string,
            required => false,
            default => <<"mqttv4">>,
            enum => [<<"mqttv3">>, <<"mqttv4">>, <<"mqttv5">>],
            title => #{en => <<"Protocol Version">>,
                       zh => <<"协议版本"/utf8>>},
            description => #{en => <<"MQTTT Protocol version">>,
                             zh => <<"MQTT 协议版本"/utf8>>}
        },
        keepalive => #{
            order => 10,
            type => string,
            required => false,
            default => <<"60s">> ,
            title => #{en => <<"Keepalive">>,
                       zh => <<"心跳间隔"/utf8>>},
            description => #{en => <<"Keepalive">>,
                             zh => <<"心跳间隔"/utf8>>}
        },
        reconnect_interval => #{
            order => 11,
            type => string,
            required => false,
            default => <<"30s">>,
            title => #{en => <<"Reconnect Interval">>,
                       zh => <<"重连间隔"/utf8>>},
            description => #{en => <<"Reconnect interval of bridge:<br/>">>,
                             zh => <<"重连间隔"/utf8>>}
        },
        retry_interval => #{
            order => 12,
            type => string,
            required => false,
            default => <<"20s">>,
            title => #{en => <<"Retry interval">>,
                       zh => <<"重传间隔"/utf8>>},
            description => #{en => <<"Retry interval for bridge QoS1 message delivering">>,
                             zh => <<"消息重传间隔"/utf8>>}
        },
        bridge_mode => #{
            order => 13,
            type => boolean,
            required => false,
            default => false,
            title => #{en => <<"Bridge Mode">>,
                       zh => <<"桥接模式"/utf8>>},
            description => #{en => <<"Bridge mode for MQTT bridge connection">>,
                             zh => <<"MQTT 连接是否为桥接模式"/utf8>>}
        },
        ssl => #{
            order => 14,
            type => boolean,
            default => false,
            title => #{en => <<"Enable SSL">>,
                       zh => <<"开启SSL链接"/utf8>>},
            description => #{en => <<"Enable SSL or not">>,
                             zh => <<"是否开启 SSL"/utf8>>}
        },
        cacertfile => #{
            order => 15,
            type => file,
            required => false,
            default => <<"etc/certs/cacert.pem">>,
            title => #{en => <<"CA certificates">>,
                       zh => <<"CA 证书"/utf8>>},
            description => #{en => <<"The file path of the CA certificates">>,
                             zh => <<"CA 证书路径"/utf8>>}
        },
        certfile => #{
            order => 16,
            type => file,
            required => false,
            default => <<"etc/certs/client-cert.pem">>,
            title => #{en => <<"SSL Certfile">>,
                       zh => <<"SSL 客户端证书"/utf8>>},
            description => #{en => <<"The file path of the client certfile">>,
                             zh => <<"客户端证书路径"/utf8>>}
        },
        keyfile => #{
            order => 17,
            type => file,
            required => false,
            default => <<"etc/certs/client-key.pem">>,
            title => #{en => <<"SSL Keyfile">>,
                       zh => <<"SSL 密钥文件"/utf8>>},
            description => #{en => <<"The file path of the client keyfile">>,
                             zh => <<"客户端密钥路径"/utf8>>}
        },
        ciphers => #{
            order => 18,
            type => string,
            required => false,
            default => <<"ECDHE-ECDSA-AES256-GCM-SHA384,ECDHE-RSA-AES256-GCM-SHA384,",
                         "ECDHE-ECDSA-AES256-SHA384,ECDHE-RSA-AES256-SHA384,ECDHE-ECDSA-DES-CBC3-SHA,",
                         "ECDH-ECDSA-AES256-GCM-SHA384,ECDH-RSA-AES256-GCM-SHA384,ECDH-ECDSA-AES256-SHA384,",
                         "ECDH-RSA-AES256-SHA384,DHE-DSS-AES256-GCM-SHA384,DHE-DSS-AES256-SHA256,AES256-GCM-SHA384,",
                         "AES256-SHA256,ECDHE-ECDSA-AES128-GCM-SHA256,ECDHE-RSA-AES128-GCM-SHA256,",
                         "ECDHE-ECDSA-AES128-SHA256,ECDHE-RSA-AES128-SHA256,ECDH-ECDSA-AES128-GCM-SHA256,",
                         "ECDH-RSA-AES128-GCM-SHA256,ECDH-ECDSA-AES128-SHA256,ECDH-RSA-AES128-SHA256,",
                         "DHE-DSS-AES128-GCM-SHA256,DHE-DSS-AES128-SHA256,AES128-GCM-SHA256,AES128-SHA256,",
                         "ECDHE-ECDSA-AES256-SHA,ECDHE-RSA-AES256-SHA,DHE-DSS-AES256-SHA,ECDH-ECDSA-AES256-SHA,",
                         "ECDH-RSA-AES256-SHA,AES256-SHA,ECDHE-ECDSA-AES128-SHA,ECDHE-RSA-AES128-SHA,",
                         "DHE-DSS-AES128-SHA,ECDH-ECDSA-AES128-SHA,ECDH-RSA-AES128-SHA,AES128-SHA">>,
            title => #{en => <<"SSL Ciphers">>,
                       zh => <<"SSL 加密算法"/utf8>>},
            description => #{en => <<"SSL Ciphers">>,
                             zh => <<"SSL 加密算法"/utf8>>}
        },
        verify => #{
            order => 19,
            type => boolean,
            default => false,
            title => #{en => <<"Verify Server Certfile">>,
                       zh => <<"校验服务器证书"/utf8>>},
            description => #{en => <<"Whether to verify the server certificate. By default, the client will not verify the server's certificate. If verification is required, please set it to true.">>,
                             zh => <<"是否校验服务器证书。 默认客户端不会去校验服务器的证书，如果需要校验，请设置成true。"/utf8>>}
        },
        server_name_indication => #{
            order => 20,
            type => string,
            title => #{en => <<"Server Name Indication">>,
                    zh => <<"服务器名称指示"/utf8>>},
            description => #{en => <<"Specify the hostname used for peer certificate verification, or set to disable to turn off this verification.">>,
                            zh => <<"指定用于对端证书验证时使用的主机名，或者设置为 disable 以关闭此项验证。"/utf8>>}
        }
    }).

-define(RESOURCE_CONFIG_SPEC_RPC, #{
        address => #{
            order => 1,
            type => string,
            required => true,
            default => <<"emqx2@127.0.0.1">>,
            title => #{en => <<"EMQX Node Name">>,
                       zh => <<"EMQX 节点名称"/utf8>>},
            description => #{en => <<"EMQX Remote Node Name">>,
                             zh => <<"远程 EMQX 节点名称 "/utf8>>}
        },
        mountpoint => #{
            order => 2,
            type => string,
            required => false,
            default => <<"bridge/emqx/${node}/">>,
            title => #{en => <<"Bridge MountPoint">>,
                       zh => <<"桥接挂载点"/utf8>>},
            description => #{en => <<"MountPoint for bridge topic<br/>"
                                     "Example: The topic of messages sent to <code>topic1</code> on local node "
                                     "will be transformed to <code>bridge/emqx/${node}/topic1</code>">>,
                             zh => <<"桥接主题的挂载点<br/>"
                                     "示例: 本地节点向 <code>topic1</code> 发消息，远程桥接节点的主题"
                                     "会变换为 <code>bridge/emqx/${node}/topic1</code>"/utf8>>}
        },
        pool_size => #{
            order => 3,
            type => number,
            required => true,
            default => 8,
            title => #{en => <<"Pool Size">>,
                       zh => <<"连接池大小"/utf8>>},
            description => #{en => <<"MQTT/RPC Connection Pool Size">>,
                             zh => <<"连接池大小"/utf8>>}
        },
        reconnect_interval => #{
            order => 4,
            type => string,
            required => false,
            default => <<"30s">>,
            title => #{en => <<"Reconnect Interval">>,
                       zh => <<"重连间隔"/utf8>>},
            description => #{en => <<"Reconnect Interval of bridge">>,
                             zh => <<"重连间隔"/utf8>>}
        },
         batch_size => #{
            order => 5,
            type => number,
            required => false,
            default => 32,
            title => #{en => <<"Batch Size">>,
                       zh => <<"批处理大小"/utf8>>},
            description => #{en => <<"Batch Size">>,
                             zh => <<"批处理大小"/utf8>>}
        },
        disk_cache => #{
            order => 6,
            type => string,
            required => false,
            default => <<"off">>,
            enum => [<<"on">>, <<"off">>],
            title => #{en => <<"Disk Cache">>,
                       zh => <<"磁盘缓存"/utf8>>},
            description => #{en => <<"The flag which determines whether messages "
                                     "can be cached on local disk when bridge is "
                                     "disconnected">>,
                             zh => <<"当桥接断开时用于控制是否将消息缓存到本地磁"
                                     "盘队列上"/utf8>>}
        }
    }).

-define(ACTION_PARAM_RESOURCE, #{
        type => string,
        required => true,
        title => #{en => <<"Resource ID">>, zh => <<"资源 ID"/utf8>>},
        description => #{en => <<"Bind a resource to this action">>,
                         zh => <<"给动作绑定一个资源"/utf8>>}
    }).

-resource_type(#{
        name => ?RESOURCE_TYPE_MQTT,
        create => on_resource_create,
        status => on_get_resource_status,
        destroy => on_resource_destroy,
        params => ?RESOURCE_CONFIG_SPEC_MQTT,
        title => #{en => <<"MQTT Bridge">>, zh => <<"MQTT Bridge"/utf8>>},
        description => #{en => <<"MQTT Message Bridge">>, zh => <<"MQTT 消息桥接"/utf8>>}
    }).


-resource_type(#{
        name => ?RESOURCE_TYPE_RPC,
        create => on_resource_create,
        status => on_get_resource_status,
        destroy => on_resource_destroy,
        params => ?RESOURCE_CONFIG_SPEC_RPC,
        title => #{en => <<"EMQX Bridge">>, zh => <<"EMQX Bridge"/utf8>>},
        description => #{en => <<"EMQX RPC Bridge">>, zh => <<"EMQX RPC 消息桥接"/utf8>>}
    }).

-rule_action(#{
        name => data_to_mqtt_broker,
        category => data_forward,
        for => 'message.publish',
        types => [?RESOURCE_TYPE_MQTT, ?RESOURCE_TYPE_RPC],
        create => on_action_create_data_to_mqtt_broker,
        params => #{'$resource' => ?ACTION_PARAM_RESOURCE,
                    forward_topic => #{
                        order => 1,
                        type => string,
                        required => false,
                        default => <<"">>,
                        title => #{en => <<"Forward Topic">>,
                                   zh => <<"转发消息主题"/utf8>>},
                        description => #{en => <<"The topic used when forwarding the message. "
                                                 "Defaults to the topic of the bridge message if not provided.">>,
                                         zh => <<"转发消息时使用的主题。如果未提供，则默认为桥接消息的主题。"/utf8>>}
                    },
                    payload_tmpl => #{
                        order => 2,
                        type => string,
                        input => textarea,
                        required => false,
                        default => <<"">>,
                        title => #{en => <<"Payload Template">>,
                                   zh => <<"消息内容模板"/utf8>>},
                        description => #{en => <<"The payload template, variable interpolation is supported. "
                                                 "If using empty template (default), then the payload will be "
                                                 "all the available vars in JSON format">>,
                                         zh => <<"消息内容模板，支持变量。"
                                                 "若使用空模板（默认），消息内容为 JSON 格式的所有字段"/utf8>>}
                    }
        },
        title => #{en => <<"Data bridge to MQTT Broker">>,
                   zh => <<"桥接数据到 MQTT Broker"/utf8>>},
        description => #{en => <<"Bridge Data to MQTT Broker">>,
                         zh => <<"桥接数据到 MQTT Broker"/utf8>>}
    }).

on_resource_create(ResId, Params) ->
    ?LOG(info, "Initiating Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
    {ok, _} = application:ensure_all_started(ecpool),
    PoolName = pool_name(ResId),
    Options = options(Params, PoolName, ResId),
    start_resource(ResId, PoolName, Options),
    case test_resource_status(PoolName) of
        true -> ok;
        false ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MQTT, ResId}, connection_failed})
    end,
    #{<<"pool">> => PoolName}.

start_resource(ResId, PoolName, Options) ->
    case ecpool:start_sup_pool(PoolName, ?MODULE, Options) of
        {ok, _} ->
            ?LOG(info, "Initiated Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]);
        {error, {already_started, _Pid}} ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            start_resource(ResId, PoolName, Options);
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "Initiate Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MQTT, ResId, Reason]),
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MQTT, ResId}, create_failed})
    end.

test_resource_status(PoolName) ->
    Parent = self(),
    Pids = [spawn(fun() -> Parent ! {self(), get_worker_status(Worker)} end)
            || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    try
        Status = [
            receive {Pid, R} -> R
            after 10000 -> %% get_worker_status/1 should be a quick operation
                throw({timeout, Pid})
            end || Pid <- Pids],
        lists:any(fun(St) -> St =:= true end, Status)
    catch
        throw:Reason ->
            ?LOG(error, "Get mqtt bridge status timeout: ~p", [Reason]),
            lists:foreach(fun(Pid) -> exit(Pid, kill) end, Pids),
            false
    end.

-define(RETRY_TIMES, 4).

get_worker_status(Worker) ->
    get_worker_status(Worker, ?RETRY_TIMES).

get_worker_status(_Worker, 0) ->
    false;
get_worker_status(Worker, Times) ->
    case ecpool_worker:client(Worker) of
        {ok, Bridge} ->
            try emqx_bridge_worker:status(Bridge) of
                connected ->
                    true;
                idle ->
                    ?LOG(info, "MQTT Bridge get status idle. Should not ignore this."),
                    timer:sleep(100),
                    get_worker_status(Worker, Times - 1);
                ErrorStatus ->
                    ?LOG(error, "MQTT Bridge get status ~p", [ErrorStatus]),
                    false
            catch Error:Reason:ST ->
                    ?LOG(error, "MQTT Bridge get status error: ~p reason: ~p stacktrace: ~p", [Error, Reason, ST]),
                    false
            end;
        {error, _} ->
            false
    end.

-spec(on_get_resource_status(ResId::binary(), Params::map()) -> Status::map()).
on_get_resource_status(_ResId, #{<<"pool">> := PoolName}) ->
    IsAlive = test_resource_status(PoolName),
    #{is_alive => IsAlive}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
    ?LOG(info, "Destroying Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
    case ecpool:stop_sup_pool(PoolName) of
        ok ->
            ?LOG(info, "Destroyed Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]);
        {error, Reason} ->
            ?LOG(error, "Destroy Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MQTT, ResId, Reason]),
            error({{?RESOURCE_TYPE_MQTT, ResId}, destroy_failed})
    end.

on_action_create_data_to_mqtt_broker(ActId, Opts = #{<<"pool">> := PoolName,
                                                     <<"forward_topic">> := ForwardTopic,
                                                     <<"payload_tmpl">> := PayloadTmpl}) ->
    ?LOG(info, "Initiating Action ~p.", [?FUNCTION_NAME]),
    PayloadTks = emqx_rule_utils:preproc_tmpl(PayloadTmpl),
    TopicTks = case ForwardTopic == <<"">> of
        true -> undefined;
        false -> emqx_rule_utils:preproc_tmpl(assert_topic_valid(ForwardTopic))
    end,
    Opts.

on_action_data_to_mqtt_broker(Msg, _Env =
                              #{id := Id, clientid := From, flags := Flags,
                                topic := Topic, timestamp := TimeStamp, qos := QoS,
                                ?BINDING_KEYS := #{
                                    'ActId' := ActId,
                                    'PoolName' := PoolName,
                                    'TopicTks' := TopicTks,
                                    'PayloadTks' := PayloadTks
                                }}) ->
    Topic1 = case TopicTks =:= undefined of
        true -> Topic;
        false -> emqx_rule_utils:proc_tmpl(TopicTks, Msg)
    end,
    BrokerMsg = #message{id = Id,
                         qos = QoS,
                         from = From,
                         flags = Flags,
                         topic = assert_topic_valid(Topic1),
                         payload = format_data(PayloadTks, Msg),
                         timestamp = TimeStamp},
    ecpool:with_client(PoolName,
      fun(BridgePid) ->
        BridgePid ! {deliver, rule_engine, BrokerMsg}
      end),
    emqx_rule_metrics:inc_actions_success(ActId).

format_data([], Msg) ->
    emqx_json:encode(Msg);

format_data(Tokens, Msg) ->
    emqx_rule_utils:proc_tmpl(Tokens, Msg).

subscriptions(Subscriptions) ->
    scan_binary(<<"[", Subscriptions/binary, "].">>).

is_node_addr(Addr0) ->
    Addr = binary_to_list(Addr0),
    case string:tokens(Addr, "@") of
        [_NodeName, _Hostname] -> true;
        _ -> false
    end.

scan_binary(Bin) ->
    TermString = binary_to_list(Bin),
    scan_string(TermString).

scan_string(TermString) ->
    {ok, Tokens, _} = erl_scan:string(TermString),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

connect(Options) when is_list(Options) ->
    connect(maps:from_list(Options));
connect(Options = #{disk_cache := DiskCache, ecpool_worker_id := Id, pool_name := Pool}) ->
    Options0 = case DiskCache of
                   true ->
                       DataDir = filename:join([emqx:get_env(data_dir), replayq, Pool, integer_to_list(Id)]),
                       QueueOption = #{replayq_dir => DataDir},
                       Options#{queue => QueueOption};
                   false ->
                       Options
               end,
    Options1 = case maps:is_key(append, Options0) of
        false -> Options0;
        true ->
            case maps:get(append, Options0, false) of
                true ->
                    ClientId = lists:concat([str(maps:get(clientid, Options0)), "_", str(emqx_guid:to_hexstr(emqx_guid:gen()))]),
                    Options0#{clientid => ClientId};
                false ->
                    Options0
            end
    end,
    Options2 = maps:without([ecpool_worker_id, pool_name, append], Options1),
    emqx_bridge_worker:start_link(name(Pool, Id), Options2).
name(Pool, Id) ->
    list_to_atom(atom_to_list(Pool) ++ ":" ++ integer_to_list(Id)).
pool_name(ResId) ->
    list_to_atom("bridge_mqtt:" ++ str(ResId)).

options(Options, PoolName, ResId) ->
    GetD = fun(Key, Default) -> maps:get(Key, Options, Default) end,
    Get = fun(Key) -> GetD(Key, undefined) end,
    Address = Get(<<"address">>),
    [{max_inflight_batches, 32},
     {forward_mountpoint, str(assert_topic_valid(Get(<<"mountpoint">>)))},
     {disk_cache, cuttlefish_flag:parse(str(GetD(<<"disk_cache">>, "off")))},
     {start_type, auto},
     {reconnect_delay_ms, cuttlefish_duration:parse(str(Get(<<"reconnect_interval">>)), ms)},
     {if_record_metrics, false},
     {pool_size, GetD(<<"pool_size">>, 1)},
     {pool_name, PoolName}
    ] ++ case is_node_addr(Address) of
             true ->
                 [{address, binary_to_atom(Get(<<"address">>), utf8)},
                  {connect_module, emqx_bridge_rpc},
                  {batch_size, Get(<<"batch_size">>)}];
             false ->
                 [{address, binary_to_list(Address)},
                  {bridge_mode, GetD(<<"bridge_mode">>, true)},
                  {clean_start, true},
                  {clientid, str(Get(<<"clientid">>))},
                  {append, Get(<<"append">>)},
                  {connect_module, emqx_bridge_mqtt},
                  {keepalive, cuttlefish_duration:parse(str(Get(<<"keepalive">>)), s)},
                  {username, str(Get(<<"username">>))},
                  {password, str(Get(<<"password">>))},
                  {proto_ver, mqtt_ver(Get(<<"proto_ver">>))},
                  {retry_interval, cuttlefish_duration:parse(str(GetD(<<"retry_interval">>, "30s")), s)}
                  | maybe_ssl(Options, Get(<<"ssl">>), ResId)]
         end.

assert_topic_valid(T) ->
    case emqx_topic:wildcard(T) of
        true -> throw({?BAD_TOPIC_WITH_WILDCARD, T});
        false -> T
    end.

maybe_ssl(_Options, false, _ResId) ->
    [];
maybe_ssl(Options, true, ResId) ->
    [{ssl, true}, {ssl_opts, emqx_plugin_libs_ssl:save_files_return_opts(Options, "rules", ResId)}].

mqtt_ver(ProtoVer) ->
    case ProtoVer of
       <<"mqttv3">> -> v3;
       <<"mqttv4">> -> v4;
       <<"mqttv5">> -> v5;
       _ -> v4
   end.
