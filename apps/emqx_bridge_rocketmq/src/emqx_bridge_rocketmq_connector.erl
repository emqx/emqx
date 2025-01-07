%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rocketmq_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, namespace/0]).

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-import(hoconsc, [mk/2]).

-define(ROCKETMQ_HOST_OPTIONS, #{
    default_port => 9876
}).

%%=====================================================================
%% Hocon schema

namespace() -> rocketmq.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {servers, servers()},
        {namespace,
            mk(
                binary(),
                #{required => false, desc => ?DESC(namespace)}
            )},
        {topic,
            mk(
                emqx_schema:template(),
                #{default => <<"TopicTest">>, desc => ?DESC(topic)}
            )},
        {access_key,
            mk(
                binary(),
                #{default => <<>>, desc => ?DESC("access_key")}
            )},
        {secret_key, emqx_schema_secret:mk(#{default => <<>>, desc => ?DESC("secret_key")})},
        {security_token, emqx_schema_secret:mk(#{default => <<>>, desc => ?DESC(security_token)})},
        {sync_timeout,
            mk(
                emqx_schema:timeout_duration(),
                #{default => <<"3s">>, desc => ?DESC(sync_timeout)}
            )},
        {refresh_interval,
            mk(
                emqx_schema:timeout_duration(),
                #{default => <<"3s">>, desc => ?DESC(refresh_interval)}
            )},
        {send_buffer,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1024KB">>, desc => ?DESC(send_buffer)}
            )},

        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ] ++ emqx_connector_schema_lib:ssl_fields().

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?ROCKETMQ_HOST_OPTIONS).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

resource_type() -> rocketmq.

callback_mode() -> always_sync.

on_start(
    InstanceId,
    #{
        servers := BinServers,
        access_key := AccessKey,
        secret_key := SecretKey,
        security_token := SecurityToken,
        ssl := SSLOptsMap
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_rocketmq_connector",
        connector => InstanceId,
        config => redact(Config)
    }),
    Servers = lists:map(
        fun(#{hostname := Host, port := Port}) -> {Host, Port} end,
        emqx_schema:parse_servers(BinServers, ?ROCKETMQ_HOST_OPTIONS)
    ),
    ClientId = client_id(InstanceId),
    ACLInfo = acl_info(AccessKey, SecretKey, SecurityToken),
    Namespace = maps:get(namespace, Config, <<>>),
    ClientCfg0 = #{acl_info => ACLInfo, namespace => Namespace},
    SSLOpts = emqx_tls_lib:to_client_opts(SSLOptsMap),
    ClientCfg = emqx_utils_maps:put_if(ClientCfg0, ssl_opts, SSLOpts, SSLOpts =/= []),
    State = #{
        client_id => ClientId,
        acl_info => ACLInfo,
        namespace => Namespace,
        installed_channels => #{},
        ssl_opts => SSLOpts
    },

    ok = emqx_resource:allocate_resource(InstanceId, client_id, ClientId),
    create_producers_map(ClientId),

    case rocketmq:ensure_supervised_client(ClientId, Servers, ClientCfg) of
        {ok, _Pid} ->
            {ok, State};
        {error, Reason0} ->
            Reason = redact(Reason0),
            ?tp(
                rocketmq_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        namespace := Namespace,
        acl_info := ACLInfo,
        ssl_opts := SSLOpts
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ChannelConfig, ACLInfo, Namespace, SSLOpts),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

create_channel_state(
    #{parameters := Conf} = _ChannelConfig,
    ACLInfo,
    Namespace,
    SSLOpts
) ->
    #{
        topic := Topic,
        sync_timeout := SyncTimeout,
        strategy := Strategy
    } = Conf,
    TopicTks = emqx_placeholder:preproc_tmpl(Topic),
    ProducerOpts = make_producer_opts(Conf, ACLInfo, Namespace, Strategy, SSLOpts),
    Templates = parse_template(Conf),
    DispatchStrategy = parse_dispatch_strategy(Strategy),
    State = #{
        topic => Topic,
        topic_tokens => TopicTks,
        templates => Templates,
        dispatch_strategy => DispatchStrategy,
        sync_timeout => SyncTimeout,
        acl_info => ACLInfo,
        producers_opts => ProducerOpts
    },
    {ok, State}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    InstanceId,
    ChannelId,
    #{installed_channels := Channels} = State
) ->
    case maps:find(ChannelId, Channels) of
        {ok, _} -> on_get_status(InstanceId, State);
        error -> ?status_disconnected
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_rocketmq_connector",
        connector => InstanceId
    }),

    lists:foreach(
        fun
            ({_, client_id, ClientId}) ->
                destory_producers_map(ClientId),
                ok = rocketmq:stop_and_delete_supervised_client(ClientId);
            ({_, _ProducerGroup, Producer}) ->
                _ = rocketmq:stop_and_delete_supervised_producers(Producer)
        end,
        emqx_resource:get_allocated_resources_list(InstanceId)
    ).

on_query(InstanceId, Query, State) ->
    do_query(InstanceId, Query, send_sync, State).

%% We only support batch inserts and all messages must have the same topic
on_batch_query(InstanceId, [{_ChannelId, _Msg} | _] = Query, State) ->
    do_query(InstanceId, Query, batch_send_sync, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

on_get_status(_InstanceId, #{client_id := ClientId}) ->
    case rocketmq_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            status_result(rocketmq_client:get_status(Pid));
        _ ->
            ?status_connecting
    end.

status_result(_Status = true) -> ?status_connected;
status_result(_Status) -> ?status_connecting.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(
    InstanceId,
    Query,
    QueryFunc,
    #{
        client_id := ClientId,
        installed_channels := Channels
    } = State
) ->
    ?TRACE(
        "QUERY",
        "rocketmq_connector_received",
        #{connector => InstanceId, query => Query, state => redact(State)}
    ),
    ChannelId = get_channel_id(Query),
    #{
        topic_tokens := TopicTks,
        templates := Templates,
        dispatch_strategy := DispatchStrategy,
        sync_timeout := RequestTimeout,
        producers_opts := ProducerOpts
    } = maps:get(ChannelId, Channels),

    TopicKey = get_topic_key(Query, TopicTks),
    Data = apply_template(Query, Templates, DispatchStrategy),
    emqx_trace:rendered_action_template(ChannelId, #{
        topic_key => TopicKey,
        data => Data,
        request_timeout => RequestTimeout
    }),
    Result = safe_do_produce(
        ChannelId, InstanceId, QueryFunc, ClientId, TopicKey, Data, ProducerOpts, RequestTimeout
    ),
    case Result of
        {error, Reason} ->
            ?tp(
                rocketmq_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "rocketmq_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
                reason => Reason
            }),
            Result;
        _ ->
            ?tp(
                rocketmq_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

get_channel_id({ChannelId, _}) -> ChannelId;
get_channel_id([{ChannelId, _} | _]) -> ChannelId.

safe_do_produce(
    ChannelId, InstanceId, QueryFunc, ClientId, TopicKey, Data, ProducerOpts, RequestTimeout
) ->
    try
        Producers = get_producers(ChannelId, InstanceId, ClientId, TopicKey, ProducerOpts),
        produce(InstanceId, QueryFunc, Producers, Data, RequestTimeout)
    catch
        _Type:Reason ->
            {error, {unrecoverable_error, redact(Reason)}}
    end.

produce(_InstanceId, QueryFunc, Producers, Data, RequestTimeout) ->
    rocketmq:QueryFunc(Producers, Data, RequestTimeout).

parse_template(Config) ->
    Templates =
        case maps:get(template, Config, undefined) of
            undefined -> #{};
            <<>> -> #{};
            Template -> #{send_message => Template}
        end,

    parse_template(maps:to_list(Templates), #{}).

parse_template([{Key, H} | T], Templates) ->
    ParamsTks = emqx_placeholder:preproc_tmpl(H),
    parse_template(
        T,
        Templates#{Key => ParamsTks}
    );
parse_template([], Templates) ->
    Templates.

%% returns a procedure to generate the produce context
parse_dispatch_strategy(roundrobin) ->
    fun(_) ->
        #{}
    end;
parse_dispatch_strategy(Template) ->
    Tokens = emqx_placeholder:preproc_tmpl(Template),
    fun(Msg) ->
        #{
            key =>
                case emqx_placeholder:proc_tmpl(Tokens, Msg) of
                    <<"undefined">> ->
                        %% Since the key may be absent on some kinds of events (ex:
                        %% `topic' is absent in `client.disconnected'), and this key is
                        %% used for routing, we generate a random key when it's absent to
                        %% better distribute the load, effectively making it `random'
                        %% dispatch if the key is absent and we are using `key_dispatch'.
                        %% Otherwise, it'll be deterministic.
                        emqx_utils:rand_id(8);
                    Key ->
                        Key
                end
        }
    end.

get_topic_key({_, Msg}, TopicTks) ->
    emqx_placeholder:proc_tmpl(TopicTks, Msg);
get_topic_key([Query | _], TopicTks) ->
    get_topic_key(Query, TopicTks).

%% return a message data and its context,
%% {binary(), rocketmq_producers:produce_context()})
apply_template({Key, Msg} = _Req, Templates, DispatchStrategy) ->
    {
        case maps:get(Key, Templates, undefined) of
            undefined ->
                emqx_utils_json:encode(Msg);
            Template ->
                emqx_placeholder:proc_tmpl(Template, Msg)
        end,
        DispatchStrategy(Msg)
    };
apply_template([{Key, _} | _] = Reqs, Templates, DispatchStrategy) ->
    case maps:get(Key, Templates, undefined) of
        undefined ->
            [{emqx_utils_json:encode(Msg), DispatchStrategy(Msg)} || {_, Msg} <- Reqs];
        Template ->
            [
                {emqx_placeholder:proc_tmpl(Template, Msg), DispatchStrategy(Msg)}
             || {_, Msg} <- Reqs
            ]
    end.

client_id(ResourceId) ->
    erlang:binary_to_atom(ResourceId, utf8).

redact(Msg) ->
    emqx_utils:redact(Msg, fun is_sensitive_key/1).

is_sensitive_key(secret_key) ->
    true;
is_sensitive_key(security_token) ->
    true;
is_sensitive_key(_) ->
    false.

make_producer_opts(
    #{
        send_buffer := SendBuff,
        refresh_interval := RefreshInterval
    },
    ACLInfo,
    Namespace,
    Strategy,
    SSLOpts
) ->
    ProducerOpts = #{
        tcp_opts => [{sndbuf, SendBuff}],
        ref_topic_route_interval => RefreshInterval,
        acl_info => emqx_secret:wrap(ACLInfo),
        namespace => Namespace,
        partitioner =>
            case Strategy of
                roundrobin -> roundrobin;
                _ -> key_dispatch
            end
    },
    emqx_utils_maps:put_if(ProducerOpts, ssl_opts, SSLOpts, SSLOpts =/= []).

acl_info(<<>>, _, _) ->
    #{};
acl_info(AccessKey, SecretKey, SecurityToken) when is_binary(AccessKey) ->
    Info = #{
        access_key => AccessKey,
        secret_key => emqx_maybe:define(emqx_secret:unwrap(SecretKey), <<>>)
    },
    case emqx_maybe:define(emqx_secret:unwrap(SecurityToken), <<>>) of
        <<>> ->
            Info;
        Token ->
            Info#{security_token => Token}
    end;
acl_info(_, _, _) ->
    #{}.

create_producers_map(ClientId) ->
    _ = ets:new(ClientId, [public, named_table, {read_concurrency, true}]),
    ok.

%% The resource manager will not terminate when restarting a resource,
%% so manually destroying the ets table is necessary.
destory_producers_map(ClientId) ->
    case ets:whereis(ClientId) of
        undefined ->
            ok;
        Tid ->
            ets:delete(Tid)
    end.

get_producers(ChannelId, InstanceId, ClientId, Topic, ProducerOpts) ->
    %% The topic need to be included in the name since we can have multiple
    %% topics per channel due to templating.
    ProducerGroup = iolist_to_binary([ChannelId, "_", Topic]),
    case ets:lookup(ClientId, ProducerGroup) of
        [{_, Producers}] ->
            Producers;
        _ ->
            %% TODO: the name needs to be an atom but this may cause atom leak so we
            %% should figure out a way to avoid this
            ProducerOpts2 = ProducerOpts#{name => binary_to_atom(ProducerGroup)},
            {ok, Producers} = rocketmq:ensure_supervised_producers(
                ClientId, ProducerGroup, Topic, ProducerOpts2
            ),
            ok = emqx_resource:allocate_resource(InstanceId, ProducerGroup, Producers),
            ets:insert(ClientId, {ProducerGroup, Producers}),
            Producers
    end.
