%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_connector_rocketmq).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1]).

%% `emqx_resource' API
-export([
    callback_mode/0,
    is_buffer_supported/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(ROCKETMQ_HOST_OPTIONS, #{
    default_port => 9876
}).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {servers, servers()},
        {topic,
            mk(
                binary(),
                #{default => <<"TopicTest">>, desc => ?DESC(topic)}
            )},
        {access_key,
            mk(
                binary(),
                #{default => <<>>, desc => ?DESC("access_key")}
            )},
        {secret_key,
            mk(
                binary(),
                #{default => <<>>, desc => ?DESC("secret_key")}
            )},
        {security_token, mk(binary(), #{default => <<>>, desc => ?DESC(security_token)})},
        {sync_timeout,
            mk(
                emqx_schema:duration(),
                #{default => <<"3s">>, desc => ?DESC(sync_timeout)}
            )},
        {refresh_interval,
            mk(
                emqx_schema:duration(),
                #{default => <<"3s">>, desc => ?DESC(refresh_interval)}
            )},
        {send_buffer,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1024KB">>, desc => ?DESC(send_buffer)}
            )},

        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

servers() ->
    Meta = #{desc => ?DESC("servers")},
    emqx_schema:servers_sc(Meta, ?ROCKETMQ_HOST_OPTIONS).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> always_sync.

is_buffer_supported() -> false.

on_start(
    InstanceId,
    #{servers := BinServers, topic := Topic, sync_timeout := SyncTimeout} = Config
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

    TopicTks = emqx_plugin_libs_rule:preproc_tmpl(Topic),
    #{acl_info := AclInfo} = ProducerOpts = make_producer_opts(Config),
    ClientCfg = #{acl_info => AclInfo},
    Templates = parse_template(Config),
    ProducersMapPID = create_producers_map(ClientId),
    State = #{
        client_id => ClientId,
        topic => Topic,
        topic_tokens => TopicTks,
        sync_timeout => SyncTimeout,
        templates => Templates,
        producers_map_pid => ProducersMapPID,
        producers_opts => emqx_secret:wrap(ProducerOpts)
    },

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

on_stop(InstanceId, #{client_id := ClientId, topic := RawTopic, producers_map_pid := Pid} = _State) ->
    ?SLOG(info, #{
        msg => "stopping_rocketmq_connector",
        connector => InstanceId
    }),

    Producers = ets:match(ClientId, {{RawTopic, '$1'}, '$2'}),
    lists:foreach(
        fun([Topic, Producer]) ->
            ets:delete(ClientId, {RawTopic, Topic}),
            _ = rocketmq:stop_and_delete_supervised_producers(Producer)
        end,
        Producers
    ),

    Pid ! ok,
    ok = rocketmq:stop_and_delete_supervised_client(ClientId).

on_query(InstanceId, Query, State) ->
    do_query(InstanceId, Query, send_sync, State).

%% We only support batch inserts and all messages must have the same topic
on_batch_query(InstanceId, [{send_message, _Msg} | _] = Query, State) ->
    do_query(InstanceId, Query, batch_send_sync, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

on_get_status(_InstanceId, #{client_id := ClientId}) ->
    case rocketmq_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            status_result(rocketmq_client:get_status(Pid));
        _ ->
            connecting
    end.

status_result(_Status = true) -> connected;
status_result(_Status) -> connecting.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(
    InstanceId,
    Query,
    QueryFunc,
    #{
        templates := Templates,
        client_id := ClientId,
        topic := RawTopic,
        topic_tokens := TopicTks,
        producers_opts := ProducerOpts,
        sync_timeout := RequestTimeout
    } = State
) ->
    ?TRACE(
        "QUERY",
        "rocketmq_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),

    TopicKey = get_topic_key(Query, RawTopic, TopicTks),
    Data = apply_template(Query, Templates),

    Result = safe_do_produce(
        InstanceId, QueryFunc, ClientId, TopicKey, Data, ProducerOpts, RequestTimeout
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

safe_do_produce(InstanceId, QueryFunc, ClientId, TopicKey, Data, ProducerOpts, RequestTimeout) ->
    try
        Producers = get_producers(ClientId, TopicKey, ProducerOpts),
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
    ParamsTks = emqx_plugin_libs_rule:preproc_tmpl(H),
    parse_template(
        T,
        Templates#{Key => ParamsTks}
    );
parse_template([], Templates) ->
    Templates.

get_topic_key({_, Msg}, RawTopic, TopicTks) ->
    {RawTopic, emqx_plugin_libs_rule:proc_tmpl(TopicTks, Msg)};
get_topic_key([Query | _], RawTopic, TopicTks) ->
    get_topic_key(Query, RawTopic, TopicTks).

apply_template({Key, Msg} = _Req, Templates) ->
    case maps:get(Key, Templates, undefined) of
        undefined ->
            emqx_utils_json:encode(Msg);
        Template ->
            emqx_plugin_libs_rule:proc_tmpl(Template, Msg)
    end;
apply_template([{Key, _} | _] = Reqs, Templates) ->
    case maps:get(Key, Templates, undefined) of
        undefined ->
            [emqx_utils_json:encode(Msg) || {_, Msg} <- Reqs];
        Template ->
            [emqx_plugin_libs_rule:proc_tmpl(Template, Msg) || {_, Msg} <- Reqs]
    end.

client_id(InstanceId) ->
    Name = emqx_resource_manager:manager_id_to_resource_id(InstanceId),
    erlang:binary_to_atom(Name, utf8).

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
        access_key := AccessKey,
        secret_key := SecretKey,
        security_token := SecurityToken,
        send_buffer := SendBuff,
        refresh_interval := RefreshInterval
    }
) ->
    ACLInfo = acl_info(AccessKey, SecretKey, SecurityToken),
    #{
        tcp_opts => [{sndbuf, SendBuff}],
        ref_topic_route_interval => RefreshInterval,
        acl_info => ACLInfo
    }.

acl_info(<<>>, <<>>, <<>>) ->
    #{};
acl_info(AccessKey, SecretKey, <<>>) when is_binary(AccessKey), is_binary(SecretKey) ->
    #{
        access_key => AccessKey,
        secret_key => SecretKey
    };
acl_info(AccessKey, SecretKey, SecurityToken) when
    is_binary(AccessKey), is_binary(SecretKey), is_binary(SecurityToken)
->
    #{
        access_key => AccessKey,
        secret_key => SecretKey,
        security_token => SecurityToken
    };
acl_info(_, _, _) ->
    #{}.

create_producers_map(ClientId) ->
    erlang:spawn(fun() ->
        case ets:whereis(ClientId) of
            undefined ->
                _ = ets:new(ClientId, [public, named_table]),
                ok;
            _ ->
                ok
        end,
        receive
            _Msg ->
                ok
        end
    end).

get_producers(ClientId, {_, Topic1} = TopicKey, ProducerOpts) ->
    case ets:lookup(ClientId, TopicKey) of
        [{_, Producers0}] ->
            Producers0;
        _ ->
            ProducerGroup = iolist_to_binary([atom_to_list(ClientId), "_", Topic1]),
            {ok, Producers0} = rocketmq:ensure_supervised_producers(
                ClientId, ProducerGroup, Topic1, emqx_secret:unwrap(ProducerOpts)
            ),
            ets:insert(ClientId, {TopicKey, Producers0}),
            Producers0
    end.
