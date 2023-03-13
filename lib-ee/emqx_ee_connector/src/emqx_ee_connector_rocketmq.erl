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

-ifdef(TEST).
-export([execute/2]).
-endif.

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {topic,
            mk(
                binary(),
                #{default => <<"TopicTest">>, desc => ?DESC(topic)}
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
        {security_token, mk(binary(), #{default => <<>>, desc => ?DESC(security_token)})}
        | relational_fields()
    ].

add_default_username(Fields) ->
    lists:map(
        fun
            ({username, OrigUsernameFn}) ->
                {username, add_default_fn(OrigUsernameFn, <<"">>)};
            (Field) ->
                Field
        end,
        Fields
    ).

add_default_fn(OrigFn, Default) ->
    fun
        (default) -> Default;
        (Field) -> OrigFn(Field)
    end.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?ROCKETMQ_HOST_OPTIONS).

relational_fields() ->
    Fields = [username, password, auto_reconnect],
    Values = lists:filter(
        fun({E, _}) -> lists:member(E, Fields) end,
        emqx_connector_schema_lib:relational_db_fields()
    ),
    add_default_username(Values).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> always_sync.

is_buffer_supported() -> false.

on_start(
    InstanceId,
    #{server := Server, topic := Topic} = Config1
) ->
    ?SLOG(info, #{
        msg => "starting_rocketmq_connector",
        connector => InstanceId,
        config => redact(Config1)
    }),
    Config = maps:merge(default_security_info(), Config1),
    {Host, Port} = emqx_schema:parse_server(Server, ?ROCKETMQ_HOST_OPTIONS),

    Server1 = [{Host, Port}],
    ClientId = client_id(InstanceId),
    ClientCfg = #{acl_info => #{}},

    TopicTks = emqx_plugin_libs_rule:preproc_tmpl(Topic),
    ProducerOpts = make_producer_opts(Config),
    Templates = parse_template(Config),
    ProducersMapPID = create_producers_map(ClientId),
    State = #{
        client_id => ClientId,
        topic_tokens => TopicTks,
        config => Config,
        templates => Templates,
        producers_map_pid => ProducersMapPID,
        producers_opts => ProducerOpts
    },

    case rocketmq:ensure_supervised_client(ClientId, Server1, ClientCfg) of
        {ok, _Pid} ->
            {ok, State};
        {error, _Reason} = Error ->
            ?tp(
                rocketmq_connector_start_failed,
                #{error => _Reason}
            ),
            Error
    end.

on_stop(InstanceId, #{client_id := ClientId, producers_map_pid := Pid} = _State) ->
    ?SLOG(info, #{
        msg => "stopping_rocketmq_connector",
        connector => InstanceId
    }),
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
        {ok, _Pid} ->
            connected;
        _ ->
            connecting
    end.

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
        topic_tokens := TopicTks,
        producers_opts := ProducerOpts,
        config := #{topic := RawTopic, resource_opts := #{request_timeout := RequestTimeout}}
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
            {error, {unrecoverable_error, Reason}}
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
            emqx_json:encode(Msg);
        Template ->
            emqx_plugin_libs_rule:proc_tmpl(Template, Msg)
    end;
apply_template([{Key, _} | _] = Reqs, Templates) ->
    case maps:get(Key, Templates, undefined) of
        undefined ->
            [emqx_json:encode(Msg) || {_, Msg} <- Reqs];
        Template ->
            [emqx_plugin_libs_rule:proc_tmpl(Template, Msg) || {_, Msg} <- Reqs]
    end.

client_id(InstanceId) ->
    Name = emqx_resource_manager:manager_id_to_resource_id(InstanceId),
    erlang:binary_to_atom(Name, utf8).

redact(Msg) ->
    emqx_misc:redact(Msg, fun is_sensitive_key/1).

is_sensitive_key(security_token) ->
    true;
is_sensitive_key(_) ->
    false.

make_producer_opts(
    #{
        username := Username,
        password := Password,
        security_token := SecurityToken,
        send_buffer := SendBuff,
        refresh_interval := RefreshInterval
    }
) ->
    ACLInfo = acl_info(Username, Password, SecurityToken),
    #{
        tcp_opts => [{sndbuf, SendBuff}],
        ref_topic_route_interval => RefreshInterval,
        acl_info => ACLInfo
    }.

acl_info(<<>>, <<>>, <<>>) ->
    #{};
acl_info(Username, Password, <<>>) when is_binary(Username), is_binary(Password) ->
    #{
        access_key => Username,
        secret_key => Password
    };
acl_info(Username, Password, SecurityToken) when
    is_binary(Username), is_binary(Password), is_binary(SecurityToken)
->
    #{
        access_key => Username,
        secret_key => Password,
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
                ClientId, ProducerGroup, Topic1, ProducerOpts
            ),
            ets:insert(ClientId, {TopicKey, Producers0}),
            Producers0
    end.

default_security_info() ->
    #{username => <<>>, password => <<>>, security_token => <<>>}.
