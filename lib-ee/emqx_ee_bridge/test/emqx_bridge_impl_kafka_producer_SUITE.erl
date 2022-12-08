%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_impl_kafka_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("brod/include/brod.hrl").

-define(PRODUCER, emqx_bridge_impl_kafka).

%%------------------------------------------------------------------------------
%% Things for REST API tests
%%------------------------------------------------------------------------------

-import(
    emqx_common_test_http,
    [
        request_api/3,
        request_api/5,
        get_http_data/1
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_dashboard.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083").

%% -define(API_VERSION, "v5").

-define(BASE_PATH, "/api/v5").

-define(APP_DASHBOARD, emqx_dashboard).
-define(APP_MANAGEMENT, emqx_management).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(300) ->
    ct:fail("Kafka is not up even though we have waited for a while");
wait_until_kafka_is_up(Attempts) ->
    KafkaTopic = "test-topic-one-partition",
    case resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_until_kafka_is_up(Attempts + 1)
    end.

init_per_suite(Config) ->
    %% Need to unload emqx_authz. See emqx_machine_SUITE:init_per_suite for
    %% more info.
    application:unload(emqx_authz),
    %% some configs in emqx_conf app are mandatory
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_rule_engine, emqx_bridge, emqx_management, emqx_dashboard],
        fun set_special_configs/1
    ),
    application:set_env(emqx_machine, applications, [
        emqx_prometheus,
        emqx_modules,
        emqx_dashboard,
        emqx_gateway,
        emqx_statsd,
        emqx_resource,
        emqx_rule_engine,
        emqx_bridge,
        emqx_ee_bridge,
        emqx_plugin_libs,
        emqx_management,
        emqx_retainer,
        emqx_exhook,
        emqx_authn,
        emqx_authz,
        emqx_plugin
    ]),
    {ok, _} = application:ensure_all_started(emqx_machine),
    wait_until_kafka_is_up(),
    %% Wait until bridges API is up
    (fun WaitUntilRestApiUp() ->
        case show(http_get(["bridges"])) of
            {ok, 200, _Res} ->
                ok;
            Val ->
                ct:pal("REST API for bridges not up. Wait and try again. Response: ~p", [Val]),
                timer:sleep(1000),
                WaitUntilRestApiUp()
        end
    end)(),
    Config.

end_per_suite(Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx_prometheus,
        emqx_modules,
        emqx_dashboard,
        emqx_gateway,
        emqx_statsd,
        emqx_resource,
        emqx_rule_engine,
        emqx_bridge,
        emqx_ee_bridge,
        emqx_plugin_libs,
        emqx_management,
        emqx_retainer,
        emqx_exhook,
        emqx_authn,
        emqx_authz,
        emqx_plugin,
        emqx_conf,
        emqx_bridge,
        emqx_management,
        emqx_dashboard,
        emqx_machine
    ]),
    mria:stop(),
    Config.

set_special_configs(emqx_management) ->
    Listeners = #{http => #{port => 8081}},
    Config = #{
        listeners => Listeners,
        applications => [#{id => "admin", secret => "public"}]
    },
    emqx_config:put([emqx_management], Config),
    ok;
set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.
%%------------------------------------------------------------------------------
%% Test cases for all combinations of SSL, no SSL and authentication types
%%------------------------------------------------------------------------------

t_publish_no_auth(_CtConfig) ->
    publish_with_and_without_ssl("none").

t_publish_sasl_plain(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_plain_settings()).

t_publish_sasl_scram256(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_scram256_settings()).

t_publish_sasl_scram512(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_scram512_settings()).

t_publish_sasl_kerberos(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_kerberos_settings()).

%%------------------------------------------------------------------------------
%% Test cases for REST api
%%------------------------------------------------------------------------------

show(X) ->
    % erlang:display('______________ SHOW ______________:'),
    % erlang:display(X),
    X.

t_kafka_bridge_rest_api_plain_text(_CtConfig) ->
    kafka_bridge_rest_api_all_auth_methods(false).

t_kafka_bridge_rest_api_ssl(_CtConfig) ->
    kafka_bridge_rest_api_all_auth_methods(true).

kafka_bridge_rest_api_all_auth_methods(UseSSL) ->
    NormalHostsString =
        case UseSSL of
            true -> kafka_hosts_string_ssl();
            false -> kafka_hosts_string()
        end,
    SASLHostsString =
        case UseSSL of
            true -> kafka_hosts_string_ssl_sasl();
            false -> kafka_hosts_string_sasl()
        end,
    BinifyMap = fun(Map) ->
        maps:from_list([
            {erlang:iolist_to_binary(K), erlang:iolist_to_binary(V)}
         || {K, V} <- maps:to_list(Map)
        ])
    end,
    SSLSettings =
        case UseSSL of
            true -> #{<<"ssl">> => BinifyMap(valid_ssl_settings())};
            false -> #{}
        end,
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => NormalHostsString,
                <<"authentication">> => <<"none">>
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_plain_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_scram256_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_scram512_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_kerberos_settings())
            },
            SSLSettings
        )
    ),
    ok.

kafka_bridge_rest_api_helper(Config) ->
    BridgeType = "kafka",
    BridgeName = "my_kafka_bridge",
    BridgeID = emqx_bridge_resource:bridge_id(
        erlang:list_to_binary(BridgeType),
        erlang:list_to_binary(BridgeName)
    ),
    ResourceId = emqx_bridge_resource:resource_id(
        erlang:list_to_binary(BridgeType),
        erlang:list_to_binary(BridgeName)
    ),
    UrlEscColon = "%3A",
    BridgeIdUrlEnc = BridgeType ++ UrlEscColon ++ BridgeName,
    BridgesParts = ["bridges"],
    BridgesPartsIdDeleteAlsoActions = ["bridges", BridgeIdUrlEnc ++ "?also_delete_dep_actions"],
    OpUrlFun = fun(OpName) -> ["bridges", BridgeIdUrlEnc, "operation", OpName] end,
    BridgesPartsOpDisable = OpUrlFun("disable"),
    BridgesPartsOpEnable = OpUrlFun("enable"),
    BridgesPartsOpRestart = OpUrlFun("restart"),
    BridgesPartsOpStop = OpUrlFun("stop"),
    %% List bridges
    MyKafkaBridgeExists = fun() ->
        {ok, _Code, BridgesData} = show(http_get(BridgesParts)),
        Bridges = show(json(BridgesData)),
        lists:any(
            fun
                (#{<<"name">> := <<"my_kafka_bridge">>}) -> true;
                (_) -> false
            end,
            Bridges
        )
    end,
    %% Delete if my_kafka_bridge exists
    case MyKafkaBridgeExists() of
        true ->
            %% Delete the bridge my_kafka_bridge
            {ok, 204, <<>>} = show(http_delete(BridgesPartsIdDeleteAlsoActions));
        false ->
            ok
    end,
    false = MyKafkaBridgeExists(),
    %% Create new Kafka bridge
    KafkaTopic = "test-topic-one-partition",
    CreateBodyTmp = #{
        <<"type">> => <<"kafka">>,
        <<"name">> => <<"my_kafka_bridge">>,
        <<"bootstrap_hosts">> => maps:get(<<"bootstrap_hosts">>, Config),
        <<"enable">> => true,
        <<"authentication">> => maps:get(<<"authentication">>, Config),
        <<"producer">> => #{
            <<"mqtt">> => #{
                topic => <<"t/#">>
            },
            <<"kafka">> => #{
                <<"topic">> => erlang:list_to_binary(KafkaTopic)
            }
        }
    },
    CreateBody =
        case maps:is_key(<<"ssl">>, Config) of
            true -> CreateBodyTmp#{<<"ssl">> => maps:get(<<"ssl">>, Config)};
            false -> CreateBodyTmp
        end,
    {ok, 201, _Data} = show(http_post(BridgesParts, show(CreateBody))),
    %% Check that the new bridge is in the list of bridges
    true = MyKafkaBridgeExists(),
    %% Create a rule that uses the bridge
    {ok, 201, _Rule} = http_post(
        ["rules"],
        #{
            <<"name">> => <<"kafka_bridge_rest_api_helper_rule">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"kafka_bridge_topic/#\"">>
        }
    ),
    %% counters should be empty before
    ?assertEqual(0, emqx_resource_metrics:matched_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:success_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:inflight_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:batching_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:queuing_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_other_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_not_enabled_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_success_get(ResourceId)),
    %% Get offset before sending message
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    %% Send message to topic and check that it got forwarded to Kafka
    Body = <<"message from EMQX">>,
    emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
    %% Give Kafka some time to get message
    timer:sleep(100),
    %% Check that Kafka got message
    BrodOut = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    {ok, {_, [KafkaMsg]}} = show(BrodOut),
    Body = KafkaMsg#kafka_message.value,
    %% Check crucial counters and gauges
    ?assertEqual(1, emqx_resource_metrics:matched_get(ResourceId)),
    ?assertEqual(1, emqx_resource_metrics:success_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:inflight_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:batching_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:queuing_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_other_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_not_enabled_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_success_get(ResourceId)),
    %% Perform operations
    {ok, 200, _} = show(http_post(show(BridgesPartsOpDisable), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpDisable), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpEnable), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpEnable), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpStop), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpStop), #{})),
    {ok, 200, _} = show(http_post(show(BridgesPartsOpRestart), #{})),
    %% Cleanup
    {ok, 204, _} = show(http_delete(BridgesPartsIdDeleteAlsoActions)),
    false = MyKafkaBridgeExists(),
    ok.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

publish_with_and_without_ssl(AuthSettings) ->
    publish_helper(#{
        auth_settings => AuthSettings,
        ssl_settings => #{}
    }),
    publish_helper(#{
        auth_settings => AuthSettings,
        ssl_settings => valid_ssl_settings()
    }),
    ok.

publish_helper(#{
    auth_settings := AuthSettings,
    ssl_settings := SSLSettings
}) ->
    HostsString =
        case {AuthSettings, SSLSettings} of
            {"none", Map} when map_size(Map) =:= 0 ->
                kafka_hosts_string();
            {"none", Map} when map_size(Map) =/= 0 ->
                kafka_hosts_string_ssl();
            {_, Map} when map_size(Map) =:= 0 ->
                kafka_hosts_string_sasl();
            {_, _} ->
                kafka_hosts_string_ssl_sasl()
        end,
    Hash = erlang:phash2([HostsString, AuthSettings, SSLSettings]),
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    InstId = emqx_bridge_resource:resource_id("kafka", Name),
    BridgeId = emqx_bridge_resource:bridge_id("kafka", Name),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId,
        "ssl" => SSLSettings
    }),
    emqx_bridge_resource:create(kafka, erlang:list_to_atom(Name), Conf, #{}),
    %% To make sure we get unique value
    timer:sleep(1),
    Time = erlang:monotonic_time(),
    BinTime = integer_to_binary(Time),
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    StartRes = ?PRODUCER:on_start(InstId, Conf),
    {ok, State} = StartRes,
    OnQueryRes = ?PRODUCER:on_query(InstId, {send_message, Msg}, State),
    ok = OnQueryRes,
    {ok, {_, [KafkaMsg]}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg),
    ok = ?PRODUCER:on_stop(InstId, State),
    ok = emqx_bridge_resource:remove(BridgeId),
    ok.

config(Args) ->
    ConfText = hocon_config(Args),
    ct:pal("Running tests with conf:\n~s", [ConfText]),
    {ok, Conf} = hocon:binary(ConfText),
    #{config := Parsed} = hocon_tconf:check_plain(
        emqx_ee_bridge_kafka,
        #{<<"config">> => Conf},
        #{atom_key => true}
    ),
    InstId = maps:get("instance_id", Args),
    <<"bridge:", BridgeId/binary>> = InstId,
    Parsed#{bridge_name => erlang:element(2, emqx_bridge_resource:parse_bridge_id(BridgeId))}.

hocon_config(Args) ->
    AuthConf = maps:get("authentication", Args),
    AuthTemplate = iolist_to_binary(hocon_config_template_authentication(AuthConf)),
    AuthConfRendered = bbmustache:render(AuthTemplate, AuthConf),
    SSLConf = maps:get("ssl", Args, #{}),
    SSLTemplate = iolist_to_binary(hocon_config_template_ssl(SSLConf)),
    SSLConfRendered = bbmustache:render(SSLTemplate, SSLConf),
    Hocon = bbmustache:render(
        iolist_to_binary(hocon_config_template()),
        Args#{
            "authentication" => AuthConfRendered,
            "ssl" => SSLConfRendered
        }
    ),
    Hocon.

%% erlfmt-ignore
hocon_config_template() ->
"""
bootstrap_hosts = \"{{ kafka_hosts_string }}\"
enable = true
authentication = {{{ authentication }}}
ssl = {{{ ssl }}}
producer = {
    mqtt {
       topic = \"t/#\"
    }
    kafka = {
        topic = \"{{ kafka_topic }}\"
    }
}
""".

%% erlfmt-ignore
hocon_config_template_authentication("none") ->
    "none";
hocon_config_template_authentication(#{"mechanism" := _}) ->
"""
{
    mechanism = {{ mechanism }}
    password = {{ password }}
    username = {{ username }}
}
""";
hocon_config_template_authentication(#{"kerberos_principal" := _}) ->
"""
{
    kerberos_principal = \"{{ kerberos_principal }}\"
    kerberos_keytab_file = \"{{ kerberos_keytab_file }}\"
}
""".

%% erlfmt-ignore
hocon_config_template_ssl(Map) when map_size(Map) =:= 0 ->
"""
{
    enable = false
}
""";
hocon_config_template_ssl(_) ->
"""
{
    enable = true
    cacertfile = \"{{{cacertfile}}}\"
    certfile = \"{{{certfile}}}\"
    keyfile = \"{{{keyfile}}}\"
}
""".

kafka_hosts_string() ->
    "kafka-1.emqx.net:9092,".

kafka_hosts_string_sasl() ->
    "kafka-1.emqx.net:9093,".

kafka_hosts_string_ssl() ->
    "kafka-1.emqx.net:9094,".

kafka_hosts_string_ssl_sasl() ->
    "kafka-1.emqx.net:9095,".

valid_ssl_settings() ->
    #{
        "cacertfile" => <<"/var/lib/secret/ca.crt">>,
        "certfile" => <<"/var/lib/secret/client.crt">>,
        "keyfile" => <<"/var/lib/secret/client.key">>,
        "enable" => <<"true">>
    }.

valid_sasl_plain_settings() ->
    #{
        "mechanism" => "plain",
        "username" => "emqxuser",
        "password" => "password"
    }.

valid_sasl_scram256_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_256"
    }.

valid_sasl_scram512_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_512"
    }.

valid_sasl_kerberos_settings() ->
    #{
        "kerberos_principal" => "rig@KDC.EMQX.NET",
        "kerberos_keytab_file" => "/var/lib/secret/rig.keytab"
    }.

kafka_hosts() ->
    kpro:parse_endpoints(kafka_hosts_string()).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).

%%------------------------------------------------------------------------------
%% Internal functions rest API helpers
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

random_num() ->
    erlang:system_time(nanosecond).

http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_()).

http_delete(Parts) ->
    request_api(delete, api_path(Parts), auth_header_()).

http_post(Parts, Body) ->
    request_api(post, api_path(Parts), [], auth_header_(), Body).

http_put(Parts, Body) ->
    request_api(put, api_path(Parts), [], auth_header_(), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).
do_request_dashboard(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    {ok, Jsx} = emqx_json:safe_decode(Data, [return_maps]),
    Jsx.
