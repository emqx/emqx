%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_sparkplug_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-include("emqx_sparkplug.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    WorkDir = emqx_cth_suite:work_dir(TCConfig),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_ctl,
            emqx_retainer,
            emqx_schema_registry_testlib:emqx_schema_registry_app_spec(),
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}}
        ],
        #{work_dir => WorkDir}
    ),
    try
        Package = plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {apps, Apps},
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | TCConfig
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(TCConfig) ->
    {apps, Apps} = lists:keyfind(apps, 1, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    ok = cleanup_plugin(TCConfig),
    ok = install_and_start_plugin(TCConfig),
    ok = emqx_retainer:clean(),
    TCConfig.

end_per_testcase(_TestCase, TCConfig) ->
    ok = cleanup_plugin(TCConfig),
    ok = emqx_retainer:clean(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

plugin_package() ->
    Root = emqx_common_test_helpers:proj_root(),
    Vsn = string:trim(read_file(filename:join([Root, "plugins", "emqx_sparkplug", "VERSION"]))),
    Package = filename:join([Root, "_build", "plugins", "emqx_sparkplug-" ++ Vsn ++ ".tar.gz"]),
    _ = file:delete(Package),
    build_in_tree_plugin_package(Root, Package).

build_in_tree_plugin_package(Root, Package) ->
    Output = os:cmd(
        "cd " ++ Root ++
            " && PROFILE=emqx-enterprise make plugin-emqx_sparkplug 2>&1"
    ),
    case filelib:is_regular(Package) of
        true ->
            Package;
        false ->
            error({plugin_package_build_failed, Package, Output})
    end.

read_file(Path) ->
    {ok, Bin} = file:read_file(Path),
    binary_to_list(Bin).

install_and_start_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    ok = emqx_plugins:write_package(NameVsn, PackageBin),
    ok = emqx_plugins:allow_installation(
        NameVsn,
        binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase)
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok.

cleanup_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    case emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => false}) of
        {ok, _Plugin} ->
            _ = emqx_plugins:ensure_stopped(NameVsn),
            _ = emqx_plugins:ensure_disabled(NameVsn),
            _ = emqx_plugins:ensure_uninstalled(NameVsn);
        {error, _Reason} ->
            ok
    end,
    _ = emqx_plugins:delete_package(NameVsn),
    _ = emqx_plugins:forget_allowed_installation(NameVsn),
    ok.

fmt(FmtStr, Context) -> emqx_bridge_v2_testlib:fmt(FmtStr, Context).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Defaults = #{proto_ver => v5},
    Opts = maps:merge(Defaults, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(C),
    C.

spb_encode(Payload) ->
    emqx_schema_registry_serde:rsf_spb_encode([Payload]).

publish_nbirth(C, Payload) ->
    publish_nbirth(C, Payload, _Opts = #{}).

publish_nbirth(C, Payload0, Opts) ->
    publish_spb_msg(C, node, <<"NBIRTH">>, Payload0, Opts).

publish_dbirth(C, Payload) ->
    publish_dbirth(C, Payload, _Opts = #{}).

publish_dbirth(C, Payload0, Opts) ->
    publish_spb_msg(C, device, <<"DBIRTH">>, Payload0, Opts).

publish_spb_msg(C, NodeOrDevice, MsgType, Payload0, Opts) ->
    Topic = spb_topic(NodeOrDevice, MsgType, Opts),
    Payload = spb_encode(Payload0),
    emqtt:publish(C, Topic, Payload),
    ok.

spb_opts(Opts) ->
    Defaults = #{
        namespace => <<"spBv1.0">>,
        group_id => <<"group_id0">>,
        edge_node_id => <<"eon_id0">>,
        device_id => <<"dev_id0">>
    },
    maps:merge(Defaults, Opts).

nbirth_topic() ->
    nbirth_topic(_Opts = #{}).

nbirth_topic(Opts) ->
    spb_topic(node, <<"NBIRTH">>, Opts).

dbirth_topic() ->
    dbirth_topic(_Opts = #{}).

dbirth_topic(Opts) ->
    spb_topic(device, <<"DBIRTH">>, Opts).

spb_topic(NodeOrDevice, MsgType, Opts) ->
    #{
        namespace := Namespace,
        group_id := GroupId,
        edge_node_id := EdgeNodeId,
        device_id := DeviceId
    } = spb_opts(Opts),
    Fmt =
        case NodeOrDevice of
            node -> <<"${ns}/${gid}/${mt}/${enid}">>;
            device -> <<"${ns}/${gid}/${mt}/${enid}/${did}">>
        end,
    fmt(Fmt, #{
        mt => MsgType,
        ns => Namespace,
        gid => GroupId,
        enid => EdgeNodeId,
        did => DeviceId
    }).

sample_birth_payload1() ->
    #{
        <<"metrics">> =>
            [
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 424,
                    <<"name">> => <<"non_aliased_metric1">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 84,
                    <<"name">> => <<"aliased_metric1">>,
                    <<"alias">> => 1,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 2,
                    <<"int_value">> => 42,
                    <<"name">> => <<"non_aliased_metric2">>,
                    <<"timestamp">> => 1678094561525
                },
                #{
                    <<"datatype">> => 5,
                    <<"int_value">> => 1,
                    <<"name">> => <<"aliased_metric2">>,
                    <<"alias">> => 2,
                    <<"timestamp">> => 1678094561525
                }
            ],
        <<"seq">> => 88,
        <<"timestamp">> => 1678094561521
    }.

all_certificates_topic() ->
    emqx_topic:join([?SPB_CERT_PREFIX, ~"#"]).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Smoke test for keeping track of published `NBIRTH` and `DBIRTH` messages and making them
available under `$sparkplug/certificates`.
""".
t_00_smoke(_) ->
    S = start_client(#{clientid => ~"sub", clean_start => true}),
    P1 = start_client(#{clientid => ~"pub1", clean_start => true}),
    P2 = start_client(#{clientid => ~"pub2", clean_start => true}),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(S, all_certificates_topic(), [{qos, 1}]),
    BirthBin = spb_encode(sample_birth_payload1()),
    NBirthTopic1 = nbirth_topic(),
    NBirthTopic2 = nbirth_topic(#{edge_node_id => ~"eon_id2"}),
    DBirthTopic1 = dbirth_topic(),
    DBirthTopic2 = dbirth_topic(#{edge_node_id => ~"eon_id2", device_id => ~"dev_id2"}),
    {ok, _} = emqtt:publish(P1, NBirthTopic1, BirthBin, [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            payload := BirthBin,
            topic := <<"$sparkplug/certificates/", NBirthTopic1/binary>>,
            %% received in real time
            retain := false,
            qos := 1
        }}
    ),
    {ok, _} = emqtt:publish(P2, NBirthTopic2, BirthBin, [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            payload := BirthBin,
            topic := <<"$sparkplug/certificates/", NBirthTopic2/binary>>,
            %% received in real time
            retain := false,
            qos := 1
        }}
    ),
    {ok, _} = emqtt:publish(P1, DBirthTopic1, BirthBin, [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            payload := BirthBin,
            topic := <<"$sparkplug/certificates/", DBirthTopic1/binary>>,
            %% received in real time
            retain := false,
            qos := 1
        }}
    ),
    {ok, _} = emqtt:publish(P2, DBirthTopic2, BirthBin, [{qos, 1}]),
    ?assertReceive(
        {publish, #{
            payload := BirthBin,
            topic := <<"$sparkplug/certificates/", DBirthTopic2/binary>>,
            %% received in real time
            retain := false,
            qos := 1
        }}
    ),
    emqtt:stop(P1),
    emqtt:stop(P2),
    emqtt:stop(S),
    %% receive retained birth messages
    lists:foreach(
        fun(QoS) ->
            S1 = start_client(#{clientid => ~"sub1", clean_start => true}),
            {ok, _, [_]} = emqtt:subscribe(S1, all_certificates_topic(), [{qos, QoS}]),
            ?assertReceive(
                {publish, #{
                    payload := BirthBin,
                    topic := <<"$sparkplug/certificates/", NBirthTopic1/binary>>,
                    retain := true,
                    qos := QoS
                }}
            ),
            ?assertReceive(
                {publish, #{
                    payload := BirthBin,
                    topic := <<"$sparkplug/certificates/", NBirthTopic2/binary>>,
                    retain := true,
                    qos := QoS
                }}
            ),
            ?assertReceive(
                {publish, #{
                    payload := BirthBin,
                    topic := <<"$sparkplug/certificates/", DBirthTopic1/binary>>,
                    retain := true,
                    qos := QoS
                }}
            ),
            ?assertReceive(
                {publish, #{
                    payload := BirthBin,
                    topic := <<"$sparkplug/certificates/", DBirthTopic2/binary>>,
                    retain := true,
                    qos := QoS
                }}
            ),
            emqtt:stop(S1)
        end,
        lists:seq(0, 2)
    ),
    ok.
