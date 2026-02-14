%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_schema_testlib).

-compile(nowarn_export_all).
-compile(export_all).

tls_opts() ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    #{
        <<"enable">> => true,
        <<"keyfile">> => bin(filename:join([CertsPath, "client-key.pem"])),
        <<"certfile">> => bin(filename:join([CertsPath, "client-cert.pem"])),
        <<"cacertfile">> => bin(filename:join([CertsPath, "cacert.pem"]))
    }.

bin(X) -> emqx_utils_conv:bin(X).

http_connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"url">> => <<"please override">>,
        <<"connect_timeout">> => <<"15s">>,
        <<"headers">> => #{<<"content-type">> => <<"application/json">>},
        <<"max_inactive">> => <<"10s">>,
        <<"pool_size">> => 1,
        <<"pool_type">> => <<"random">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"http">>, <<"x">>, InnerConfigMap).

http_action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"path">> => <<"/path">>,
            <<"method">> => <<"post">>,
            <<"headers">> => #{<<"headerk">> => <<"headerv">>},
            <<"body">> => <<"${.}">>,
            <<"max_retries">> => 2
        },
        <<"resource_opts">> =>
            maps:without(
                [<<"batch_size">>, <<"batch_time">>],
                emqx_bridge_v2_testlib:common_action_resource_opts()
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, <<"http">>, <<"x">>, InnerConfigMap).

mqtt_connector_config(Overrides) ->
    mqtt_connector_config(_Type = <<"mqtt">>, Overrides).

mqtt_connector_config(Type, Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"clean_start">> => true,
        <<"connect_timeout">> => <<"5s">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(Type, <<"x">>, InnerConfigMap).

mqtt_action_config(Overrides) ->
    Defaults =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => <<"remote/topic">>,
                    <<"qos">> => 2
                },
            <<"resource_opts">> =>
                maps:without(
                    [<<"batch_time">>, <<"batch_size">>],
                    emqx_bridge_v2_testlib:common_action_resource_opts()
                )
        },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, <<"mqtt">>, <<"x">>, InnerConfigMap).

mqtt_source_config(Overrides) ->
    Defaults =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => <<"remote/topic">>,
                    <<"qos">> => 2
                },
            <<"resource_opts">> => emqx_bridge_v2_testlib:common_source_resource_opts()
        },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, <<"mqtt">>, <<"x">>, InnerConfigMap).

greptimedb_connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => <<"toxiproxy:4001">>,
        <<"dbname">> => <<"public">>,
        <<"username">> => <<"greptime_user">>,
        <<"password">> => <<"greptime_pwd">>,
        <<"ttl">> => <<"3 years">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"greptimedb">>, <<"x">>, InnerConfigMap).

greptimedb_action_config(Overrides) ->
    %% N.B.: this single space character is relevant
    %% the measurement name should not be the topic
    %% from greptimedb:
    %% error => {error,{case_clause,{error,{<<"3">>,<<"Invalid%20table%20name:%20test/greptimedb">
    WriteSyntax =
        <<"mqtt,clientid=${clientid}", " ", "payload=${payload},",
            "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "float_value=${payload.float_key},", "undef_value=${payload.undef},",
            "${undef_key}=\"hard-coded-value\",", "bool=${payload.bool}">>,
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"precision">> => <<"ns">>,
            <<"write_syntax">> => WriteSyntax
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, <<"greptimedb">>, <<"x">>, InnerConfigMap).
