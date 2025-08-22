%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_schema_testlib).

-compile(nowarn_export_all).
-compile(export_all).

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
