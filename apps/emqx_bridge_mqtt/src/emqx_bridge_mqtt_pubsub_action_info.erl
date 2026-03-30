%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_pubsub_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    is_source/0,
    action_convert_from_connector/2
]).

action_type_name() -> mqtt.

connector_type_name() -> mqtt.

schema_module() -> emqx_bridge_mqtt_pubsub_schema.

is_source() -> true.

action_convert_from_connector(
    #{<<"proto_ver">> := ProtoVer},
    #{<<"parameters">> := #{<<"topic">> := <<"$queue/", _/binary>>}}
) when ProtoVer =/= <<"v5">> ->
    throw(#{
        kind => validation_error,
        reason => <<"queue subscriptions require connector proto_ver = v5">>
    });
action_convert_from_connector(_ConnectorConfig, SourceConfig) ->
    SourceConfig.
