%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_enterprise).

-if(?EMQX_RELEASE_EDITION == ee).

-export([
    resource_type/1,
    connector_impl_module/1
]).

-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    fields/1
]).

fields(connectors) ->
    kafka_structs().

kafka_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, "config")),
                #{
                    desc => <<"Kafka Connector Config">>,
                    required => false
                }
            )}
    ].

resource_type(Type) when is_binary(Type) -> resource_type(binary_to_atom(Type, utf8));
resource_type(kafka) -> emqx_bridge_kafka_impl_producer.

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(_ConnectorType) ->
    undefined.

-else.

-endif.
