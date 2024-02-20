%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_connector_schema).

-export([namespace/0, roots/0, fields/1, desc/1]).
-export([connector_examples/1, connector_example_values/0]).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(TYPE, pulsar).

namespace() -> ?TYPE.

roots() -> [].

fields("config_connector") ->
    lists:keydelete(enable, 1, emqx_bridge_schema:common_bridge_fields()) ++
        emqx_bridge_pulsar:fields(config) ++
        emqx_bridge_pulsar:fields(producer_opts) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("post") ->
    emqx_connector_schema:type_and_name_fields(?TYPE) ++ fields("config_connector");
fields("put") ->
    fields("config_connector");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("config_connector").

desc("config_connector") ->
    ?DESC(emqx_bridge_pulsar, "config_connector");
desc(connector_resource_opts) ->
    ?DESC(emqx_bridge_pulsar, connector_resource_opts);
desc(_) ->
    undefined.

connector_examples(Method) ->
    [
        #{
            <<"pulsar">> =>
                #{
                    summary => <<"Pulsar Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?TYPE, connector_example_values()
                    )
                }
        }
    ].

connector_example_values() ->
    #{
        name => <<"pulsar_connector">>,
        type => ?TYPE,
        enable => true,
        servers => <<"pulsar://127.0.0.1:6650">>,
        authentication => none,
        connect_timeout => <<"5s">>,
        batch_size => 10,
        compression => no_compression,
        send_buffer => <<"1MB">>,
        retention_period => <<"100s">>,
        max_batch_bytes => <<"32MB">>,
        pulsar_topic => <<"test_topic">>,
        strategy => random,
        buffer => #{mode => memory},
        ssl => #{enable => false}
    }.
