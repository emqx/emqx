%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_influxdb).

-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-type write_syntax() :: list().
-reflect_type([write_syntax/0]).
-typerefl_from_string({write_syntax/0, ?MODULE, to_influx_lines}).
-export([to_influx_lines/1]).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"influxdb_udp">> => #{
                summary => <<"InfluxDB UDP Bridge">>,
                value => values("influxdb_udp", Method)
            }
        },
        #{
            <<"influxdb_api_v1">> => #{
                summary => <<"InfluxDB HTTP API V1 Bridge">>,
                value => values("influxdb_api_v1", Method)
            }
        },
        #{
            <<"influxdb_api_v2">> => #{
                summary => <<"InfluxDB HTTP API V2 Bridge">>,
                value => values("influxdb_api_v2", Method)
            }
        }
    ].

values(Protocol, get) ->
    maps:merge(values(Protocol, post), ?METRICS_EXAMPLE);
values("influxdb_api_v2", post) ->
    SupportUint = <<"uint_value=${payload.uint_key}u,">>,
    TypeOpts = #{
        bucket => <<"example_bucket">>,
        org => <<"examlpe_org">>,
        token => <<"example_token">>,
        server => <<"127.0.0.1:8086">>
    },
    values(common, "influxdb_api_v2", SupportUint, TypeOpts);
values("influxdb_api_v1", post) ->
    SupportUint = <<>>,
    TypeOpts = #{
        database => <<"example_database">>,
        username => <<"example_username">>,
        password => <<"examlpe_password">>,
        server => <<"127.0.0.1:8086">>
    },
    values(common, "influxdb_api_v1", SupportUint, TypeOpts);
values("influxdb_udp", post) ->
    SupportUint = <<>>,
    TypeOpts = #{
        server => <<"127.0.0.1:8089">>
    },
    values(common, "influxdb_udp", SupportUint, TypeOpts);
values(Protocol, put) ->
    values(Protocol, post).

values(common, Protocol, SupportUint, TypeOpts) ->
    CommonConfigs = #{
        type => list_to_atom(Protocol),
        name => <<"demo">>,
        enable => true,
        local_topic => <<"local/topic/#">>,
        write_syntax =>
            <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
                "${clientid}_int_value=${payload.int_key}i,", SupportUint/binary,
                "bool=${payload.bool}">>,
        precision => ms,
        resource_opts => #{
            enable_batch => false,
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        server => <<"127.0.0.1:8086">>,
        ssl => #{enable => false}
    },
    maps:merge(TypeOpts, CommonConfigs).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_influxdb".

roots() -> [].

fields("post_udp") ->
    method_fileds(post, influxdb_udp);
fields("post_api_v1") ->
    method_fileds(post, influxdb_api_v1);
fields("post_api_v2") ->
    method_fileds(post, influxdb_api_v2);
fields("put_udp") ->
    method_fileds(put, influxdb_udp);
fields("put_api_v1") ->
    method_fileds(put, influxdb_api_v1);
fields("put_api_v2") ->
    method_fileds(put, influxdb_api_v2);
fields("get_udp") ->
    method_fileds(get, influxdb_udp);
fields("get_api_v1") ->
    method_fileds(get, influxdb_api_v1);
fields("get_api_v2") ->
    method_fileds(get, influxdb_api_v2);
fields(Type) when
    Type == influxdb_udp orelse Type == influxdb_api_v1 orelse Type == influxdb_api_v2
->
    influxdb_bridge_common_fields() ++
        connector_fields(Type).

method_fileds(post, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType);
method_fileds(get, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType) ++
        emqx_bridge_schema:metrics_status_fields();
method_fileds(put, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType).

influxdb_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {write_syntax, fun write_syntax/1}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields(Type) ->
    emqx_ee_connector_influxdb:fields(Type).

type_name_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for InfluxDB using `", string:to_upper(Method), "` method."];
desc(influxdb_udp) ->
    ?DESC(emqx_ee_connector_influxdb, "influxdb_udp");
desc(influxdb_api_v1) ->
    ?DESC(emqx_ee_connector_influxdb, "influxdb_api_v1");
desc(influxdb_api_v2) ->
    ?DESC(emqx_ee_connector_influxdb, "influxdb_api_v2");
desc(_) ->
    undefined.

write_syntax(type) ->
    ?MODULE:write_syntax();
write_syntax(required) ->
    true;
write_syntax(validator) ->
    [?NOT_EMPTY("the value of the field 'write_syntax' cannot be empty")];
write_syntax(converter) ->
    fun to_influx_lines/1;
write_syntax(desc) ->
    ?DESC("write_syntax");
write_syntax(format) ->
    <<"sql">>;
write_syntax(_) ->
    undefined.

to_influx_lines(RawLines) ->
    Lines = string:tokens(str(RawLines), "\n"),
    lists:reverse(lists:foldl(fun converter_influx_line/2, [], Lines)).

converter_influx_line(Line, AccIn) ->
    case string:tokens(str(Line), " ") of
        [MeasurementAndTags, Fields, Timestamp] ->
            {Measurement, Tags} = split_measurement_and_tags(MeasurementAndTags),
            [
                #{
                    measurement => Measurement,
                    tags => kv_pairs(Tags),
                    fields => kv_pairs(string:tokens(Fields, ",")),
                    timestamp => Timestamp
                }
                | AccIn
            ];
        [MeasurementAndTags, Fields] ->
            {Measurement, Tags} = split_measurement_and_tags(MeasurementAndTags),
            %% TODO: fix here both here and influxdb driver.
            %% Default value should evaluated by InfluxDB.
            [
                #{
                    measurement => Measurement,
                    tags => kv_pairs(Tags),
                    fields => kv_pairs(string:tokens(Fields, ",")),
                    timestamp => "${timestamp}"
                }
                | AccIn
            ];
        _ ->
            throw("Bad InfluxDB Line Protocol schema")
    end.

split_measurement_and_tags(Subject) ->
    case string:tokens(Subject, ",") of
        [] ->
            throw("Bad Measurement schema");
        [Measurement] ->
            {Measurement, []};
        [Measurement | Tags] ->
            {Measurement, Tags}
    end.

kv_pairs(Pairs) ->
    kv_pairs(Pairs, []).
kv_pairs([], Acc) ->
    lists:reverse(Acc);
kv_pairs([Pair | Rest], Acc) ->
    case string:tokens(Pair, "=") of
        [K, V] ->
            %% Reduplicated keys will be overwritten. Follows InfluxDB Line Protocol.
            kv_pairs(Rest, [{K, V} | Acc]);
        _ ->
            throw(io_lib:format("Bad InfluxDB Line Protocol Key Value pair: ~p", Pair))
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
