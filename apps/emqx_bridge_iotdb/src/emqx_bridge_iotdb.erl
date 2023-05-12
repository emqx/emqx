%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb).

-include("emqx_bridge_iotdb.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% emqx_ee_bridge "unofficial" API
-export([conn_bridge_examples/1]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "bridge_iotdb".

roots() -> [].

fields("config") ->
    basic_config() ++ request_config();
fields("post") ->
    [
        type_field(),
        name_field()
    ] ++ fields("config");
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
fields("creation_opts") ->
    lists:filter(
        fun({K, _V}) ->
            not lists:member(K, unsupported_opts())
        end,
        emqx_resource_schema:fields("creation_opts")
    );
fields(auth_basic) ->
    [
        {username, mk(binary(), #{required => true, desc => ?DESC("config_auth_basic_username")})},
        {password,
            mk(binary(), #{
                required => true,
                desc => ?DESC("config_auth_basic_password"),
                format => <<"password">>,
                sensitive => true,
                converter => fun emqx_schema:password_converter/2
            })}
    ].

desc("config") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("post") ->
    ["Configuration for IoTDB using `POST` method."];
desc(Name) ->
    lists:member(Name, struct_names()) orelse throw({missing_desc, Name}),
    ?DESC(Name).

struct_names() ->
    [
        auth_basic
    ].

basic_config() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_enable"),
                    default => true
                }
            )},
        {authentication,
            mk(
                hoconsc:union([ref(?MODULE, auth_basic)]),
                #{
                    default => auth_basic, desc => ?DESC("config_authentication")
                }
            )},
        {is_aligned,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_is_aligned"),
                    default => false
                }
            )},
        {device_id,
            mk(
                binary(),
                #{
                    desc => ?DESC("config_device_id")
                }
            )},
        {iotdb_version,
            mk(
                hoconsc:enum([?VSN_1_0_X, ?VSN_0_13_X]),
                #{
                    desc => ?DESC("config_iotdb_version"),
                    default => ?VSN_1_0_X
                }
            )}
    ] ++ resource_creation_opts() ++
        proplists_without(
            [max_retries, base_url, request],
            emqx_connector_http:fields(config)
        ).

proplists_without(Keys, List) ->
    [El || El = {K, _} <- List, not lists:member(K, Keys)].

request_config() ->
    [
        {base_url,
            mk(
                emqx_schema:url(),
                #{
                    desc => ?DESC("config_base_url")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    default => 2,
                    desc => ?DESC("config_max_retries")
                }
            )},
        {request_timeout,
            mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("config_request_timeout")
                }
            )}
    ].

resource_creation_opts() ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ].

unsupported_opts() ->
    [
        batch_size,
        batch_time
    ].

%%======================================================================================

type_field() ->
    {type,
        mk(
            hoconsc:enum([iotdb]),
            #{
                required => true,
                desc => ?DESC("desc_type")
            }
        )}.

name_field() ->
    {name,
        mk(
            binary(),
            #{
                required => true,
                desc => ?DESC("desc_name")
            }
        )}.

%%======================================================================================

conn_bridge_examples(Method) ->
    [
        #{
            <<"iotdb">> =>
                #{
                    summary => <<"Apache IoTDB Bridge">>,
                    value => conn_bridge_example(Method, iotdb)
                }
        }
    ].

conn_bridge_example(_Method, Type) ->
    #{
        name => <<"My IoTDB Bridge">>,
        type => Type,
        enable => true,
        authentication => #{
            <<"username">> => <<"root">>,
            <<"password">> => <<"*****">>
        },
        is_aligned => false,
        device_id => <<"my_device">>,
        base_url => <<"http://iotdb.local:18080/">>,
        iotdb_version => ?VSN_1_0_X,
        connect_timeout => <<"15s">>,
        pool_type => <<"random">>,
        pool_size => 8,
        enable_pipelining => 100,
        ssl => #{enable => false},
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            auto_restart_interval => ?AUTO_RESTART_INTERVAL_RAW,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.
