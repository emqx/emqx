%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_dameng).

-behaviour(emqx_connector_examples).

-include("emqx_bridge_dameng.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, dameng).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload) "
    "values ( ${id}, ${topic}, ${qos}, ${payload} )"
>>).

-define(DEFAULT_SERVER, <<"127.0.0.1">>).

%% -------------------------------------------------------------------------------------------------
%% api.

%% ====================
%% Bridge V2: Connector + Action

connector_examples(Method) ->
    [
        #{
            <<"dameng">> =>
                #{
                    summary => <<"Dameng DM8 Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        server => ?DEFAULT_SERVER,
        port => ?DAMENG_DEFAULT_PORT,
        pool_size => 8,
        username => ?DAMENG_DEFAULT_USERNAME,
        password => <<"******">>,
        driver => ?DAMENG_DEFAULT_DRIVER,
        charset => ?DAMENG_DEFAULT_CHARSET,
        resource_opts => #{health_check_interval => <<"20s">>}
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"dameng">> =>
                #{
                    summary => <<"Dameng DM8 Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        <<"parameters">> =>
            #{<<"sql">> => ?DEFAULT_SQL}
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

namespace() ->
    "bridge_dameng".

roots() ->
    [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(dameng_action));
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        fields("config_connector") -- emqx_connector_schema:common_fields()
    );
fields("config_connector") ->
    common_fields() ++
        emqx_bridge_dameng_connector:fields(config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ] ++ emqx_bridge_dameng_connector:fields(config);
fields(action) ->
    {?ACTION_TYPE,
        mk(
            hoconsc:map(name, ref(?MODULE, dameng_action)),
            #{desc => ?DESC("dameng_action"), required => false}
        )};
fields(dameng_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{required => true, desc => ?DESC(action_parameters)}
        ),
        #{resource_opts_ref => ref(?MODULE, action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {sql,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 100}},
        {batch_time, #{default => <<"100ms">>}}
    ]);
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    fields("post", dameng);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_v2_api:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

common_fields() ->
    [] ++ emqx_connector_schema:common_fields().

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Dameng DM8 using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc("config_connector") ->
    ?DESC("config_connector");
desc(dameng_action) ->
    ?DESC("dameng_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(action_resource_opts) ->
    emqx_bridge_v2_schema:desc(action_resource_opts);
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
