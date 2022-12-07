%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, rs},
        {group, sharded},
        {group, single}
        | (emqx_common_test_helpers:all(?MODULE) -- group_tests())
    ].

group_tests() ->
    [
        t_setup_via_config_and_publish,
        t_setup_via_http_api_and_publish
    ].

groups() ->
    [
        {rs, group_tests()},
        {sharded, group_tests()},
        {single, group_tests()}
    ].

init_per_group(Type = rs, Config) ->
    MongoHost = os:getenv("MONGO_RS_HOST", "mongo1"),
    MongoPort = list_to_integer(os:getenv("MONGO_RS_PORT", "27017")),
    case emqx_common_test_helpers:is_tcp_server_available(MongoHost, MongoPort) of
        true ->
            ok = start_apps(),
            emqx_mgmt_api_test_util:init_suite(),
            {Name, MongoConfig} = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig},
                {mongo_type, Type},
                {mongo_name, Name}
                | Config
            ];
        false ->
            {skip, no_mongo}
    end;
init_per_group(Type = sharded, Config) ->
    MongoHost = os:getenv("MONGO_SHARDED_HOST", "mongosharded3"),
    MongoPort = list_to_integer(os:getenv("MONGO_SHARDED_PORT", "27017")),
    case emqx_common_test_helpers:is_tcp_server_available(MongoHost, MongoPort) of
        true ->
            ok = start_apps(),
            emqx_mgmt_api_test_util:init_suite(),
            {Name, MongoConfig} = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig},
                {mongo_type, Type},
                {mongo_name, Name}
                | Config
            ];
        false ->
            {skip, no_mongo}
    end;
init_per_group(Type = single, Config) ->
    MongoHost = os:getenv("MONGO_SINGLE_HOST", "mongo"),
    MongoPort = list_to_integer(os:getenv("MONGO_SINGLE_PORT", "27017")),
    case emqx_common_test_helpers:is_tcp_server_available(MongoHost, MongoPort) of
        true ->
            ok = start_apps(),
            emqx_mgmt_api_test_util:init_suite(),
            {Name, MongoConfig} = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig},
                {mongo_type, Type},
                {mongo_name, Name}
                | Config
            ];
        false ->
            {skip, no_mongo}
    end.

end_per_group(_Type, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    catch clear_db(Config),
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    catch clear_db(Config),
    delete_bridge(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

start_apps() ->
    ensure_loaded(),
    %% some configs in emqx_conf app are mandatory,
    %% we want to make sure they are loaded before
    %% ekka start in emqx_common_test_helpers:start_apps/1
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]).

ensure_loaded() ->
    _ = application:load(emqx_ee_bridge),
    _ = emqx_ee_bridge:module_info(),
    ok.

mongo_type_bin(rs) ->
    <<"mongodb_rs">>;
mongo_type_bin(sharded) ->
    <<"mongodb_sharded">>;
mongo_type_bin(single) ->
    <<"mongodb_single">>.

mongo_config(MongoHost, MongoPort0, rs = Type) ->
    MongoPort = integer_to_list(MongoPort0),
    Servers = MongoHost ++ ":" ++ MongoPort,
    Name = atom_to_binary(?MODULE),
    ConfigString =
        io_lib:format(
            "bridges.mongodb_rs.~s {\n"
            "  enable = true\n"
            "  collection = mycol\n"
            "  replica_set_name = rs0\n"
            "  servers = [~p]\n"
            "  w_mode = safe\n"
            "  database = mqtt\n"
            "}",
            [Name, Servers]
        ),
    {Name, parse_and_check(ConfigString, Type, Name)};
mongo_config(MongoHost, MongoPort0, sharded = Type) ->
    MongoPort = integer_to_list(MongoPort0),
    Servers = MongoHost ++ ":" ++ MongoPort,
    Name = atom_to_binary(?MODULE),
    ConfigString =
        io_lib:format(
            "bridges.mongodb_sharded.~s {\n"
            "  enable = true\n"
            "  collection = mycol\n"
            "  servers = [~p]\n"
            "  w_mode = safe\n"
            "  database = mqtt\n"
            "}",
            [Name, Servers]
        ),
    {Name, parse_and_check(ConfigString, Type, Name)};
mongo_config(MongoHost, MongoPort0, single = Type) ->
    MongoPort = integer_to_list(MongoPort0),
    Server = MongoHost ++ ":" ++ MongoPort,
    Name = atom_to_binary(?MODULE),
    ConfigString =
        io_lib:format(
            "bridges.mongodb_single.~s {\n"
            "  enable = true\n"
            "  collection = mycol\n"
            "  server = ~p\n"
            "  w_mode = safe\n"
            "  database = mqtt\n"
            "}",
            [Name, Server]
        ),
    {Name, parse_and_check(ConfigString, Type, Name)}.

parse_and_check(ConfigString, Type, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = mongo_type_bin(Type),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    Type = mongo_type_bin(?config(mongo_type, Config)),
    Name = ?config(mongo_name, Config),
    MongoConfig = ?config(mongo_config, Config),
    emqx_bridge:create(Type, Name, MongoConfig).

delete_bridge(Config) ->
    Type = mongo_type_bin(?config(mongo_type, Config)),
    Name = ?config(mongo_name, Config),
    emqx_bridge:remove(Type, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

clear_db(Config) ->
    Type = mongo_type_bin(?config(mongo_type, Config)),
    Name = ?config(mongo_name, Config),
    #{<<"collection">> := Collection} = ?config(mongo_config, Config),
    ResourceID = emqx_bridge_resource:resource_id(Type, Name),
    {ok, _, #{state := #{poolname := PoolName}}} = emqx_resource:get_instance(ResourceID),
    Selector = #{},
    {true, _} = ecpool:pick_and_do(
        PoolName, {mongo_api, delete, [Collection, Selector]}, no_handover
    ),
    ok.

find_all(Config) ->
    Type = mongo_type_bin(?config(mongo_type, Config)),
    Name = ?config(mongo_name, Config),
    #{<<"collection">> := Collection} = ?config(mongo_config, Config),
    ResourceID = emqx_bridge_resource:resource_id(Type, Name),
    emqx_resource:query(ResourceID, {find, Collection, #{}, #{}}).

send_message(Config, Payload) ->
    Name = ?config(mongo_name, Config),
    Type = mongo_type_bin(?config(mongo_type, Config)),
    BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
    emqx_bridge:send_message(BridgeID, Payload).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = erlang:unique_integer(),
    ok = send_message(Config, #{key => Val}),
    ?assertMatch(
        {ok, [#{<<"key">> := Val}]},
        find_all(Config)
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    Type = mongo_type_bin(?config(mongo_type, Config)),
    Name = ?config(mongo_name, Config),
    MongoConfig0 = ?config(mongo_config, Config),
    MongoConfig = MongoConfig0#{
        <<"name">> => Name,
        <<"type">> => Type
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(MongoConfig)
    ),
    Val = erlang:unique_integer(),
    ok = send_message(Config, #{key => Val}),
    ?assertMatch(
        {ok, [#{<<"key">> := Val}]},
        find_all(Config)
    ),
    ok.
