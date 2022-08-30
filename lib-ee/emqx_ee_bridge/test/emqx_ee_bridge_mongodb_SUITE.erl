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
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            MongoConfig = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig}
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
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            MongoConfig = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig}
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
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            MongoConfig = mongo_config(MongoHost, MongoPort, Type),
            [
                {mongo_host, MongoHost},
                {mongo_port, MongoPort},
                {mongo_config, MongoConfig}
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

mongo_config(MongoHost0, MongoPort0, rs) ->
    MongoHost = list_to_binary(MongoHost0),
    MongoPort = integer_to_binary(MongoPort0),
    Servers = <<MongoHost/binary, ":", MongoPort/binary>>,
    Name = atom_to_binary(?MODULE),
    #{
        <<"type">> => <<"mongodb_rs">>,
        <<"name">> => Name,
        <<"enable">> => true,
        <<"collection">> => <<"mycol">>,
        <<"servers">> => Servers,
        <<"database">> => <<"mqtt">>,
        <<"w_mode">> => <<"safe">>,
        <<"replica_set_name">> => <<"rs0">>
    };
mongo_config(MongoHost0, MongoPort0, sharded) ->
    MongoHost = list_to_binary(MongoHost0),
    MongoPort = integer_to_binary(MongoPort0),
    Servers = <<MongoHost/binary, ":", MongoPort/binary>>,
    Name = atom_to_binary(?MODULE),
    #{
        <<"type">> => <<"mongodb_sharded">>,
        <<"name">> => Name,
        <<"enable">> => true,
        <<"collection">> => <<"mycol">>,
        <<"servers">> => Servers,
        <<"database">> => <<"mqtt">>,
        <<"w_mode">> => <<"safe">>
    };
mongo_config(MongoHost0, MongoPort0, single) ->
    MongoHost = list_to_binary(MongoHost0),
    MongoPort = integer_to_binary(MongoPort0),
    Server = <<MongoHost/binary, ":", MongoPort/binary>>,
    Name = atom_to_binary(?MODULE),
    #{
        <<"type">> => <<"mongodb_single">>,
        <<"name">> => Name,
        <<"enable">> => true,
        <<"collection">> => <<"mycol">>,
        <<"server">> => Server,
        <<"database">> => <<"mqtt">>,
        <<"w_mode">> => <<"safe">>
    }.

create_bridge(Config0 = #{<<"type">> := Type, <<"name">> := Name}) ->
    Config = maps:without(
        [
            <<"type">>,
            <<"name">>
        ],
        Config0
    ),
    emqx_bridge:create(Type, Name, Config).

delete_bridge(Config) ->
    #{
        <<"type">> := Type,
        <<"name">> := Name
    } = ?config(mongo_config, Config),
    emqx_bridge:remove(Type, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

clear_db(Config) ->
    #{
        <<"name">> := Name,
        <<"type">> := Type,
        <<"collection">> := Collection
    } = ?config(mongo_config, Config),
    ResourceID = emqx_bridge_resource:resource_id(Type, Name),
    {ok, _, #{state := #{poolname := PoolName}}} = emqx_resource:get_instance(ResourceID),
    Selector = #{},
    {true, _} = ecpool:pick_and_do(
        PoolName, {mongo_api, delete, [Collection, Selector]}, no_handover
    ),
    ok.

find_all(Config) ->
    #{
        <<"name">> := Name,
        <<"type">> := Type,
        <<"collection">> := Collection
    } = ?config(mongo_config, Config),
    ResourceID = emqx_bridge_resource:resource_id(Type, Name),
    emqx_resource:query(ResourceID, {find, Collection, #{}, #{}}).

send_message(Config, Payload) ->
    #{
        <<"name">> := Name,
        <<"type">> := Type
    } = ?config(mongo_config, Config),
    BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
    emqx_bridge:send_message(BridgeID, Payload).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    MongoConfig = ?config(mongo_config, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(MongoConfig)
    ),
    Val = erlang:unique_integer(),
    ok = send_message(Config, #{key => Val}),
    ?assertMatch(
        {ok, [#{<<"key">> := Val}]},
        find_all(Config)
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    MongoConfig = ?config(mongo_config, Config),
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
