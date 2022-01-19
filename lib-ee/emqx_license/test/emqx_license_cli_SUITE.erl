%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_config:save_schema_mod_and_names(emqx_license_schema),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    meck:new(emqx_license_parser, [passthrough]),
    meck:expect(emqx_license_parser, public_key, fun public_key/0),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(emqx_license_parser),
    ok.

set_special_configs(emqx_license) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config),
    RawConfig = #{<<"file">> => emqx_license_test_lib:default_license()},
    emqx_config:put_raw([<<"license">>], RawConfig);

set_special_configs(_) -> ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_help(_Config) ->
    _ = emqx_license_cli:license([]).

t_info(_Config) ->
    _ = emqx_license_cli:license(["info"]).

t_reload(_Config) ->
    _ = emqx_license_cli:license(["reload", "/invalid/path"]),
    _ = emqx_license_cli:license(["reload", emqx_license_test_lib:default_license()]),
    _ = emqx_license_cli:license(["reload"]).

t_update(_Config) ->
    {ok, LicenseValue} = file:read_file(emqx_license_test_lib:default_license()),
    _ = emqx_license_cli:license(["update", LicenseValue]),
    _ = emqx_license_cli:license(["reload"]),
    _ = emqx_license_cli:license(["update", "Invalid License Value"]).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

public_key() -> <<"MEgCQQChzN6lCUdt4sYPQmWBYA3b8Zk87Jfk+1A1zcTd+lCU0Tf
                  vXhSHgEWz18No4lL2v1n+70CoYpc2fzfhNJitgnV9AgMBAAE=">>.

digest() -> <<"3jHg0zCb4NL5v8eIoKn+CNDMq8A04mXEOefqlUBSSVs=">>.
