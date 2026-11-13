%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_udf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%% The reference function server, `ghcr.io/emqx/emqx-rule-udf-wasm-server'.
%% CI starts it from `.ci/docker-compose-file/docker-compose-rule-udf.yaml'.
%% Set RULE_UDF_SERVER_HOST and RULE_UDF_SERVER_PORT to use another server.
-define(SERVER_HOST, "rule-udf-server").
-define(SERVER_PORT, "8080").
-define(SERVER_TOKEN, <<"ct-token">>).

-define(UPPER_SRC, <<"def handle(s):\n    return s.upper()\n">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Host = os:getenv("RULE_UDF_SERVER_HOST", ?SERVER_HOST),
    Port = list_to_integer(os:getenv("RULE_UDF_SERVER_PORT", ?SERVER_PORT)),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            start_apps([{server_host, Host}, {server_port, Port} | Config]);
        false ->
            case os:getenv("IS_CI") of
                "yes" -> error({function_server_unavailable, Host, Port});
                _ -> {skip, function_server_unavailable}
            end
    end.

start_apps(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_rule_engine,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => WorkDir}
    ),
    {module, emqx_rule_funcs} = code:ensure_loaded(emqx_rule_funcs),
    ok = filelib:ensure_path(filename:join([InstallDir, "dummy"])),
    Package = plugin_package(),
    {ok, PackageBin} = file:read_file(Package),
    [
        {apps, Apps},
        {plugin_name_vsn, filename:basename(Package, ".tar.gz")},
        {plugin_package_bin, PackageBin}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    ok = install_plugin(Config),
    ok = configure_plugin(Config),
    ok = emqx_plugins:ensure_started(?config(plugin_name_vsn, Config)),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_health_check_reports_server_reachable(Config) ->
    ?assertMatch(
        {ok, #{health_status := #{status := ok}}},
        emqx_plugins:describe(
            ?config(plugin_name_vsn, Config), #{fill_readme => false, health_check => true}
        )
    ),
    ?assertMatch(
        {200, #{
            <<"reachable">> := true,
            <<"info">> := #{<<"protocol">> := <<"emqx-rule-udf/1">>}
        }},
        api(get, ["server"])
    ).

t_save_and_call(_Config) ->
    Name = <<"str.upper">>,
    ?assertMatch(
        {200, #{<<"name">> := Name, <<"source">> := ?UPPER_SRC}},
        save(Name, ?UPPER_SRC)
    ),
    ?assertMatch({200, #{<<"programs">> := [#{<<"name">> := Name}]}}, api(get, ["programs"])),
    ?assertEqual({200, #{<<"result">> => <<"HELLO">>}}, call(Name, [<<"hello">>])).

t_saved_program_is_written_to_data_dir(_Config) ->
    Name = <<"file-copy.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    Path = filename:join(emqx_rule_udf_store:programs_dir(), Name),
    ?assertEqual({ok, ?UPPER_SRC}, file:read_file(Path)),
    {200, _} = api(delete, ["programs", Name]),
    ?assertEqual({error, enoent}, file:read_file(Path)).

t_rule_sql_calls_program(_Config) ->
    Src = <<"def handle(p):\n    return {\"temp_f\": round(p[\"temp_c\"] * 9 / 5 + 32, 1)}\n">>,
    {200, _} = save(<<"temp-convert.py">>, Src),
    SQL = <<"SELECT udf('temp-convert.py', json_decode(payload)) AS out FROM \"t/#\"">>,
    Context = #{topic => <<"t/1">>, payload => <<"{\"temp_c\": 30}">>},
    ?assertMatch(
        {ok, #{<<"out">> := #{<<"temp_f">> := 86.0}}},
        emqx_rule_sqltester:test(#{sql => SQL, context => Context})
    ).

t_rule_sql_fails_for_unknown_program(_Config) ->
    SQL = <<"SELECT udf('no-such-program', payload) AS out FROM \"t/#\"">>,
    Context = #{topic => <<"t/1">>, payload => <<"x">>},
    ?assertMatch({error, _}, emqx_rule_sqltester:test(#{sql => SQL, context => Context})).

t_disallowed_import_is_rejected_and_not_saved(_Config) ->
    Name = <<"bad-import.py">>,
    Src = <<"import os\n\ndef handle(x):\n    return os.getcwd()\n">>,
    ?assertMatch(
        {422, #{<<"code">> := <<"INVALID_SOURCE">>, <<"problems">> := [_ | _]}},
        save(Name, Src)
    ),
    ?assertMatch({404, #{<<"code">> := <<"PROGRAM_NOT_FOUND">>}}, api(get, ["programs", Name])).

t_bad_name_is_rejected(_Config) ->
    ?assertMatch({400, #{<<"code">> := <<"BAD_NAME">>}}, save(<<".hidden">>, ?UPPER_SRC)).

t_sandbox_blocks_host_files(_Config) ->
    %% No import statement, so the server lint accepts it. The sandbox has no
    %% host file system, so the call fails.
    Name = <<"read-passwd.py">>,
    Src = <<"def handle(_x):\n    return open('/etc/passwd').read()\n">>,
    {200, _} = save(Name, Src),
    {422, #{<<"code">> := <<"FUNCTION_ERROR">>, <<"message">> := Msg}} = call(Name, [1]),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"FileNotFoundError">>)).

t_function_error_is_returned(_Config) ->
    Name = <<"div-zero.py">>,
    {200, _} = save(Name, <<"def handle(x):\n    return x / 0\n">>),
    ?assertMatch({422, #{<<"code">> := <<"FUNCTION_ERROR">>}}, call(Name, [1])).

t_program_is_uploaded_again_when_server_lost_it(Config) ->
    Name = <<"reupload.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    %% The server forgets all functions on restart: simulate that.
    ?assertEqual(200, server_request(Config, delete, "/v1/functions/" ++ binary_to_list(Name))),
    ?assertEqual(404, server_request(Config, get, "/v1/functions/" ++ binary_to_list(Name))),
    ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])),
    ?assertEqual(200, server_request(Config, get, "/v1/functions/" ++ binary_to_list(Name))).

t_delete_program(_Config) ->
    Name = <<"to-delete.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    ?assertEqual({200, #{<<"deleted">> => Name}}, api(delete, ["programs", Name])),
    ?assertMatch({404, _}, api(get, ["programs", Name])),
    ?assertMatch({404, _}, call(Name, [<<"x">>])),
    ?assertMatch({404, _}, api(delete, ["programs", Name])).

t_only_global_admin_can_change_or_run_programs(_Config) ->
    Name = <<"viewer.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    Viewer = #{auth_meta => #{actor => #{role => <<"viewer">>, namespace => ?global_ns}}},
    NsAdmin = #{auth_meta => #{actor => #{role => <<"administrator">>, namespace => <<"ns1">>}}},
    Put = #{body => #{<<"source">> => ?UPPER_SRC}},
    Call = #{body => #{<<"args">> => [<<"x">>]}},
    lists:foreach(
        fun(Ctx) ->
            ?assertMatch(
                {error, 403, _, _}, emqx_rule_udf_api:handle(put, [<<"programs">>, Name], Put, Ctx)
            ),
            ?assertMatch(
                {error, 403, _, _},
                emqx_rule_udf_api:handle(delete, [<<"programs">>, Name], #{}, Ctx)
            ),
            ?assertMatch(
                {error, 403, _, _},
                emqx_rule_udf_api:handle(post, [<<"programs">>, Name, <<"call">>], Call, Ctx)
            ),
            ?assertMatch(
                {ok, 200, _, _}, emqx_rule_udf_api:handle(get, [<<"programs">>, Name], #{}, Ctx)
            )
        end,
        [Viewer, NsAdmin]
    ).

t_config_update_while_stopped(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertMatch({204, _}, plugin_config_request(put, NameVsn, plugin_config(Config))),
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_CONFIG">>}},
        plugin_config_request(put, NameVsn, #{<<"server">> => #{<<"url">> => <<"ftp://x">>}})
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

save(Name, Source) ->
    api(put, ["programs", Name], #{<<"runtime">> => <<"python3">>, <<"source">> => Source}).

call(Name, Args) ->
    api(post, ["programs", Name, "call"], #{<<"args">> => Args}).

api(Method, Path) ->
    api(Method, Path, []).

api(Method, Path, Body) ->
    URL = emqx_mgmt_api_test_util:api_path(
        ["plugin_api", "emqx_rule_udf" | [str(P) || P <- Path]]
    ),
    Headers = [
        emqx_mgmt_api_test_util:auth_header_(),
        {"Connection", "close"},
        {"Content-Type", "application/json"}
    ],
    emqx_mgmt_api_test_util:simplify_decode_result(
        emqx_mgmt_api_test_util:request_api(Method, URL, "", Headers, Body, #{return_all => true})
    ).

str(B) when is_binary(B) -> binary_to_list(B);
str(S) -> S.

%% A direct request to the function server, as an operator would send it.
server_request(Config, Method, Path) ->
    URL = server_url(Config) ++ Path,
    Headers = [{"authorization", "Bearer " ++ binary_to_list(?SERVER_TOKEN)}],
    {ok, {{_, Status, _}, _, _}} = httpc:request(Method, {URL, Headers}, [], []),
    Status.

server_url(Config) ->
    "http://" ++ ?config(server_host, Config) ++ ":" ++
        integer_to_list(?config(server_port, Config)).

plugin_config(Config) ->
    #{
        <<"server">> => #{
            <<"url">> => list_to_binary(server_url(Config)),
            <<"token">> => ?SERVER_TOKEN,
            <<"pool_size">> => 4,
            <<"connect_timeout_ms">> => 5000,
            <<"request_timeout_ms">> => 5000
        }
    }.

configure_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    {204, _} = plugin_config_request(put, NameVsn, plugin_config(Config)),
    ok.

plugin_config_request(Method, NameVsn, Body) ->
    URL = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config"]),
    Headers = [
        emqx_mgmt_api_test_util:auth_header_(),
        {"Connection", "close"},
        {"Content-Type", "application/json"}
    ],
    emqx_mgmt_api_test_util:simplify_decode_result(
        emqx_mgmt_api_test_util:request_api(Method, URL, "", Headers, Body, #{return_all => true})
    ).

plugin_package() ->
    Root = emqx_common_test_helpers:proj_root(),
    {ok, VsnBin} = file:read_file(filename:join([Root, "plugins", "emqx_rule_udf", "VERSION"])),
    Vsn = string:trim(binary_to_list(VsnBin)),
    Package = filename:join([Root, "_build", "plugins", "emqx_rule_udf-" ++ Vsn ++ ".tar.gz"]),
    case os:getenv("IS_CI") of
        "yes" ->
            %% CI builds the package before the tests. A missing package
            %% must fail the suite, not skip it.
            case filelib:is_regular(Package) of
                true -> Package;
                false -> error({missing_prebuilt_plugin_package, Package})
            end;
        _ ->
            _ = file:delete(Package),
            Output = os:cmd(
                "cd " ++ Root ++ " && PROFILE=emqx-enterprise make plugin-emqx_rule_udf 2>&1"
            ),
            case filelib:is_regular(Package) of
                true -> Package;
                false -> error({plugin_package_build_failed, Package, Output})
            end
    end.

install_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    ok = emqx_plugins:write_package(NameVsn, PackageBin),
    ok = emqx_plugins:allow_installation(
        NameVsn, binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase)
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, fresh_install).

cleanup_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    case emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => false}) of
        {ok, _} ->
            _ = emqx_plugins:ensure_stopped(NameVsn),
            _ = emqx_plugins:ensure_disabled(NameVsn),
            _ = emqx_plugins:ensure_uninstalled(NameVsn);
        {error, _} ->
            ok
    end,
    _ = emqx_plugins:delete_package(NameVsn),
    _ = emqx_plugins:forget_allowed_installation(NameVsn),
    %% The program table outlives the plugin. Start each case empty.
    _ = catch mnesia:clear_table(emqx_rule_udf_program),
    ok.
