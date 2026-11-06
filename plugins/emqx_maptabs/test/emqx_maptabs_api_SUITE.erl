%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_api_SUITE).

-moduledoc """
The REST API over HTTP, through the plugin API gateway: request body
decoding, scope check and callback resolution included. Cluster
behaviour of the same handlers is in emqx_maptabs_cluster_SUITE.
""".

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-define(TABLE, <<"can_signals">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_ctl,
            emqx_rule_engine,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(filename:join([InstallDir, "dummy"])),
    try
        Package = emqx_maptabs_SUITE:plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {apps, Apps},
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | Config
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_maptabs_SUITE:cleanup_plugin(Config),
    ok = emqx_maptabs_SUITE:wipe_storage(),
    ok = emqx_maptabs_SUITE:install_and_start_plugin(Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = emqx_maptabs_SUITE:cleanup_plugin(Config),
    ok = emqx_maptabs_SUITE:wipe_storage(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "Upload, read back, list, reload and delete a table over HTTP.".
t_http_table_lifecycle(_Config) ->
    Rows = [#{key => 1, v => <<"v1">>}, #{key => <<"k2">>, v => <<"v2">>}],
    Version = canonical_version(Rows),
    %% a JSON array body: the gateway hands it over decoded
    {200, #{<<"version">> := Version, <<"row_count">> := 2, <<"nodes">> := [Node]}} =
        api(put, ["tables", ?TABLE], {json, Rows}),
    ?assertEqual(atom_to_binary(node()), Node),
    ?assertEqual(#{<<"v">> => <<"v1">>}, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual(#{<<"v">> => <<"v2">>}, emqx_maptabs:lookup(?TABLE, <<"k2">>)),
    {200, Stored} = raw(get, ["tables", ?TABLE], none),
    ?assertEqual(Version, md5(Stored)),
    ?assertMatch(
        {200, [
            #{
                <<"node">> := Node,
                <<"tables">> := [#{<<"name">> := ?TABLE, <<"version">> := Version}]
            }
        ]},
        api(get, ["tables"], none)
    ),
    ?assertEqual({200, #{<<"nodes">> => [Node]}}, api(post, ["reload"], none)),
    ?assertMatch({204, _}, raw(delete, ["tables", ?TABLE], none)),
    ?assertEqual(undefined, emqx_maptabs:lookup(?TABLE, 1)),
    ?assertEqual({error, not_found}, emqx_maptabs:read_table(?TABLE)),
    ok.

-doc """
The stored bytes are the canonical JSON of the body: a pretty-printed
non-JSON content-type body and an application/json body with the same
content get one version.
""".
t_http_body_stored_canonical(_Config) ->
    Pretty = <<"[\n  {\"key\": 1,   \"v\": \"v1\"}\n]\n">>,
    {200, #{<<"version">> := Version}} = api(put, ["tables", ?TABLE], {text, Pretty}),
    ?assertEqual(canonical_version([#{key => 1, v => <<"v1">>}]), Version),
    {200, Stored} = raw(get, ["tables", ?TABLE], none),
    ?assertNotEqual(Pretty, Stored),
    ?assertEqual(Version, md5(Stored)),
    ?assertMatch(
        {200, #{<<"version">> := Version}},
        api(put, ["tables", ?TABLE], {json, [#{key => 1, v => <<"v1">>}]})
    ),
    ok.

-doc "A name that is not [a-zA-Z0-9_-]+ is rejected with 400 before the body is read.".
t_http_rejects_invalid_name(_Config) ->
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_REQUEST">>, <<"reason">> := <<"invalid_table_name">>}},
        api(put, ["tables", "bad.name"], {json, [#{key => 1}]})
    ),
    ?assertMatch(
        {400, #{<<"reason">> := <<"invalid_table_name">>}},
        api(get, ["tables", "bad.name"], none)
    ),
    ok.

-doc "Each content validation failure is a 400 carrying the loader's reason, row and key.".
t_http_rejects_invalid_content(_Config) ->
    Put = fun(Body) -> api(put, ["tables", ?TABLE], Body) end,
    ?assertMatch(
        {400, #{<<"reason">> := <<"not_a_json_array">>}},
        Put({json, #{key => 1}})
    ),
    ?assertMatch(
        {400, #{<<"reason">> := <<"invalid_json">>}},
        Put({text, <<"[{oops">>})
    ),
    ?assertMatch(
        {400, #{<<"reason">> := <<"missing_key">>, <<"row_number">> := 2}},
        Put({json, [#{key => 1}, #{nokey => 2}]})
    ),
    ?assertMatch(
        {400, #{<<"reason">> := <<"float_key">>, <<"row_number">> := 1, <<"key">> := 1.5}},
        Put({json, [#{key => 1.5}]})
    ),
    ?assertMatch(
        {400, #{<<"reason">> := <<"duplicate_key">>, <<"row_number">> := 2, <<"key">> := 7}},
        Put({json, [#{key => 7}, #{key => 7}]})
    ),
    ?assertEqual({error, not_found}, emqx_maptabs:read_table(?TABLE)),
    ok.

-doc "The row, size and table-count limits apply to uploads, each as a 400.".
t_http_rejects_over_limits(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    {200, _} = api(put, ["tables", ?TABLE], {json, [#{key => 1}]}),
    ok = emqx_plugins:update_config(NameVsn, #{
        <<"max_tables">> => 1,
        <<"max_rows_per_table">> => 2,
        <<"max_table_file_bytes">> => 100
    }),
    try
        ?assertMatch(
            {400, #{
                <<"reason">> := <<"too_many_rows">>,
                <<"row_count">> := 3,
                <<"max_rows_per_table">> := 2
            }},
            api(put, ["tables", ?TABLE], {json, [#{key => N} || N <- [1, 2, 3]]})
        ),
        ?assertMatch(
            {400, #{
                <<"reason">> := <<"table_file_too_large">>,
                <<"max_table_file_bytes">> := 100
            }},
            api(put, ["tables", ?TABLE], {json, [#{key => 1, f => binary:copy(<<"x">>, 200)}]})
        ),
        ?assertMatch(
            {400, #{<<"reason">> := <<"too_many_tables">>, <<"max_tables">> := 1}},
            api(put, ["tables", "tab2"], {json, [#{key => 1}]})
        )
    after
        ok = emqx_plugins:update_config(NameVsn, #{
            <<"max_tables">> => 100,
            <<"max_rows_per_table">> => 10000,
            <<"max_table_file_bytes">> => 10000000
        })
    end,
    ok.

-doc "Reading or deleting an unknown table is a 404.".
t_http_unknown_table(_Config) ->
    ?assertMatch(
        {404, #{<<"code">> := <<"NOT_FOUND">>, <<"name">> := <<"nope">>}},
        api(get, ["tables", "nope"], none)
    ),
    ?assertMatch(
        {404, #{<<"code">> := <<"NOT_FOUND">>, <<"name">> := <<"nope">>}},
        api(delete, ["tables", "nope"], none)
    ),
    ok.

-doc "An API key needs the plugin_api scope; the gateway enforces it for the plugin.".
t_http_requires_plugin_api_scope(_Config) ->
    {ok, Allowed} = create_api_key(<<"maptabs-plugin-api">>, [?SCOPE_PLUGIN_API]),
    {ok, Denied} = create_api_key(<<"maptabs-connections">>, [?SCOPE_CONNECTIONS]),
    try
        ?assertMatch({200, [_]}, api(get, ["tables"], none, Allowed)),
        ?assertMatch({403, _}, api(get, ["tables"], none, Denied))
    after
        delete_api_key(<<"maptabs-plugin-api">>),
        delete_api_key(<<"maptabs-connections">>)
    end,
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

canonical_version(Rows) ->
    md5(emqx_utils_json:encode(emqx_utils_json:decode(emqx_utils_json:encode(Rows)))).

md5(Bin) ->
    binary:encode_hex(erlang:md5(Bin), lowercase).

api(Method, Path, Body) ->
    api(Method, Path, Body, emqx_mgmt_api_test_util:auth_header_()).

api(Method, Path, Body, Auth) ->
    {Status, Resp} = raw(Method, Path, Body, Auth),
    case emqx_utils_json:safe_decode(Resp) of
        {ok, Decoded} -> {Status, Decoded};
        {error, _} -> {Status, Resp}
    end.

raw(Method, Path, Body) ->
    raw(Method, Path, Body, emqx_mgmt_api_test_util:auth_header_()).

raw(Method, Path, Body, Auth) ->
    Url = emqx_mgmt_api_test_util:api_path(["plugin_api", "emqx_maptabs" | Path]),
    Headers = [Auth],
    Request =
        case Body of
            %% httpc takes no body-less form for post
            none when Method =:= post -> {Url, Headers, "application/json", <<>>};
            none -> {Url, Headers};
            {json, Term} -> {Url, Headers, "application/json", emqx_utils_json:encode(Term)};
            {text, Bin} -> {Url, Headers, "text/plain", Bin}
        end,
    {ok, {{_, Status, _}, _RespHeaders, RespBody}} =
        httpc:request(Method, Request, [], [{body_format, binary}]),
    {Status, RespBody}.

create_api_key(Name, Scopes) ->
    delete_api_key(Name),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    Body = #{
        name => Name,
        desc => <<"maptabs api test">>,
        enable => true,
        expired_at => <<"infinity">>,
        scopes => Scopes
    },
    {ok, Res} = emqx_mgmt_api_test_util:request_api(
        post, Path, "", emqx_dashboard_SUITE:auth_header_(), Body
    ),
    #{<<"api_key">> := Key, <<"api_secret">> := Secret} = emqx_utils_json:decode(Res),
    {ok, emqx_common_test_http:auth_header(binary_to_list(Key), binary_to_list(Secret))}.

delete_api_key(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    _ = emqx_mgmt_api_test_util:request_api(delete, Path, emqx_dashboard_SUITE:auth_header_()),
    ok.
