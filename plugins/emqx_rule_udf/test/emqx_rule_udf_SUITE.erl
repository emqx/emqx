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

-define(UPPER_SRC, <<"""
def handle(s):
    return s.upper()
""">>).

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

-doc "The plugin health check and `GET server` report a reachable function server.".
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
            <<"info">> := #{<<"protocol">> := <<"emqx-rule-udf/2">>}
        }},
        api(get, ["server"])
    ).

-doc "A saved program is listed, and a test run returns its result.".
t_save_and_call(_Config) ->
    Name = <<"str.upper">>,
    ?assertMatch(
        {200, #{<<"name">> := Name, <<"source">> := ?UPPER_SRC}},
        save(Name, ?UPPER_SRC)
    ),
    ?assertMatch({200, #{<<"programs">> := [#{<<"name">> := Name}]}}, api(get, ["programs"])),
    ?assertEqual({200, #{<<"result">> => <<"HELLO">>}}, call(Name, [<<"hello">>])).

-doc "Saving a program writes its file; deleting it removes the file.".
t_saved_program_is_written_to_data_dir(_Config) ->
    Name = <<"file-copy.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    Path = filename:join(emqx_rule_udf_store:programs_dir(), Name),
    ?assertEqual({ok, ?UPPER_SRC}, file:read_file(Path)),
    {200, _} = api(delete, ["programs", Name]),
    ?assertEqual({error, enoent}, file:read_file(Path)).

-doc "Rule SQL calls a saved program with `udf(...)` and gets its result.".
t_rule_sql_calls_program(_Config) ->
    Src = <<"""
    def handle(p):
        return {"temp_f": round(p["temp_c"] * 9 / 5 + 32, 1)}
    """>>,
    {200, _} = save(<<"temp-convert.py">>, Src),
    SQL = <<"""
    SELECT udf('temp-convert.py', json_decode(payload)) AS out FROM "t/#"
    """>>,
    Context = #{topic => <<"t/1">>, payload => <<"""
    {"temp_c": 30}
    """>>},
    ?assertMatch(
        {ok, #{<<"out">> := #{<<"temp_f">> := 86.0}}},
        emqx_rule_sqltester:test(#{sql => SQL, context => Context})
    ).

-doc "Rule SQL fails when `udf(...)` names a program that does not exist.".
t_rule_sql_fails_for_unknown_program(_Config) ->
    SQL = <<"""
    SELECT udf('no-such-program', payload) AS out FROM "t/#"
    """>>,
    Context = #{topic => <<"t/1">>, payload => <<"x">>},
    ?assertMatch({error, _}, emqx_rule_sqltester:test(#{sql => SQL, context => Context})).

-doc "A source that the server rejects is not saved, and the reply carries the problems.".
t_disallowed_import_is_rejected_and_not_saved(_Config) ->
    Name = <<"bad-import.py">>,
    Src = <<"""
    import os

    def handle(x):
        return os.getcwd()
    """>>,
    ?assertMatch(
        {422, #{<<"code">> := <<"INVALID_SOURCE">>, <<"problems">> := [_ | _]}},
        save(Name, Src)
    ),
    ?assertMatch({404, #{<<"code">> := <<"PROGRAM_NOT_FOUND">>}}, api(get, ["programs", Name])).

-doc "Saving a program without a runtime fails, and nothing is saved.".
t_missing_runtime_is_rejected(_Config) ->
    Name = <<"no-runtime.py">>,
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_REQUEST">>}},
        api(put, ["programs", Name], #{<<"source">> => ?UPPER_SRC})
    ),
    ?assertMatch({404, #{<<"code">> := <<"PROGRAM_NOT_FOUND">>}}, api(get, ["programs", Name])).

-doc "A runtime that the server does not offer is rejected, and nothing is saved.".
t_unsupported_runtime_is_rejected_and_not_saved(_Config) ->
    %% The plugin passes any runtime through; the function server decides.
    Name = <<"lua-prog">>,
    Body = #{<<"runtime">> => <<"lua5">>, <<"source">> => <<"return 1">>},
    ?assertMatch(
        {422, #{<<"code">> := <<"UNSUPPORTED_RUNTIME">>}},
        api(put, ["programs", Name], Body)
    ),
    ?assertMatch({404, #{<<"code">> := <<"PROGRAM_NOT_FOUND">>}}, api(get, ["programs", Name])).

-doc "A program name that breaks the name rule is rejected.".
t_bad_name_is_rejected(_Config) ->
    ?assertMatch({400, #{<<"code">> := <<"BAD_NAME">>}}, save(<<".hidden">>, ?UPPER_SRC)).

-doc "A program cannot read a host file: the call fails in the sandbox.".
t_sandbox_blocks_host_files(_Config) ->
    %% No import statement, so the server lint accepts it. The sandbox has no
    %% host file system, so the call fails.
    Name = <<"read-passwd.py">>,
    Src = <<"""
    def handle(_x):
        return open('/etc/passwd').read()
    """>>,
    {200, _} = save(Name, Src),
    {422, #{<<"code">> := <<"FUNCTION_ERROR">>, <<"message">> := Msg}} = call(Name, [1]),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"FileNotFoundError">>)).

-doc "An exception in a program returns `FUNCTION_ERROR`.".
t_function_error_is_returned(_Config) ->
    Name = <<"div-zero.py">>,
    Src = <<"""
    def handle(x):
        return x / 0
    """>>,
    {200, _} = save(Name, Src),
    ?assertMatch({422, #{<<"code">> := <<"FUNCTION_ERROR">>}}, call(Name, [1])).

-doc "Saving does not load a program on the server; the first call sends it.".
t_first_call_sends_the_program(Config) ->
    %% A source that no earlier run can have left in the server cache.
    Src = iolist_to_binary([
        "# ", integer_to_list(erlang:system_time(nanosecond)), $\n, ?UPPER_SRC
    ]),
    Name = <<"first-call.py">>,
    {200, #{<<"sha256">> := Sha256}} = save(Name, Src),
    %% Saving only checks the program: the server does not keep it.
    CallBody = #{<<"name">> => Name, <<"sha256">> => Sha256, <<"args">> => [<<"x">>]},
    ?assertEqual(404, server_call(Config, CallBody)),
    ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])),
    %% The call carried the program, and the server keeps it loaded.
    ?assertEqual(200, server_call(Config, CallBody)).

-doc "Calls work when every request reaches a server instance that does not have the program.".
t_call_works_when_no_instance_keeps_the_program(Config) ->
    %% Behind a load balancer, each request may reach an instance that does not
    %% have the program. The proxy simulates that: it answers every call that
    %% does not carry the program with FUNCTION_NOT_FOUND.
    {ok, ProxyURL} = start_proxy(Config, #{forget => true}),
    try
        {204, _} = set_server(Config, #{<<"url">> => ProxyURL}),
        Name = <<"lb.py">>,
        {200, _} = save(Name, ?UPPER_SRC),
        ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])),
        ?assertEqual(<<"XYZ">>, emqx_rule_udf_client:call(Name, [<<"xyz">>]))
    after
        cowboy:stop_listener(?MODULE)
    end.

-doc "With an empty `cacertfile`, an https server is trusted through `node.trusted_certs` only.".
t_https_trusts_node_trusted_certs(Config) ->
    %% The function server uses a private CA. The plugin trusts it only after
    %% the CA is in `node.trusted_certs'.
    #{cacertfile := CAFile} = TLS = gen_tls_files(?config(priv_dir, Config)),
    {ok, ProxyURL} = start_proxy(Config, #{tls => TLS}),
    Previous = application:get_env(emqx, trusted_certs_path),
    try
        ok = application:unset_env(emqx, trusted_certs_path),
        {204, _} = set_server(Config, #{<<"url">> => ProxyURL}),
        {error, #{reason := Reason}} = emqx_rule_udf_client:server_info(),
        ?assertNotEqual(nomatch, string:find(io_lib:format("~0p", [Reason]), "unknown_ca")),
        ok = application:set_env(emqx, trusted_certs_path, CAFile),
        %% A changed config restarts the pool, which reads the trusted CAs.
        {204, _} = set_server(Config, #{<<"url">> => ProxyURL, <<"pool_size">> => 3}),
        ?assertMatch(
            {ok, #{<<"protocol">> := <<"emqx-rule-udf/2">>}}, emqx_rule_udf_client:server_info()
        )
    after
        restore_trusted_certs(Previous),
        cowboy:stop_listener(?MODULE)
    end.

restore_trusted_certs({ok, Path}) -> application:set_env(emqx, trusted_certs_path, Path);
restore_trusted_certs(undefined) -> application:unset_env(emqx, trusted_certs_path).

-doc "An https server is trusted through `cacertfile`; a missing file is a config error.".
t_https_trusts_configured_cacertfile(Config) ->
    #{cacertfile := CAFile} = TLS = gen_tls_files(?config(priv_dir, Config)),
    {ok, ProxyURL} = start_proxy(Config, #{tls => TLS}),
    Previous = application:get_env(emqx, trusted_certs_path),
    try
        ok = application:unset_env(emqx, trusted_certs_path),
        ?assertMatch(
            {400, #{<<"code">> := <<"BAD_CONFIG">>}},
            set_server(Config, #{<<"url">> => ProxyURL, <<"cacertfile">> => <<"/no/such.pem">>})
        ),
        {204, _} = set_server(Config, #{
            <<"url">> => ProxyURL, <<"cacertfile">> => list_to_binary(CAFile)
        }),
        ?assertMatch({ok, #{<<"protocol">> := _}}, emqx_rule_udf_client:server_info())
    after
        restore_trusted_certs(Previous),
        cowboy:stop_listener(?MODULE)
    end.

-doc "An unexpected call reply is cut short in the error.".
t_bad_response_body_is_truncated(Config) ->
    Name = <<"big-reply.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    Big = emqx_utils_json:encode(#{<<"unexpected">> => binary:copy(<<"x">>, 100_000)}),
    {ok, ProxyURL} = start_proxy(Config, #{call_reply => Big}),
    try
        {204, _} = set_server(Config, #{<<"url">> => ProxyURL}),
        {error, #{reason := bad_response, body := Body}} =
            emqx_rule_udf_client:try_call(Name, [<<"a">>]),
        ?assert(byte_size(Body) =< 300, byte_size(Body))
    after
        cowboy:stop_listener(?MODULE)
    end.

-doc "A config update completes when the old HTTP pool does not stop.".
t_stuck_pool_does_not_block_config_update(Config) ->
    Name = <<"stuck.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    %% A suspended process handles no exit signal but `kill', so a plain stop of
    %% the pool supervisor (`shutdown => infinity') would wait forever.
    {_, PoolSup, _, _} = lists:keyfind(
        {ehttpc_pool_sup, emqx_rule_udf_http}, 1, supervisor:which_children(ehttpc_sup)
    ),
    true = erlang:suspend_process(PoolSup),
    ?assertMatch({204, _}, set_server(Config, #{<<"pool_size">> => 3})),
    ?assertNot(is_process_alive(PoolSup)),
    ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])).

-doc "The client process state does not hold the token in plain text.".
t_token_is_not_kept_in_plain_text(_Config) ->
    State = io_lib:format("~p", [sys:get_state(emqx_rule_udf_client)]),
    ?assertEqual(nomatch, string:find(State, binary_to_list(?SERVER_TOKEN))).

-doc "A rejected config update keeps the client working.".
t_invalid_config_update_keeps_client(Config) ->
    Name = <<"keep.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    ?assertMatch(
        {400, #{<<"code">> := <<"BAD_CONFIG">>}}, set_server(Config, #{<<"pool_size">> => -1})
    ),
    ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])).

-doc "When the new HTTP pool fails to start, the old settings and pool stay in use.".
t_failed_pool_start_restores_old_pool(Config) ->
    Name = <<"restore.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    ok = meck:new(ehttpc_sup, [passthrough, no_history]),
    try
        ok = meck:expect(ehttpc_sup, start_pool, fun(Pool, Opts) ->
            case proplists:get_value(pool_size, Opts) of
                7 -> {error, simulated};
                _ -> meck:passthrough([Pool, Opts])
            end
        end),
        ?assertMatch({400, _}, set_server(Config, #{<<"pool_size">> => 7})),
        ?assertEqual(<<"ABC">>, emqx_rule_udf_client:call(Name, [<<"abc">>])),
        %% The old config is still the current one: applying it again is a no-op.
        ?assertMatch({204, _}, set_server(Config, #{})),
        ?assertEqual(<<"XYZ">>, emqx_rule_udf_client:call(Name, [<<"xyz">>]))
    after
        meck:unload(ehttpc_sup)
    end.

-doc "Saves do not start more periodic file syncs: one timer runs.".
t_one_file_sync_timer(_Config) ->
    %% Each save syncs the files at once. It must not start another periodic sync.
    lists:foreach(
        fun(I) -> {200, _} = save(<<"sync-", (integer_to_binary(I))/binary>>, ?UPPER_SRC) end,
        lists:seq(1, 5)
    ),
    Pid = whereis(emqx_rule_udf_store),
    erlang:trace(Pid, true, ['receive']),
    try
        timer:sleep(5500),
        Syncs = count_sync_messages(Pid, 0),
        %% One timer chain delivers one `sync' per 5 s, at most two in 5.5 s.
        ?assert(Syncs =< 2, Syncs)
    after
        erlang:trace(Pid, false, ['receive'])
    end.

count_sync_messages(Pid, N) ->
    receive
        {trace, Pid, 'receive', sync} -> count_sync_messages(Pid, N + 1);
        {trace, Pid, 'receive', _} -> count_sync_messages(Pid, N)
    after 0 -> N
    end.

-doc "A deleted program cannot be read or called, and a second delete returns 404.".
t_delete_program(_Config) ->
    Name = <<"to-delete.py">>,
    {200, _} = save(Name, ?UPPER_SRC),
    ?assertEqual({200, #{<<"deleted">> => Name}}, api(delete, ["programs", Name])),
    ?assertMatch({404, _}, api(get, ["programs", Name])),
    ?assertMatch({404, _}, call(Name, [<<"x">>])),
    ?assertMatch({404, _}, api(delete, ["programs", Name])).

-doc "Only the global administrator can change or run programs; others can read them.".
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

-doc "A config update is checked while the plugin is stopped.".
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

%% A call sent to the function server directly, without the plugin.
server_call(Config, Body) ->
    URL = server_url(Config) ++ "/v2/call",
    Headers = [{"authorization", "Bearer " ++ binary_to_list(?SERVER_TOKEN)}],
    Request = {URL, Headers, "application/json", emqx_utils_json:encode(Body)},
    {ok, {{_, Status, _}, _, _}} = httpc:request(post, Request, [], []),
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

%% Change some server settings of the plugin, keeping the others.
set_server(Config, Changes) ->
    #{<<"server">> := Server} = Conf = plugin_config(Config),
    plugin_config_request(
        put, ?config(plugin_name_vsn, Config), Conf#{<<"server">> := maps:merge(Server, Changes)}
    ).

%% An HTTP proxy to the function server. With `forget', it answers a call that
%% does not carry the function as a server that does not have it loaded. With `tls', it serves HTTPS. With
%% `call_reply', it answers every call with that 200 body.
start_proxy(Config, Opts) ->
    State = #{
        server_url => server_url(Config),
        forget => maps:get(forget, Opts, false),
        call_reply => maps:get(call_reply, Opts, undefined)
    },
    Dispatch = cowboy_router:compile([{'_', [{'_', ?MODULE, State}]}]),
    ProtoOpts = #{env => #{dispatch => Dispatch}},
    case Opts of
        #{tls := #{certfile := Cert, keyfile := Key}} ->
            TransOpts = [{port, 0}, {certfile, Cert}, {keyfile, Key}],
            {ok, _} = cowboy:start_tls(?MODULE, TransOpts, ProtoOpts),
            Scheme = "https://localhost:";
        #{} ->
            {ok, _} = cowboy:start_clear(?MODULE, [{port, 0}], ProtoOpts),
            Scheme = "http://127.0.0.1:"
    end,
    {ok, iolist_to_binary([Scheme, integer_to_list(ranch:get_port(?MODULE))])}.

%% A CA, and a server certificate for `localhost' that it signs.
gen_tls_files(Dir) ->
    {CACert, _} = CA = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    Server = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => CA,
        subject => #{name => "localhost"},
        extensions => #{subject_alt_name => [{dns, "localhost"}]}
    }),
    %% Unique names: `ssl' caches PEM files by path.
    Id = integer_to_list(erlang:unique_integer([positive])),
    CAFile = filename:join(Dir, "udf-ca-" ++ Id ++ ".pem"),
    ok = emqx_cth_tls:write_pem(CAFile, CACert),
    {CertFile, KeyFile} = emqx_cth_tls:write_cert(Dir, "udf-server-" ++ Id, Server),
    #{cacertfile => CAFile, certfile => CertFile, keyfile => KeyFile}.

%% cowboy handler of the proxy
init(Req0, #{call_reply := Reply} = State) when is_binary(Reply) ->
    {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Reply, Req0),
        State};
init(Req0, #{forget := Forget} = State) ->
    Path = binary_to_list(cowboy_req:path(Req0)),
    {ok, Body, Req} = cowboy_req:read_body(Req0),
    case Forget andalso Path =:= "/v2/call" andalso emqx_utils_json:decode(Body) of
        #{<<"function">> := _} -> forward(Req, Path, Body, State);
        #{} -> {ok, not_loaded_reply(Req), State};
        false -> forward(Req, Path, Body, State)
    end.

not_loaded_reply(Req) ->
    Body = emqx_utils_json:encode(#{
        <<"code">> => <<"FUNCTION_NOT_FOUND">>, <<"message">> => <<"not loaded">>
    }),
    cowboy_req:reply(404, #{<<"content-type">> => <<"application/json">>}, Body, Req).

forward(Req, Path, Body, #{server_url := ServerURL} = State) ->
    Headers = [{"authorization", binary_to_list(cowboy_req:header(<<"authorization">>, Req))}],
    Method = binary_to_atom(string:lowercase(cowboy_req:method(Req))),
    Request =
        case Body of
            <<>> -> {ServerURL ++ Path, Headers};
            _ -> {ServerURL ++ Path, Headers, "application/json", Body}
        end,
    {ok, {{_, Status, _}, _, RespBody}} =
        httpc:request(Method, Request, [], [{body_format, binary}]),
    {ok, cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>}, RespBody, Req),
        State}.

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
