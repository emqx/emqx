-module(emqx_acme_api_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_plugins/include/emqx_plugins.hrl").

all() ->
    [
        t_put_config_returns_400_when_parser_rejects_avro_valid_input
    ].

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join(WorkDir, "plugins"),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_ctl,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}}
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(InstallDir),
    NameVsn = name_vsn(),
    {ok, TarBin} = file:read_file(prebuilt_tarball()),
    ok = emqx_plugins:write_package(NameVsn, TarBin),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    [{suite_apps, Apps}, {install_dir, InstallDir}, {name_vsn, NameVsn} | Config].

end_per_suite(Config) ->
    NameVsn = ?config(name_vsn, Config),
    catch emqx_plugins:ensure_stopped(NameVsn),
    catch emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

-doc "A request body that passes avro (the avsc declares `domains` as a "
"string and `\"mqtt.example.com,admin@example\"` is a valid string) but is "
"rejected by the plugin's parser must be reported back to the API caller as "
"an error, with the persisted config left untouched. The HTTP layer "
"translates that error to a 400 BAD_CONFIG response (see "
"emqx_mgmt_api_plugins:return_config_update_result/1).".
t_put_config_returns_400_when_parser_rejects_avro_valid_input(Config) ->
    NameVsn = ?config(name_vsn, Config),
    OldConfig = emqx_plugins:get_config(NameVsn),
    BadConfig = OldConfig#{<<"domains">> => <<"mqtt.example.com,admin@example">>},

    %% Layer 1: avro decode accepts the bad input. This is the precondition
    %% that distinguishes the parse-fail case from the avro-fail case.
    ?assertMatch(
        {ok, _AvroValue},
        emqx_plugins:decode_plugin_config_map(NameVsn, BadConfig)
    ),

    %% Layer 2: emqx_plugins:update_config/2 returns {error, Reason}. This
    %% is what the BPAPI multi_call surfaces, which the HTTP API maps to
    %% 400 BAD_CONFIG via return_config_update_result/1.
    ?assertMatch(
        {error, {invalid_domain, <<"admin@example">>}},
        emqx_plugins:update_config(NameVsn, BadConfig)
    ),

    %% Layer 3: persisted config and in-memory cache are untouched.
    %% emqx_plugins:update_config/2 uses a maybe-block so the
    %% backup_and_update / put_cached_config steps short-circuit on the
    %% parse error; nothing reaches disk or the cache.
    ?assertEqual(OldConfig, emqx_plugins:get_config(NameVsn)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

name_vsn() ->
    %% application:load is idempotent and only loads the .app file (does
    %% not start the app), so it's safe to call here just to read the vsn
    %% key before the plugin manager takes over the install/start lifecycle.
    _ = application:load(emqx_acme),
    {ok, Vsn} = application:get_key(emqx_acme, vsn),
    iolist_to_binary(["emqx_acme-", Vsn]).

%% The tarball is built by `make plugin-emqx_acme` and lives under
%% _build/plugins/. Walk up from code:lib_dir(emqx_acme) until a `_build`
%% segment is found, then descend into _build/plugins/<NameVsn>.tar.gz.
prebuilt_tarball() ->
    LibDir = code:lib_dir(emqx_acme),
    BuildDir = build_dir_of(LibDir),
    Tarball = filename:join([BuildDir, "plugins", name_vsn_str() ++ ".tar.gz"]),
    case filelib:is_regular(Tarball) of
        true ->
            Tarball;
        false ->
            ct:fail(
                "prebuilt plugin tarball not found at ~ts; "
                "run `make plugin-emqx_acme` first",
                [Tarball]
            )
    end.

name_vsn_str() ->
    binary_to_list(name_vsn()).

build_dir_of(Path) ->
    case filename:basename(Path) of
        "_build" -> Path;
        _ -> build_dir_of(filename:dirname(Path))
    end.
