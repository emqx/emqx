%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Common Test Helper / Running test suites
%%
%% The purpose of this module is to run application-level, integration
%% tests in an isolated fashion.
%%
%% Isolation is this context means that each testrun does not leave any
%% persistent state accessible to following testruns. The goal is to
%% make testruns completely independent of each other, of the order in
%% which they are executed, and of the testrun granularity, i.e. whether
%% they are executed individually or as part of a larger suite. This
%% should help to increase reproducibility and reduce the risk of false
%% positives.
%%
%% Isolation is achieved through the following measures:
%% * Each testrun completely terminates and unload all applications
%%   started during the testrun.
%% * Each testrun is executed in a separate directory, usually under
%%   common_test's private directory, where all persistent state should
%%   be stored.
%% * Additionally, each cleans out few bits of persistent state that
%%   survives the above measures, namely persistent VM terms related
%%   to configuration and authentication (see `clean_suite_state/0`).
%%
%% Integration test in this context means a test that works with applications
%% as a whole, and needs to start and stop them as part of the test run.
%% For this, there's an abstraction called _appspec_ that describes how to
%% configure and start an application.
%%
%% The module also provides a set of default appspecs for some applications
%% that hide details and quirks of how to start them, to make it easier to
%% write test suites.
%%
%% Most of the time, you just need to:
%% 1. Describe the appspecs for the applications you want to test.
%% 2. Call `emqx_cth_suite:start/2` to start the applications before the testrun
%%    (e.g. in `init_per_suite/1` / `init_per_group/2`), providing the appspecs
%%    and unique work dir for the testrun (e.g. `work_dir/1`). Save the result
%%    in a context.
%% 3. Call `emqx_cth_suite:stop/1` to stop the applications after the testrun
%%    finishes (e.g. in `end_per_suite/1` / `end_per_group/2`), providing the
%%    result from step 2.
-module(emqx_cth_suite).

-include_lib("common_test/include/ct.hrl").

-export([start/2]).
-export([stop/1]).

-export([work_dir/1]).
-export([work_dir/2]).
-export([clean_work_dir/1]).

-export([load_apps/1]).
-export([start_apps/2]).
-export([start_app/2]).
-export([start_app/3]).
-export([stop_apps/1]).

-export([default_config/2]).
-export([merge_appspec/2]).
-export([merge_config/2]).

%% "Unofficial" `emqx_config_handler' and `emqx_conf' APIs
-export([schema_module/0, upgrade_raw_conf/1]).

-export([skip_if_oss/0]).

-export_type([appspec/0]).
-export_type([appspec_opts/0]).

-define(NOW,
    (calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]))
).

-define(PAL(IMPORTANCE, FMT, ARGS),
    case erlang:whereis(ct_logs) of
        undefined ->
            io:format("*** " ?MODULE_STRING " ~s @ ~p ***~n" ++ FMT ++ "~n", [?NOW, node() | ARGS]);
        _ ->
            ct:pal(?MODULE, IMPORTANCE, FMT, ARGS, [{heading, ?MODULE_STRING}])
    end
).

%%

-type appname() :: atom().
-type appspec() :: {appname(), appspec_opts()}.

%% Config structure serializable into HOCON document.
-type config() :: #{atom() => scalar() | [scalar()] | config() | [config()]}.
-type scalar() :: atom() | number() | string() | binary().

-type hookfun(R) ::
    fun(() -> R)
    | fun((appname()) -> R)
    | fun((appname(), appspec_opts()) -> R).

-type appspec_opts() :: #{
    %% 1. Perform anything right before starting the application
    %% If not defined or set to `false`, this step will be skipped.
    %% Merging amounts to redefining.
    before_start => hookfun(_) | false,

    %% 2. Enable loading application config
    %% If not defined or set to `false`, this step will be skipped.
    %% If application is missing a schema module, this step will fail.
    %% Merging amounts to appending, unless `false` is used, then merge result is also `false`.
    config => iodata() | config() | emqx_config:raw_config() | false,

    %% 3. Override the application environment
    %% If not defined or set to `false`, this step will be skipped.
    %% Merging amounts to appending, unless `false` is used, then merge result is `[]`.
    override_env => [{atom(), term()}] | false,

    %% 4. Starting the application
    %% If not defined or set to `true`, `application:ensure_all_started/1` is used.
    %% If custom function is used, it should return list of all applications that were started.
    %% If set to `false`, application will not be started.
    %% Merging amounts to redefining.
    start => hookfun({ok, [appname()]}) | boolean(),

    %% 5. Perform anything right after starting the application
    %% If not defined or set to `false`, this step will be skipped.
    %% Merging amounts to redefining.
    after_start => hookfun(_) | false
}.

%% @doc Start applications with a clean slate.
%% Provided appspecs will be merged with defaults defined in `default_appspec/1`.
-spec start([appname() | appspec()], SuiteOpts) ->
    StartedApps :: [appname()]
when
    SuiteOpts :: #{
        %% Working directory
        %% Everything a test produces should go here. If this directory is not empty,
        %% function will raise an error. Most of the time, the result of `work_dir/1`
        %% or `work_dir/2` (if used in a testcase) should be fine here.
        work_dir := file:name()
    }.
start(Apps, SuiteOpts = #{work_dir := WorkDir}) ->
    emqx_common_test_helpers:clear_screen(),
    % 1. Prepare appspec instructions
    AppSpecs = [mk_appspec(App, SuiteOpts) || App <- Apps],
    % 2. Load every app so that stuff scanning attributes of loaded modules works
    ok = lists:foreach(fun load_appspec/1, AppSpecs),
    % 3. Verify that we're running with a clean state.
    ok = filelib:ensure_dir(filename:join(WorkDir, foo)),
    ok = verify_clean_suite_state(SuiteOpts),
    % 4. Setup isolated mnesia directory
    ok = emqx_common_test_helpers:load(mnesia),
    ok = application:set_env(mnesia, dir, filename:join([WorkDir, mnesia])),
    ok = application:set_env(emqx_durable_storage, db_data_dir, filename:join([WorkDir, ds])),
    % 5. Start ekka separately.
    % For some reason it's designed to be started in non-regular way, so we have to track
    % applications started in the process manually.
    EkkaSpecs = [{App, proplists:get_value(App, AppSpecs, #{})} || App <- [gen_rpc, mria, ekka]],
    EkkaApps = start_apps(EkkaSpecs, SuiteOpts),
    % 6. Start apps following instructions.
    RestSpecs = [AppSpec || AppSpec <- AppSpecs, not lists:member(AppSpec, EkkaSpecs)],
    EkkaApps ++ start_appspecs(RestSpecs).

load_apps(Apps) ->
    lists:foreach(fun load_appspec/1, [mk_appspec(App, #{}) || App <- Apps]).

load_appspec({App, _Opts}) ->
    ok = emqx_common_test_helpers:load(App),
    load_app_deps(App).

load_app_deps(App) ->
    case application:get_key(App, applications) of
        {ok, Deps} ->
            Apps = [D || D <- Deps, application:get_key(D, id) == undefined],
            ok = lists:foreach(fun emqx_common_test_helpers:load/1, Apps),
            ok = lists:foreach(fun load_app_deps/1, Apps);
        undefined ->
            ok
    end.

start_apps(Apps, SuiteOpts) ->
    start_appspecs([mk_appspec(App, SuiteOpts) || App <- Apps]).

start_app(App, StartOpts) ->
    start_app(App, StartOpts, #{}).

start_app(App, StartOpts, SuiteOpts) ->
    start_appspecs([mk_appspec({App, StartOpts}, SuiteOpts)]).

start_appspecs(AppSpecs) ->
    lists:flatmap(
        fun({App, Spec}) -> start_appspec(App, Spec) end,
        AppSpecs
    ).

mk_appspec({App, Opts}, SuiteOpts) ->
    Defaults = default_appspec(App, SuiteOpts),
    {App, merge_appspec(Defaults, init_spec(Opts))};
mk_appspec(App, SuiteOpts) ->
    Defaults = default_appspec(App, SuiteOpts),
    {App, Defaults}.

init_spec(Opts = #{}) ->
    Opts;
init_spec(Config) when is_list(Config); is_binary(Config) ->
    #{config => [Config, "\n"]}.

start_appspec(App, StartOpts) ->
    _ = log_appspec(App, StartOpts),
    _ = maybe_before_start(App, StartOpts),
    _ = maybe_configure_app(App, StartOpts),
    _ = maybe_override_env(App, StartOpts),
    case maybe_start(App, StartOpts) of
        {ok, Started} ->
            ?PAL(?STD_IMPORTANCE, "Started applications: ~0p", [Started]),
            _ = maybe_after_start(App, StartOpts),
            Started;
        {error, Reason} ->
            error({failed_to_start_app, App, Reason})
    end.

log_appspec(App, StartOpts) when map_size(StartOpts) > 0 ->
    Fmt = lists:flatmap(
        fun(Opt) -> "~n * ~p: " ++ spec_fmt(fc, Opt) end,
        maps:keys(StartOpts)
    ),
    Args = lists:flatmap(
        fun({Opt, V}) -> [Opt, spec_fmt(ffun, {Opt, V})] end,
        maps:to_list(StartOpts)
    ),
    ?PAL(?STD_IMPORTANCE, "Starting ~p with:" ++ Fmt, [App | Args]);
log_appspec(App, #{}) ->
    ?PAL(?STD_IMPORTANCE, "Starting ~p", [App]).

spec_fmt(fc, config) -> "~n~ts";
spec_fmt(fc, _) -> "~p";
spec_fmt(ffun, {config, false}) -> "false (don't inhibit config loader)";
spec_fmt(ffun, {config, C}) -> render_config(C);
spec_fmt(ffun, {_, X}) -> X.

maybe_configure_app(_App, #{config := false}) ->
    ok;
maybe_configure_app(_App, AppConfig = #{schema_mod := SchemaModule}) when is_atom(SchemaModule) ->
    #{config := Config} = AppConfig,
    configure_app(SchemaModule, Config);
maybe_configure_app(App, #{config := Config}) ->
    case app_schema(App) of
        {ok, SchemaModule} ->
            configure_app(SchemaModule, Config);
        {error, Reason} ->
            error({failed_to_configure_app, App, Reason})
    end;
maybe_configure_app(_App, #{}) ->
    ok.

configure_app(SchemaModule, Config) ->
    ok = emqx_config:init_load(SchemaModule, render_config(Config)),
    ok.

maybe_override_env(App, #{override_env := Env = [{_, _} | _]}) ->
    ok = application:set_env([{App, Env}]);
maybe_override_env(_App, #{}) ->
    ok.

maybe_before_start(App, #{before_start := Fun} = Opts) when is_function(Fun) ->
    apply_hookfun(Fun, App, Opts);
maybe_before_start(_App, #{}) ->
    ok.

maybe_start(_App, #{start := false}) ->
    {ok, []};
maybe_start(App, #{start := Fun} = Opts) when is_function(Fun) ->
    apply_hookfun(Fun, App, Opts);
maybe_start(App, #{}) ->
    application:ensure_all_started(App).

maybe_after_start(App, #{after_start := Fun} = Opts) when is_function(Fun) ->
    apply_hookfun(Fun, App, Opts);
maybe_after_start(_App, #{}) ->
    ok.

apply_hookfun(Fun, _App, _Opts) when is_function(Fun, 0) ->
    Fun();
apply_hookfun(Fun, App, _Opts) when is_function(Fun, 1) ->
    Fun(App);
apply_hookfun(Fun, App, Opts) when is_function(Fun, 2) ->
    Fun(App, Opts).

-spec merge_appspec(appspec_opts(), appspec_opts()) ->
    appspec_opts().
merge_appspec(Opts1, Opts2) ->
    maps:merge_with(
        fun
            (config, C1, C2) -> merge_config(C1, C2);
            (override_env, E1, E2) -> merge_envs(E1, E2);
            (_Opt, _Val1, Val2) -> Val2
        end,
        init_spec(Opts1),
        init_spec(Opts2)
    ).

merge_envs(false, E2) ->
    E2;
merge_envs(_E, false) ->
    [];
merge_envs(E1, E2) ->
    lists:foldl(fun({K, _} = Opt, EAcc) -> lists:keystore(K, 1, EAcc, Opt) end, E1, E2).

merge_config(false, C2) ->
    C2;
merge_config(_C, false) ->
    false;
merge_config(C1, C2) ->
    [render_config(C1), "\n", render_config(C2)].

default_appspec(ekka, _SuiteOpts) ->
    #{
        start => fun start_ekka/0
    };
default_appspec(emqx, SuiteOpts) ->
    #{
        override_env => [{data_dir, maps:get(work_dir, SuiteOpts, "data")}],
        % NOTE
        % We inform `emqx` of our config loader before starting it so that it won't
        % overwrite everything with a default configuration.
        before_start => fun inhibit_config_loader/2
    };
default_appspec(emqx_conf, SuiteOpts) ->
    Config = #{
        node => #{
            name => node(),
            cookie => erlang:get_cookie(),
            data_dir => unicode:characters_to_binary(maps:get(work_dir, SuiteOpts, "data"))
        }
    },
    % NOTE
    % Since `emqx_conf_schema` manages config for a lot of applications, it's good to include
    % their defaults as well.
    SharedConfig = lists:foldl(
        fun(App, Acc) ->
            emqx_utils_maps:deep_merge(Acc, default_config(App, SuiteOpts))
        end,
        Config,
        [emqx]
    ),
    #{
        config => SharedConfig,
        before_start => fun(App, Conf) ->
            % NOTE
            % We inform `emqx` of our config loader before starting `emqx_conf` so that it won't
            % overwrite everything with a default configuration.
            ok = inhibit_config_loader(App, Conf),
            % NOTE
            % This should be done to pass authz schema validations.
            % In production, acl.conf file is created by the release process.
            ok = emqx_common_test_helpers:copy_acl_conf()
        end
    };
default_appspec(emqx_dashboard, _SuiteOpts) ->
    #{
        after_start => fun() ->
            true = emqx_dashboard_listener:is_ready(infinity)
        end
    };
default_appspec(App, _SuiteOpts) when
    App == emqx_schema_registry;
    App == emqx_schema_validation;
    App == emqx_message_transformation
->
    %% NOTE: Start those apps with default configuration.
    #{config => #{}};
default_appspec(_, _) ->
    #{}.

default_config(App, SuiteOpts) ->
    maps:get(config, default_appspec(App, SuiteOpts), #{}).

%%

%% @doc Determine the unique work directory for the current test run.
%% Takes into account name of the test suite, and all test groups the current run
%% is part of.
-spec work_dir(CTConfig :: proplists:proplist()) ->
    file:filename_all().
work_dir(CTConfig) ->
    % Directory specific to the current test run.
    [PrivDir] = proplists:get_all_values(priv_dir, CTConfig),
    % Directory specific to the currently executing test suite.
    [DataDir] = proplists:get_all_values(data_dir, CTConfig),
    % NOTE: Contains the name of the current test group, if executed as part of a group.
    GroupProps = proplists:get_value(tc_group_properties, CTConfig, []),
    % NOTE: Contains names of outer test groups, if any.
    GroupPathOuter = proplists:get_value(tc_group_path, CTConfig, []),
    SuiteDir = filename:basename(DataDir),
    GroupPath = lists:append([GroupProps | GroupPathOuter]),
    GroupLevels = [atom_to_list(Name) || {name, Name} <- GroupPath],
    WorkDir1 = filename:join(PrivDir, SuiteDir),
    WorkDir2 =
        case GroupLevels of
            [] ->
                WorkDir1;
            [_ | _] ->
                GroupDir = string:join(lists:reverse(GroupLevels), "."),
                filename:join(WorkDir1, GroupDir)
        end,
    WorkDir2.

%% @doc Determine the unique work directory for the current testcase run.
%% Be careful when testcase runs under no groups, and its name matches the name of a
%% previously executed test group, it's best to avoid such naming.
-spec work_dir(TestCaseName :: atom(), CTConfig :: proplists:proplist()) ->
    file:filename_all().
work_dir(TCName, CTConfig) ->
    WorkDir = work_dir(CTConfig),
    filename:join(WorkDir, TCName).

%% @doc Delete contents of the workdir.
clean_work_dir(WorkDir) ->
    ct:pal("Cleaning workdir ~p", [WorkDir]),
    case re:run(WorkDir, "_build/test/logs/") of
        {match, _} ->
            file:del_dir_r(WorkDir);
        nomatch ->
            error({unsafe_workdir, WorkDir})
    end.

%%

start_ekka() ->
    ok = emqx_common_test_helpers:start_ekka(),
    {ok, [mnesia, ekka]}.

inhibit_config_loader(_App, #{config := Config}) when Config /= false ->
    ok = emqx_app:set_config_loader(?MODULE);
inhibit_config_loader(_App, #{}) ->
    ok.

%%

-spec stop(_StartedApps :: [appname()]) ->
    ok.
stop(Apps) ->
    ok = stop_apps(Apps),
    clean_suite_state().

-spec stop_apps(_StartedApps :: [appname()]) ->
    ok.
stop_apps(Apps) ->
    ok = lists:foreach(fun application:stop/1, lists:reverse(Apps)),
    ok = lists:foreach(fun application:unload/1, Apps).

%%

verify_clean_suite_state(#{boot_type := restart}) ->
    %% when testing node restart, we do not need to verify clean state
    ok;
verify_clean_suite_state(#{work_dir := WorkDir}) ->
    {ok, []} = file:list_dir(WorkDir),
    false = emqx_schema_hooks:any_injections(),
    [] = emqx_config:get_root_names(),
    ok.

clean_suite_state() ->
    _ = emqx_schema_hooks:erase_injections(),
    _ = emqx_config:erase_all(),
    ok.

%%

app_schema(App) ->
    Mod = list_to_atom(atom_to_list(App) ++ "_schema"),
    try
        Exports = Mod:module_info(exports),
        case lists:member({roots, 0}, Exports) of
            true -> {ok, Mod};
            false -> {error, schema_no_roots}
        end
    catch
        error:undef ->
            {error, schema_not_found}
    end.

render_config(Config = #{}) ->
    unicode:characters_to_binary(hocon_pp:do(Config, #{}));
render_config(Config) ->
    unicode:characters_to_binary(Config).

%%

%% "Unofficial" `emqx_config_handler' API
schema_module() ->
    ?MODULE.

%% "Unofficial" `emqx_conf' API
upgrade_raw_conf(Conf) ->
    case emqx_release:edition() of
        ee ->
            emqx_enterprise_schema:upgrade_raw_conf(Conf);
        ce ->
            emqx_conf_schema:upgrade_raw_conf(Conf)
    end.

skip_if_oss() ->
    try emqx_release:edition() of
        ee ->
            false;
        _ ->
            {skip, not_supported_in_oss}
    catch
        error:undef ->
            {skip, standalone_not_supported}
    end.
