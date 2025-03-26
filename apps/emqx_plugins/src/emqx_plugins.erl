%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins).

-feature(maybe_expr, enable).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    describe/1,
    plugin_schema/1,
    plugin_i18n/1
]).

%% Package operations
-export([
    allow_installation/1,
    forget_allowed_installation/1,
    is_allowed_installation/1,

    ensure_installed/0,
    ensure_installed/1,
    ensure_installed/2,
    ensure_uninstalled/1,
    ensure_enabled/1,
    ensure_enabled/2,
    ensure_enabled/3,
    ensure_disabled/1,
    purge/1,
    delete_package/1
]).

%% Plugin runtime management
-export([
    ensure_started/0,
    ensure_started/1,
    ensure_stopped/0,
    ensure_stopped/1,
    restart/1,
    list/0,
    list/1
]).

%% Plugin config APIs
-export([
    get_config/1,
    get_config/2,
    put_config/3
]).

%% Package utils
-export([
    decode_plugin_config_map/2,
    with_plugin_avsc/1,
    ensure_ssl_files/2,
    ensure_ssl_files/3
]).

%% `emqx_config_handler' API
-export([
    post_config_update/5
]).

%% internal RPC targets
-export([
    get_tar/1,
    get_config/3
]).

%% Internal export
-export([
    ensure_config_map/1,
    do_ensure_started/1
]).
%% for test cases
-export([put_config_internal/2]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% Defines
-define(PLUGIN_PERSIS_CONFIG_KEY(NameVsn), {?MODULE, NameVsn}).

-define(RAW_BIN, binary).
-define(JSON_MAP, json_map).

-define(MAX_KEEP_BACKUP_CONFIGS, 10).

-define(CATCH(BODY), catch_errors(atom_to_list(?FUNCTION_NAME), fun() -> BODY end)).

-define(APP, emqx_plugins).

-define(allowed_installations, allowed_installations).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Describe a plugin.
-spec describe(name_vsn()) -> {ok, plugin_info()} | {error, any()}.
describe(NameVsn) ->
    read_plugin_info(NameVsn, #{fill_readme => true}).

-spec plugin_schema(name_vsn()) -> {ok, schema_json_map()} | {error, any()}.
plugin_schema(NameVsn) ->
    ?CATCH(emqx_plugins_fs:read_avsc_map(NameVsn)).

-spec plugin_i18n(name_vsn()) -> {ok, i18n_json_map()} | {error, any()}.
plugin_i18n(NameVsn) ->
    ?CATCH(emqx_plugins_fs:read_i18n(NameVsn)).

%% Note: this is only used for the HTTP API.
%% We could use `application:set_env', but the typespec for it makes dialyzer sad when it
%% seems a non-atom key...
-spec allow_installation(binary() | string()) -> ok.
allow_installation(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    Allowed0 = application:get_env(?APP, ?allowed_installations, #{}),
    Allowed = Allowed0#{NameVsn => true},
    application:set_env(?APP, ?allowed_installations, Allowed),
    ok.

%% Note: this is only used for the HTTP API.
-spec is_allowed_installation(binary() | string()) -> boolean().
is_allowed_installation(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    Allowed = application:get_env(?APP, ?allowed_installations, #{}),
    maps:get(NameVsn, Allowed, false).

%% Note: this is only used for the HTTP API.
-spec forget_allowed_installation(binary() | string()) -> ok.
forget_allowed_installation(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    Allowed0 = application:get_env(?APP, ?allowed_installations, #{}),
    Allowed = maps:remove(NameVsn, Allowed0),
    application:set_env(?APP, ?allowed_installations, Allowed),
    ok.

%%--------------------------------------------------------------------
%% Package operations

%% @doc Start all configured plugins are started.
-spec ensure_installed() -> ok.
ensure_installed() ->
    Fun = fun(#{name_vsn := NameVsn}) ->
        case ensure_installed(NameVsn) of
            ok -> [];
            {error, Reason} -> [{NameVsn, Reason}]
        end
    end,
    ok = for_plugins(Fun).

%% @doc Install a .tar.gz package placed in install_dir.
-spec ensure_installed(name_vsn()) -> ok | {error, map()}.
ensure_installed(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, _} ->
            ok,
            _ = maybe_ensure_plugin_config(NameVsn, ?normal);
        {error, _} ->
            ok = purge(NameVsn),
            case ensure_exists_and_installed(NameVsn) of
                ok ->
                    maybe_post_op_after_installed(NameVsn, ?normal),
                    ok;
                {error, _Reason} = Err ->
                    Err
            end
    end.

ensure_installed(NameVsn, ?fresh_install = Mode) ->
    case ensure_exists_and_installed(NameVsn) of
        ok ->
            maybe_post_op_after_installed(NameVsn, Mode),
            ok;
        {error, _Reason} = Err ->
            Err
    end.

%% @doc Ensure files and directories for the given plugin are being deleted.
%% If a plugin is running, or enabled, an error is returned.
-spec ensure_uninstalled(name_vsn()) -> ok | {error, any()}.
ensure_uninstalled(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{running_status := RunningSt}} when RunningSt =/= stopped ->
            {error, #{
                msg => "bad_plugin_running_status",
                hint => "stop_the_plugin_first"
            }};
        {ok, #{config_status := enabled}} ->
            {error, #{
                msg => "bad_plugin_config_status",
                hint => "disable_the_plugin_first"
            }};
        _ ->
            purge(NameVsn),
            ensure_delete(NameVsn)
    end.

%% @doc Ensure a plugin is enabled to the end of the plugins list.
-spec ensure_enabled(name_vsn()) -> ok | {error, any()}.
ensure_enabled(NameVsn) ->
    ensure_enabled(NameVsn, no_move).

%% @doc Ensure a plugin is enabled at the given position of the plugin list.
-spec ensure_enabled(name_vsn(), position()) -> ok | {error, any()}.
ensure_enabled(NameVsn, Position) ->
    ensure_state(NameVsn, Position, _Enabled = true, _ConfLocation = local).

-spec ensure_enabled(name_vsn(), position(), local | global) -> ok | {error, any()}.
ensure_enabled(NameVsn, Position, ConfLocation) when
    ConfLocation =:= local; ConfLocation =:= global
->
    ensure_state(NameVsn, Position, _Enabled = true, ConfLocation).

%% @doc Ensure a plugin is disabled.
-spec ensure_disabled(name_vsn()) -> ok | {error, any()}.
ensure_disabled(NameVsn) ->
    ensure_state(NameVsn, no_move, false, _ConfLocation = local).

%% @doc Delete extracted dir
%% In case one lib is shared by multiple plugins.
%% it might be the case that purging one plugin's install dir
%% will cause deletion of loaded beams.
%% It should not be a problem, because shared lib should
%% reside in all the plugin install dirs.
-spec purge(name_vsn()) -> ok.
purge(NameVsn) ->
    _ = maybe_purge_plugin_config(NameVsn),
    emqx_plugins_fs:purge_installed(NameVsn).

%% @doc Delete the package file.
-spec delete_package(name_vsn()) -> ok.
delete_package(NameVsn) ->
    _ = emqx_plugins_serde:delete_schema(NameVsn),
    emqx_plugins_fs:delete_tar(NameVsn).

%%--------------------------------------------------------------------
%% Plugin runtime management

%% @doc Start all configured plugins are started.
-spec ensure_started() -> ok.
ensure_started() ->
    Fun = fun
        (#{name_vsn := NameVsn, enable := true}) ->
            case ?CATCH(do_ensure_started(NameVsn)) of
                ok -> [];
                {error, Reason} -> [{NameVsn, Reason}]
            end;
        (#{name_vsn := NameVsn, enable := false}) ->
            ?SLOG(debug, #{msg => "plugin_disabled", name_vsn => NameVsn}),
            []
    end,
    ok = for_plugins(Fun).

%% @doc Start a plugin from Management API or CLI.
%% the input is a <name>-<vsn> string.
-spec ensure_started(name_vsn()) -> ok | {error, term()}.
ensure_started(NameVsn) ->
    case ?CATCH(do_ensure_started(NameVsn)) of
        ok ->
            ok;
        {error, ReasonMap} ->
            ?SLOG(error, ReasonMap#{msg => "failed_to_start_plugin"}),
            {error, ReasonMap}
    end.

%% @doc Stop all plugins before broker stops.
-spec ensure_stopped() -> ok.
ensure_stopped() ->
    Fun = fun
        (#{name_vsn := NameVsn, enable := true}) ->
            case ensure_stopped(NameVsn) of
                ok ->
                    [];
                {error, Reason} ->
                    [{NameVsn, Reason}]
            end;
        (#{name_vsn := NameVsn, enable := false}) ->
            ?SLOG(debug, #{msg => "plugin_disabled", action => stop_plugin, name_vsn => NameVsn}),
            []
    end,
    ok = for_plugins(Fun).

%% @doc Stop a plugin from Management API or CLI.
-spec ensure_stopped(name_vsn()) -> ok | {error, term()}.
ensure_stopped(NameVsn) ->
    ?CATCH(do_ensure_stopped(NameVsn)).

do_ensure_stopped(NameVsn) ->
    case do_read_plugin(NameVsn) of
        {ok, Plugin} ->
            ensure_apps_stopped(Plugin);
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_config(name_vsn()) -> plugin_config_map().
get_config(NameVsn) ->
    get_config(NameVsn, #{}).

-spec get_config(name_vsn(), term()) -> plugin_config_map().
get_config(NameVsn, Default) ->
    persistent_term:get(?PLUGIN_PERSIS_CONFIG_KEY(bin(NameVsn)), Default).

%% @doc Update plugin's config.
%% RPC call from Management API or CLI.
%% The plugin config Json Map was valid by avro schema
%% Or: if no and plugin config ALWAYS be valid before calling this function.
put_config(NameVsn, ConfigJsonMap, AvroValue) when (not is_binary(NameVsn)) ->
    put_config(bin(NameVsn), ConfigJsonMap, AvroValue);
put_config(NameVsn, ConfigJsonMap, _AvroValue) ->
    HoconBin = hocon_pp:do(ConfigJsonMap, #{}),
    ok = backup_and_write_hocon_bin(NameVsn, HoconBin),
    %% TODO: callback in plugin's on_config_upgraded (config vsn upgrade v1 -> v2)
    ok = maybe_call_on_config_changed(NameVsn, ConfigJsonMap),
    ok = persistent_term:put(?PLUGIN_PERSIS_CONFIG_KEY(NameVsn), ConfigJsonMap),
    ok.

%% @doc Stop and then start the plugin.
restart(NameVsn) ->
    case ensure_stopped(NameVsn) of
        ok -> ensure_started(NameVsn);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Call plugin's callback on_config_changed/2
maybe_call_on_config_changed(NameVsn, NewConf) ->
    FuncName = on_config_changed,
    maybe
        {ok, PluginAppModule} ?= app_module_name(NameVsn),
        true ?= erlang:function_exported(PluginAppModule, FuncName, 2),
        {ok, OldConf} = get_config(NameVsn),
        try erlang:apply(PluginAppModule, FuncName, [OldConf, NewConf]) of
            _ -> ok
        catch
            Class:CatchReason:Stacktrace ->
                ?SLOG(error, #{
                    msg => "failed_to_call_on_config_changed",
                    exception => Class,
                    reason => CatchReason,
                    stacktrace => Stacktrace
                }),
                ok
        end
    else
        {error, Reason} ->
            ?SLOG(info, #{msg => "failed_to_call_on_config_changed", reason => Reason});
        false ->
            ?SLOG(info, #{msg => "on_config_changed_callback_not_exported"});
        _ ->
            ok
    end.

app_module_name(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{<<"name">> := Name} = _PluginInfo} ->
            emqx_utils:safe_to_existing_atom(<<Name/binary, "_app">>);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "failed_to_read_plugin_info"}),
            {error, Reason}
    end.

%% @doc List all installed plugins.
%% Including the ones that are installed, but not enabled in config.
-spec list() -> [plugin_info()].
list() ->
    list(normal).

-spec list(all | normal | hidden) -> [plugin_info()].
list(Type) ->
    All = lists:filtermap(
        fun(NameVsn) ->
            case read_plugin_info(NameVsn, #{}) of
                {ok, Info} ->
                    filter_plugin_of_type(Type, Info);
                {error, Reason} ->
                    ?SLOG(warning, Reason#{msg => "failed_to_read_plugin_info"}),
                    false
            end
        end,
        emqx_plugins_fs:list_name_vsn()
    ),
    do_list(configured(), All).

filter_plugin_of_type(all, Info) ->
    {true, Info};
filter_plugin_of_type(normal, #{<<"hidden">> := true}) ->
    false;
filter_plugin_of_type(normal, Info) ->
    {true, Info};
filter_plugin_of_type(hidden, #{<<"hidden">> := true} = Info) ->
    {true, Info};
filter_plugin_of_type(hidden, _Info) ->
    false.

%%--------------------------------------------------------------------
%% Package utils

-spec decode_plugin_config_map(name_vsn(), map() | binary()) ->
    {ok, map() | ?plugin_without_config_schema}
    | {error, any()}.
decode_plugin_config_map(NameVsn, AvroJsonMap) ->
    case with_plugin_avsc(NameVsn) of
        true ->
            case emqx_plugins_serde:lookup_serde(NameVsn) of
                {error, not_found} ->
                    Reason = "plugin_config_schema_serde_not_found",
                    ?SLOG(error, #{
                        msg => Reason, name_vsn => NameVsn, plugin_with_avro_schema => true
                    }),
                    {error, Reason};
                {ok, _Serde} ->
                    do_decode_plugin_config_map(NameVsn, AvroJsonMap)
            end;
        false ->
            ?SLOG(debug, #{
                msg => "plugin_without_config_schema",
                name_vsn => NameVsn
            }),
            {ok, ?plugin_without_config_schema}
    end.

do_decode_plugin_config_map(NameVsn, AvroJsonMap) when is_map(AvroJsonMap) ->
    do_decode_plugin_config_map(NameVsn, emqx_utils_json:encode(AvroJsonMap));
do_decode_plugin_config_map(NameVsn, AvroJsonBin) ->
    case emqx_plugins_serde:decode(NameVsn, AvroJsonBin) of
        {ok, Config} -> {ok, Config};
        {error, ReasonMap} -> {error, ReasonMap}
    end.

-spec with_plugin_avsc(name_vsn()) -> boolean().
with_plugin_avsc(NameVsn) ->
    case read_plugin_info(NameVsn, #{fill_readme => false}) of
        {ok, #{<<"with_config_schema">> := WithAvsc}} when is_boolean(WithAvsc) ->
            WithAvsc;
        _ ->
            false
    end.

get_config_interal(Key, Default) when is_atom(Key) ->
    get_config_interal([Key], Default);
get_config_interal(Path, Default) ->
    emqx_conf:get([?CONF_ROOT | Path], Default).

put_config_internal(Key, Value) ->
    do_put_config_internal(Key, Value, _ConfLocation = local).

ensure_ssl_files(NameVsn, SSL) ->
    emqx_tls_lib:ensure_ssl_files(emqx_plugins_fs:plugin_certs_dir(NameVsn), SSL).

ensure_ssl_files(NameVsn, SSL, Opts) ->
    emqx_tls_lib:ensure_ssl_files(emqx_plugins_fs:plugin_certs_dir(NameVsn), SSL, Opts).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

ensure_delete(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    List = configured(),
    put_configured(lists:filter(fun(#{name_vsn := N1}) -> bin(N1) =/= NameVsn end, List)),
    ok.

ensure_state(NameVsn, Position, State, ConfLocation) when is_binary(NameVsn) ->
    ensure_state(binary_to_list(NameVsn), Position, State, ConfLocation);
ensure_state(NameVsn, Position, State, ConfLocation) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, _} ->
            Item = #{
                name_vsn => NameVsn,
                enable => State
            },
            ?CATCH(ensure_configured(Item, Position, ConfLocation));
        {error, Reason} ->
            ?SLOG(error, #{msg => "ensure_plugin_states_failed", reason => Reason}),
            {error, Reason}
    end.

ensure_configured(#{name_vsn := NameVsn} = Item, Position, ConfLocation) ->
    Configured = configured(),
    SplitFun = fun(#{name_vsn := NV}) -> bin(NV) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    try
        NewConfigured =
            case Rear of
                [_ | More] when Position =:= no_move ->
                    Front ++ [Item | More];
                [_ | More] ->
                    add_new_configured(Front ++ More, Position, Item);
                [] ->
                    add_new_configured(Configured, Position, Item)
            end,
        ok = put_configured(NewConfigured, ConfLocation)
    catch
        throw:Reason ->
            {error, Reason}
    end.

add_new_configured(Configured, no_move, Item) ->
    %% default to rear
    add_new_configured(Configured, rear, Item);
add_new_configured(Configured, front, Item) ->
    [Item | Configured];
add_new_configured(Configured, rear, Item) ->
    Configured ++ [Item];
add_new_configured(Configured, {Action, NameVsn}, Item) ->
    SplitFun = fun(#{name_vsn := NV}) -> bin(NV) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    Rear =:= [] andalso
        throw(#{
            msg => "position_anchor_plugin_not_configured",
            hint => "maybe_install_and_configure",
            name_vsn => NameVsn
        }),
    case Action of
        before ->
            Front ++ [Item | Rear];
        behind ->
            [Anchor | Rear0] = Rear,
            Front ++ [Anchor, Item | Rear0]
    end.

maybe_purge_plugin_config(NameVsn) ->
    _ = persistent_term:erase(?PLUGIN_PERSIS_CONFIG_KEY(NameVsn)),
    ok.

%% Make sure configured ones are ordered in front.
do_list([], All) ->
    All;
do_list([#{name_vsn := NameVsn} | Rest], All) ->
    SplitF = fun(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
        bin([Name, "-", Vsn]) =/= bin(NameVsn)
    end,
    case lists:splitwith(SplitF, All) of
        {_, []} ->
            do_list(Rest, All);
        {Front, [I | Rear]} ->
            [I | do_list(Rest, Front ++ Rear)]
    end.

do_ensure_started(NameVsn) ->
    maybe
        ok ?= ensure_exists_and_installed(NameVsn),
        {ok, Plugin} ?= do_read_plugin(NameVsn),
        ok ?= load_code_start_apps(NameVsn, Plugin)
    else
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_plugin",
                name_vsn => NameVsn,
                reason => Reason
            }),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% RPC targets
%%--------------------------------------------------------------------

%% Redundant arguments are kept for backward compatibility.
-spec get_config(name_vsn(), ?CONFIG_FORMAT_MAP, any()) ->
    {ok, plugin_config_map() | term()}.
get_config(NameVsn, ?CONFIG_FORMAT_MAP, Default) ->
    {ok, get_config(NameVsn, Default)}.

get_tar(NameVsn) ->
    emqx_plugins_fs:get_tar(NameVsn).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc Converts exception errors into `{error, Reason}` return values
catch_errors(Label, F) ->
    try
        F()
    catch
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_op_failed",
                which_op => Label,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, #{
                which_op => Label,
                exception => Reason,
                stacktrace => Stacktrace
            }}
    end.

%% read plugin info from the JSON file
%% returns {ok, Info} or {error, Reason}
read_plugin_info(NameVsn, Options) ->
    do_read_plugin(NameVsn, Options).

do_read_plugin(NameVsn) ->
    do_read_plugin(NameVsn, #{}).

do_read_plugin(NameVsn, Options) ->
    maybe
        {ok, Info0} ?= emqx_plugins_fs:read_info(NameVsn),
        ok ?= check_plugin(Info0, NameVsn, emqx_plugins_fs:info_file_path(NameVsn)),
        Info1 = populate_plugin_readme(NameVsn, Options, Info0),
        Info2 = populate_plugin_package_info(NameVsn, Info1),
        Info = populate_plugin_status(NameVsn, Info2),
        {ok, Info}
    end.

ensure_installed_locally(NameVsn) ->
    emqx_plugins_fs:ensure_installed(
        NameVsn,
        fun() ->
            case do_read_plugin(NameVsn) of
                {ok, _} ->
                    ok;
                {error, _} = Error ->
                    Error
            end
        end
    ).

ensure_exists_and_installed(NameVsn) ->
    case ensure_installed_locally(NameVsn) of
        ok ->
            ok;
        {error, #{reason := plugin_tarball_not_found}} ->
            do_get_from_cluster(NameVsn);
        {error, Reason} ->
            {error, Reason}
    end.

do_get_from_cluster(NameVsn) ->
    Nodes = [N || N <- mria:running_nodes(), N /= node()],
    case get_plugin_tar_from_any_node(Nodes, NameVsn, []) of
        {ok, TarContent} ->
            ok = emqx_plugins_fs:write_tar(NameVsn, TarContent),
            ok = ensure_installed_locally(NameVsn);
        {error, NodeErrors} when Nodes =/= [] ->
            ErrMeta = #{
                msg => "failed_to_copy_plugin_from_other_nodes",
                name_vsn => NameVsn,
                node_errors => NodeErrors,
                reason => plugin_not_found
            },
            ?SLOG(error, ErrMeta),
            {error, ErrMeta};
        {error, _} ->
            ErrMeta = #{
                msg => "no_nodes_to_copy_plugin_from",
                name_vsn => NameVsn,
                reason => plugin_not_found
            },
            ?SLOG(error, ErrMeta),
            {error, ErrMeta}
    end.

get_plugin_tar_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_plugin_tar_from_any_node([Node | T], NameVsn, Errors) ->
    case emqx_plugins_proto_v2:get_tar(Node, NameVsn, infinity) of
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_tar_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_plugin_tar_from_any_node(T, NameVsn, [{Node, Err} | Errors])
    end.

get_plugin_config_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_plugin_config_from_any_node([Node | RestNodes], NameVsn, Errors) ->
    case
        emqx_plugins_proto_v2:get_config(
            Node, NameVsn, ?CONFIG_FORMAT_MAP, ?plugin_conf_not_found, 5_000
        )
    of
        {ok, ?plugin_conf_not_found} ->
            Err = {error, {config_not_found_on_node, Node, NameVsn}},
            get_plugin_config_from_any_node(RestNodes, NameVsn, [{Node, Err} | Errors]);
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_config_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_plugin_config_from_any_node(RestNodes, NameVsn, [{Node, Err} | Errors])
    end.

populate_plugin_package_info(NameVsn, Info) ->
    Info#{md5sum => emqx_plugins_fs:read_md5sum(NameVsn)}.

populate_plugin_readme(NameVsn, #{fill_readme := true}, Info) ->
    Info#{readme => emqx_plugins_fs:read_readme(NameVsn)};
populate_plugin_readme(_NameVsn, _Options, Info) ->
    Info.

populate_plugin_status(NameVsn, Info) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    RunningSt =
        case application:get_key(AppName, vsn) of
            {ok, _} ->
                case lists:keyfind(AppName, 1, running_apps()) of
                    {AppName, _} -> running;
                    _ -> loaded
                end;
            undefined ->
                stopped
        end,
    Configured = lists:filtermap(
        fun(#{name_vsn := NV, enable := St}) ->
            case bin(NV) =:= bin(NameVsn) of
                true -> {true, St};
                false -> false
            end
        end,
        configured()
    ),
    ConfSt =
        case Configured of
            [] -> not_configured;
            [true] -> enabled;
            [false] -> disabled
        end,
    Info#{
        running_status => RunningSt,
        config_status => ConfSt
    }.

check_plugin(
    #{
        <<"name">> := Name,
        <<"rel_vsn">> := Vsn,
        <<"rel_apps">> := Apps,
        <<"description">> := _
    },
    NameVsn,
    FilePath
) ->
    case bin(NameVsn) =:= bin([Name, "-", Vsn]) of
        true ->
            try
                %% assert
                [_ | _] = Apps,
                %% validate if the list is all <app>-<vsn> strings
                lists:foreach(
                    fun(App) -> _ = emqx_plugins_utils:parse_name_vsn(App) end, Apps
                )
            catch
                _:_ ->
                    {error, #{
                        msg => "bad_rel_apps",
                        rel_apps => Apps,
                        hint => "A non-empty string list of app_name-app_vsn format"
                    }}
            end;
        false ->
            {error, #{
                msg => "name_vsn_mismatch",
                name_vsn => NameVsn,
                path => FilePath,
                name => Name,
                rel_vsn => Vsn
            }}
    end;
check_plugin(_What, NameVsn, FilePath) ->
    {error, #{
        msg => "bad_info_file_content",
        mandatory_fields => [rel_vsn, name, rel_apps, description],
        name_vsn => NameVsn,
        path => FilePath
    }}.

load_code_start_apps(RelNameVsn, #{<<"rel_apps">> := Apps}) ->
    LibDir = emqx_plugins_fs:lib_dir(RelNameVsn),
    RunningApps = running_apps(),
    %% load plugin apps and beam code
    try
        AppNames =
            lists:map(
                fun(AppNameVsn) ->
                    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(AppNameVsn),
                    EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
                    case load_plugin_app(AppName, AppVsn, EbinDir, RunningApps) of
                        ok -> AppName;
                        {error, Reason} -> throw(Reason)
                    end
                end,
                Apps
            ),
        ok = lists:foreach(
            fun(AppName) ->
                case start_app(AppName) of
                    ok -> ok;
                    {error, Reason} -> throw(Reason)
                end
            end,
            AppNames
        )
    catch
        throw:Reason ->
            {error, Reason}
    end.

load_plugin_app(AppName, AppVsn, Ebin, RunningApps) ->
    case lists:keyfind(AppName, 1, RunningApps) of
        false ->
            do_load_plugin_app(AppName, Ebin);
        {_, Vsn} ->
            case bin(Vsn) =:= bin(AppVsn) of
                true ->
                    %% already started on the exact version
                    ok;
                false ->
                    %% running but a different version
                    ?SLOG(warning, #{
                        msg => "plugin_app_already_running",
                        name => AppName,
                        running_vsn => Vsn,
                        loading_vsn => AppVsn
                    }),
                    ok
            end
    end.

do_load_plugin_app(AppName, Ebin) when is_binary(Ebin) ->
    do_load_plugin_app(AppName, binary_to_list(Ebin));
do_load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    maybe
        ok ?= load_modules(Modules),
        ok ?= application:load(AppName)
    else
        {error, {already_loaded, _}} ->
            ok;
        {error, Reason} ->
            {error, #{
                msg => "failed_to_load_plugin_app",
                name => AppName,
                reason => Reason
            }}
    end.

load_modules([]) ->
    ok;
load_modules([BeamFile | Modules]) ->
    Module = list_to_atom(filename:basename(BeamFile, ".beam")),
    _ = code:purge(Module),
    case code:load_file(Module) of
        {module, _} ->
            load_modules(Modules);
        {error, Reason} ->
            {error, #{msg => "failed_to_load_plugin_beam", path => BeamFile, reason => Reason}}
    end.

start_app(App) ->
    case run_with_timeout(application, ensure_all_started, [App], 10_000) of
        {ok, {ok, Started}} ->
            case Started =/= [] of
                true -> ?SLOG(debug, #{msg => "started_plugin_apps", apps => Started});
                false -> ok
            end;
        {ok, {error, Reason}} ->
            {error, #{
                msg => "failed_to_start_app",
                app => App,
                reason => Reason
            }};
        {error, Reason} ->
            {error, #{
                msg => "failed_to_start_plugin_app",
                app => App,
                reason => Reason
            }}
    end.

%% Stop all apps installed by the plugin package,
%% but not the ones shared with others.
ensure_apps_stopped(#{<<"rel_apps">> := Apps}) ->
    %% load plugin apps and beam code
    AppsToStop = lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps),
    case stop_apps(AppsToStop) of
        {ok, []} ->
            %% all apps stopped
            ok;
        {ok, Left} ->
            ?SLOG(warning, #{
                msg => "unabled_to_stop_plugin_apps",
                apps => Left,
                reason => "running_apps_still_depends_on_this_apps"
            }),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% On one hand, Elixir plugins might include Elixir itself, when targetting a non-Elixir
%% EMQX release.  If, on the other hand, the EMQX release already includes Elixir, we
%% shouldn't stop Elixir nor IEx.
-ifdef(EMQX_ELIXIR).
is_protected_app(elixir) -> true;
is_protected_app(iex) -> true;
is_protected_app(_) -> false.

parse_name_vsn_for_stopping(NameVsn) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    case is_protected_app(AppName) of
        true ->
            false;
        false ->
            {true, AppName}
    end.
%% ELSE ifdef(EMQX_ELIXIR)
-else.
parse_name_vsn_for_stopping(NameVsn) ->
    {AppName, _AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    {true, AppName}.
%% END ifdef(EMQX_ELIXIR)
-endif.

stop_apps(Apps) ->
    RunningApps = running_apps(),
    case do_stop_apps(Apps, [], RunningApps) of
        %% all stopped
        {ok, []} -> {ok, []};
        %% no progress
        {ok, Remain} when Remain =:= Apps -> {ok, Apps};
        %% try again
        {ok, Remain} -> stop_apps(Remain);
        {error, Reason} -> {error, Reason}
    end.

do_stop_apps([], Remain, _AllApps) ->
    {ok, lists:reverse(Remain)};
do_stop_apps([App | Apps], Remain, RunningApps) ->
    case is_needed_by_any(App, RunningApps) of
        true ->
            do_stop_apps(Apps, [App | Remain], RunningApps);
        false ->
            case stop_app(App) of
                ok ->
                    do_stop_apps(Apps, Remain, RunningApps);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(debug, #{msg => "stop_plugin_successfully", app => App}),
            ok = unload_module_and_app(App);
        {error, {not_started, App}} ->
            ?SLOG(debug, #{msg => "plugin_not_started", app => App}),
            ok = unload_module_and_app(App);
        {error, Reason} ->
            {error, #{msg => "failed_to_stop_app", app => App, reason => Reason}}
    end.

unload_module_and_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            lists:foreach(fun code:soft_purge/1, Modules);
        _ ->
            ok
    end,
    _ = application:unload(App),
    ok.

is_needed_by_any(AppToStop, RunningApps) ->
    lists:any(
        fun({RunningApp, _RunningAppVsn}) ->
            is_needed_by(AppToStop, RunningApp)
        end,
        RunningApps
    ).

is_needed_by(AppToStop, AppToStop) ->
    false;
is_needed_by(AppToStop, RunningApp) ->
    case application:get_key(RunningApp, applications) of
        {ok, Deps} -> lists:member(AppToStop, Deps);
        undefined -> false
    end.

do_put_config_internal(Key, Value, ConfLocation) when is_atom(Key) ->
    do_put_config_internal([Key], Value, ConfLocation);
do_put_config_internal(Path, Values, _ConfLocation = local) when is_list(Path) ->
    Opts = #{rawconf_with_defaults => true, override_to => cluster},
    %% Already in cluster_rpc, don't use emqx_conf:update, dead calls
    case emqx:update_config([?CONF_ROOT | Path], bin_key(Values), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end;
do_put_config_internal(Path, Values, _ConfLocation = global) when is_list(Path) ->
    Opts = #{rawconf_with_defaults => true, override_to => cluster},
    case emqx_conf:update([?CONF_ROOT | Path], bin_key(Values), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% `emqx_config_handler' API
%%--------------------------------------------------------------------

post_config_update([?CONF_ROOT], _Req, #{states := NewStates}, #{states := OldStates}, _Envs) ->
    NewStatesIndex = maps:from_list([{NV, S} || S = #{name_vsn := NV} <- NewStates]),
    OldStatesIndex = maps:from_list([{NV, S} || S = #{name_vsn := NV} <- OldStates]),
    #{changed := Changed} = emqx_utils_maps:diff_maps(NewStatesIndex, OldStatesIndex),
    maps:foreach(fun enable_disable_plugin/2, Changed),
    ok;
post_config_update(_Path, _Req, _NewConf, _OldConf, _Envs) ->
    ok.

enable_disable_plugin(NameVsn, {#{enable := true}, #{enable := false}}) ->
    %% errors are already logged in this fn
    _ = ensure_stopped(NameVsn),
    ok;
enable_disable_plugin(NameVsn, {#{enable := false}, #{enable := true}}) ->
    %% errors are already logged in this fn
    _ = ensure_started(NameVsn),
    ok;
enable_disable_plugin(_NameVsn, _Diff) ->
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

put_configured(Configured) ->
    put_configured(Configured, _ConfLocation = local).

put_configured(Configured, ConfLocation) ->
    ok = do_put_config_internal(states, bin_key(Configured), ConfLocation).

configured() ->
    get_config_interal(states, []).

for_plugins(ActionFun) ->
    case lists:flatmap(ActionFun, configured()) of
        [] ->
            ok;
        Errors ->
            ErrMeta = #{function => ActionFun, errors => Errors},
            ?tp(
                for_plugins_action_error_occurred,
                ErrMeta
            ),
            ?SLOG(error, ErrMeta#{msg => "for_plugins_action_error_occurred"}),
            ok
    end.

maybe_post_op_after_installed(NameVsn0, Mode) ->
    NameVsn = wrap_to_list(NameVsn0),
    _ = maybe_load_config_schema(NameVsn, Mode),
    ok = maybe_ensure_state(NameVsn).

maybe_ensure_state(NameVsn) ->
    EnsureStateFun = fun(#{name_vsn := NV, enable := Bool}, AccIn) ->
        case NV of
            NameVsn ->
                %% Configured, using existed cluster config
                _ = ensure_state(NV, no_move, Bool, global),
                AccIn#{ensured => true};
            _ ->
                AccIn
        end
    end,
    case lists:foldl(EnsureStateFun, #{ensured => false}, configured()) of
        #{ensured := true} ->
            ok;
        #{ensured := false} ->
            ?SLOG(info, #{msg => "plugin_not_configured", name_vsn => NameVsn}),
            %% Clean installation, no config, ensure with `Enable = false`
            _ = ensure_state(NameVsn, no_move, false, global),
            ok
    end,
    ok.

maybe_load_config_schema(NameVsn, Mode) ->
    _ =
        case emqx_plugins_fs:read_avsc_bin(NameVsn) of
            {ok, AvscBin} ->
                do_load_config_schema(NameVsn, AvscBin);
            {error, _} ->
                ok
        end,
    _ = maybe_create_config_dir(NameVsn, Mode).

do_load_config_schema(NameVsn, AvscBin) ->
    case emqx_plugins_serde:add_schema(bin(NameVsn), AvscBin) of
        ok -> ok;
        {error, already_exists} -> ok;
        {error, _Reason} -> ok
    end.

maybe_create_config_dir(NameVsn, Mode) ->
    with_plugin_avsc(NameVsn) andalso
        do_create_config_dir(NameVsn, Mode).

do_create_config_dir(NameVsn, Mode) ->
    case emqx_plugins_fs:ensure_config_dir(NameVsn) of
        ok ->
            %% get config from other nodes or get from tarball
            _ = maybe_ensure_plugin_config(NameVsn, Mode),
            ok;
        {error, _} = Error ->
            Error
    end.

-spec maybe_ensure_plugin_config(name_vsn(), ?fresh_install | ?normal) -> ok.
maybe_ensure_plugin_config(NameVsn, Mode) ->
    maybe
        true ?= with_plugin_avsc(NameVsn),
        _ = ensure_plugin_config({NameVsn, Mode})
    else
        _ -> ok
    end.

-spec ensure_plugin_config({name_vsn(), ?fresh_install | ?normal}) -> ok.
ensure_plugin_config({NameVsn, ?normal}) ->
    ensure_plugin_config(NameVsn, [N || N <- mria:running_nodes(), N /= node()]);
ensure_plugin_config({NameVsn, ?fresh_install}) ->
    ?SLOG(debug, #{
        msg => "default_plugin_config_used",
        name_vsn => NameVsn,
        hint => "fresh_install"
    }),
    cp_default_config_file(NameVsn).

-spec ensure_plugin_config(name_vsn(), list(node())) -> ok.
ensure_plugin_config(NameVsn, []) ->
    ?SLOG(debug, #{
        msg => "local_plugin_config_used",
        name_vsn => NameVsn,
        reason => "no_other_running_nodes"
    }),
    cp_default_config_file(NameVsn);
ensure_plugin_config(NameVsn, Nodes) ->
    case get_plugin_config_from_any_node(Nodes, NameVsn, []) of
        {ok, ConfigMap} when is_map(ConfigMap) ->
            HoconBin = hocon_pp:do(ConfigMap, #{}),
            Path = emqx_plugins_fs:config_file_path(NameVsn),
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, HoconBin),
            ensure_config_map(NameVsn);
        _ ->
            ?SLOG(error, #{msg => "config_not_found_from_cluster", name_vsn => NameVsn}),
            cp_default_config_file(NameVsn)
    end.

-spec cp_default_config_file(name_vsn()) -> ok.
cp_default_config_file(NameVsn) ->
    %% always copy default hocon file into config dir when can not get config from other nodes
    Source = emqx_plugins_fs:default_config_file_path(NameVsn),
    Destination = emqx_plugins_fs:config_file_path(NameVsn),
    maybe
        true ?= filelib:is_regular(Source),
        %% destination path not existed (not configured)
        false ?=
            case filelib:is_regular(Destination) of
                true ->
                    ?SLOG(debug, #{msg => "plugin_config_file_already_existed", name_vsn => NameVsn});
                false ->
                    false
            end,
        ok = filelib:ensure_dir(Destination),
        case file:copy(Source, Destination) of
            {ok, _} ->
                ensure_config_map(NameVsn);
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "failed_to_copy_plugin_default_hocon_config",
                    source => Source,
                    destination => Destination,
                    reason => Reason
                })
        end
    else
        _ -> ensure_config_map(NameVsn)
    end.

ensure_config_map(NameVsn) ->
    case emqx_plugins_fs:read_hocon(NameVsn) of
        {ok, Config} ->
            case with_plugin_avsc(NameVsn) of
                true ->
                    do_ensure_config_map(NameVsn, Config);
                false ->
                    ?SLOG(debug, #{
                        msg => "put_plugin_config_directly",
                        hint => "plugin_without_config_schema",
                        name_vsn => NameVsn
                    }),
                    put_config(NameVsn, Config, ?plugin_without_config_schema)
            end;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_read_plugin_config_hocon", name_vsn => NameVsn, reason => Reason
            }),
            ok
    end.

do_ensure_config_map(NameVsn, ConfigJsonMap) ->
    case decode_plugin_config_map(NameVsn, ConfigJsonMap) of
        {ok, AvroValue} ->
            put_config(NameVsn, ConfigJsonMap, AvroValue);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "plugin_config_validation_failed",
                name_vsn => NameVsn,
                reason => Reason
            }),
            ok
    end.

%% @private Backup the current config to a file with a timestamp suffix and
%% then save the new config to the config file.
backup_and_write_hocon_bin(NameVsn, HoconBin) ->
    %% this may fail, but we don't care
    %% e.g. read-only file system
    Path = emqx_plugins_fs:config_file_path(NameVsn),
    _ = filelib:ensure_dir(Path),
    TmpFile = Path ++ ".tmp",
    case file:write_file(TmpFile, HoconBin) of
        ok ->
            backup_and_replace(Path, TmpFile);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_save_plugin_conf_file",
                hint =>
                    "The updated cluster config is not saved on this node, please check the file system.",
                filename => TmpFile,
                reason => Reason
            }),
            %% e.g. read-only, it's not the end of the world
            ok
    end.

backup_and_replace(Path, TmpPath) ->
    Backup = Path ++ "." ++ emqx_utils_calendar:now_time(millisecond) ++ ".bak",
    case file:rename(Path, Backup) of
        ok ->
            ok = file:rename(TmpPath, Path),
            ok = prune_backup_files(Path);
        {error, enoent} ->
            %% not created yet
            ok = file:rename(TmpPath, Path);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_backup_plugin_conf_file",
                filename => Backup,
                reason => Reason
            }),
            ok
    end.

prune_backup_files(Path) ->
    Files0 = filelib:wildcard(Path ++ ".*"),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = lists:filter(fun(F) -> re:run(F, Re) =/= nomatch end, Files0),
    Sorted = lists:reverse(lists:sort(Files)),
    {_Keeps, Deletes} = lists:split(min(?MAX_KEEP_BACKUP_CONFIGS, length(Sorted)), Sorted),
    lists:foreach(
        fun(F) ->
            case file:delete(F) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_delete_backup_plugin_conf_file",
                        filename => F,
                        reason => Reason
                    }),
                    ok
            end
        end,
        Deletes
    ).

running_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:which_applications(infinity)
    ).

bin_key(Map) when is_map(Map) ->
    maps:fold(fun(K, V, Acc) -> Acc#{bin(K) => V} end, #{}, Map);
bin_key(List = [#{} | _]) ->
    lists:map(fun(M) -> bin_key(M) end, List);
bin_key(Term) ->
    Term.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

wrap_to_list(Path) ->
    binary_to_list(iolist_to_binary(Path)).

run_with_timeout(Module, Function, Args, Timeout) ->
    Self = self(),
    Fun = fun() ->
        Result = apply(Module, Function, Args),
        Self ! {self(), Result}
    end,
    Pid = spawn(Fun),
    TimerRef = erlang:send_after(Timeout, self(), {timeout, Pid}),
    receive
        {Pid, Result} ->
            _ = erlang:cancel_timer(TimerRef),
            {ok, Result};
        {timeout, Pid} ->
            exit(Pid, kill),
            {error, timeout}
    end.
