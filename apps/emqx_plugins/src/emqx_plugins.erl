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
    put_config/2
]).

%% Package utils
-export([
    decode_plugin_config_map/2
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

%% for test cases
-export([put_config_internal/2]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% Defines
-define(PLUGIN_PERSIS_CONFIG_KEY(NameVsn), {?MODULE, NameVsn}).

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
    _ = purge_plugin_config(NameVsn),
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
            emqx_plugins_apps:stop(Plugin);
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
%% The plugin config Json Map was validated by avro schema
%% Or: if no and plugin config ALWAYS be valid before calling this function.
put_config(NameVsn, ConfigJsonMap) when (not is_binary(NameVsn)) ->
    put_config(bin(NameVsn), ConfigJsonMap);
put_config(NameVsn, ConfigJsonMap) ->
    ok = emqx_plugins_local_config:backup_and_update(NameVsn, ConfigJsonMap),
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
    case has_avsc(NameVsn) of
        true ->
            case emqx_plugins_serde:decode(NameVsn, ensure_config_bin(AvroJsonMap)) of
                {ok, Config} ->
                    {ok, Config};
                {error, #{reason := plugin_serde_not_found}} ->
                    Reason = "plugin_config_schema_serde_not_found",
                    ?SLOG(error, #{
                        msg => Reason, name_vsn => NameVsn, plugin_with_avro_schema => true
                    }),
                    {error, Reason};
                {error, _} = Error ->
                    Error
            end;
        false ->
            ?SLOG(debug, #{
                msg => "plugin_without_config_schema",
                name_vsn => NameVsn
            }),
            {ok, ?plugin_without_config_schema}
    end.

-spec has_avsc(name_vsn()) -> boolean().
has_avsc(NameVsn) ->
    case read_plugin_info(NameVsn, #{fill_readme => false}) of
        {ok, #{<<"with_config_schema">> := WithAvsc}} when is_boolean(WithAvsc) ->
            WithAvsc;
        _ ->
            false
    end.

get_config_internal(Key, Default) when is_atom(Key) ->
    get_config_internal([Key], Default);
get_config_internal(Path, Default) ->
    emqx_conf:get([?CONF_ROOT | Path], Default).

put_config_internal(Key, Value) ->
    do_put_config_internal(Key, Value, _ConfLocation = local).

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

purge_plugin_config(NameVsn) ->
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
        ok ?= emqx_plugins_apps:start(NameVsn, Plugin)
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
    emqx_plugins_fs:ensure_installed_from_tar(
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
    get_config_internal(states, []).

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
                _ = emqx_plugins_serde:add_schema(bin(NameVsn), AvscBin),
                ok;
            {error, _} ->
                ok
        end,
    _ = maybe_create_config_dir(NameVsn, Mode).

maybe_create_config_dir(NameVsn, Mode) ->
    has_avsc(NameVsn) andalso
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
    case has_avsc(NameVsn) of
        true ->
            _ = ensure_plugin_config({NameVsn, Mode});
        false ->
            ok
    end.

-spec ensure_plugin_config({name_vsn(), ?fresh_install | ?normal}) -> ok.
ensure_plugin_config({NameVsn, Mode}) ->
    case ensure_local_config(NameVsn, Mode) of
        ok ->
            configure_from_local_config(NameVsn);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_ensure_plugin_config", name_vsn => NameVsn, reason => Reason
            }),
            ok
    end.

ensure_local_config(NameVsn, ?fresh_install) ->
    emqx_plugins_local_config:copy_default_config(NameVsn);
ensure_local_config(NameVsn, ?normal) ->
    Nodes = mria:running_nodes(),
    case get_plugin_config_from_any_node(Nodes, NameVsn, []) of
        {ok, Config} when is_map(Config) ->
            emqx_plugins_local_config:update(NameVsn, Config);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_get_plugin_config_from_cluster",
                name_vsn => NameVsn,
                reason => Reason
            }),
            emqx_plugins_local_config:copy_default_config(NameVsn)
    end.

configure_from_local_config(NameVsn) ->
    case validated_local_config(NameVsn) of
        {ok, Config} ->
            put_config(NameVsn, Config);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_validate_plugin_config", name_vsn => NameVsn, reason => Reason
            }),
            ok
    end.

validated_local_config(NameVsn) ->
    case emqx_plugins_fs:read_hocon(NameVsn) of
        {ok, Config} ->
            case has_avsc(NameVsn) of
                true ->
                    case decode_plugin_config_map(NameVsn, Config) of
                        {ok, _} ->
                            {ok, Config};
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "plugin_config_validation_failed",
                                name_vsn => NameVsn,
                                reason => Reason
                            }),
                            {error, Reason}
                    end;
                false ->
                    {ok, Config}
            end;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_read_plugin_config_hocon", name_vsn => NameVsn, reason => Reason
            }),
            {error, Reason}
    end.

running_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:which_applications(infinity)
    ).

ensure_config_bin(AvroJsonMap) when is_map(AvroJsonMap) ->
    emqx_utils_json:encode(AvroJsonMap);
ensure_config_bin(AvroJsonBin) when is_binary(AvroJsonBin) ->
    AvroJsonBin.

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
