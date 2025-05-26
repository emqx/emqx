%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    describe/2,
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

    get_package_from_node/2,
    get_config_from_node/2,

    ensure_uninstalled/1,
    ensure_enabled/1,
    ensure_enabled/2,
    ensure_enabled/3,
    ensure_disabled/1,
    purge/1,
    write_package/2,
    is_package_present/1,
    purge_other_versions/1,
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
    list/1,
    list/2,
    list_active/0
]).

%% Plugin config APIs
-export([
    get_config/1,
    get_config/2,
    update_config/2
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
-define(PLUGIN_CONFIG_PT_KEY(NameVsn), {?MODULE, NameVsn}).

-define(CATCH(BODY), catch_errors(atom_to_list(?FUNCTION_NAME), fun() -> BODY end)).

-define(APP, emqx_plugins).

-define(allowed_installations, allowed_installations).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Describe a plugin.
-spec describe(name_vsn()) -> {ok, emqx_plugins_info:t()} | {error, any()}.
describe(NameVsn) ->
    describe(NameVsn, #{fill_readme => true, health_check => true}).

-spec describe(name_vsn(), emqx_plugins_info:read_options()) ->
    {ok, emqx_plugins_info:t()} | {error, any()}.
describe(NameVsn, Options) ->
    read_plugin_info(NameVsn, Options).

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

%% @doc
%% * Install a .tar.gz package placed in install_dir
%% * Configure the plugin
-spec ensure_installed(name_vsn()) -> ok | {error, map()}.
ensure_installed(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{running_status := RunningSt}} ->
            configure(NameVsn, ?normal, RunningSt);
        {error, _} ->
            ok = purge(NameVsn),
            install_and_configure(NameVsn, ?normal, stopped)
    end.

ensure_installed(NameVsn, ?fresh_install = Mode) ->
    %% TODO
    %% Additionally check if the plugin is actually stopped/uninstalled.
    %% Currently, external layers (API, CLI) are responsible for
    %% not allowing to install a plugin that is already installed.
    install_and_configure(NameVsn, Mode, stopped).

%% @doc Ensure files and directories for the given plugin are being deleted.
%% If a plugin is running, or enabled, an error is returned.
-spec ensure_uninstalled(name_vsn()) -> ok | {error, any()}.
ensure_uninstalled(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{running_status := running}} ->
            {error, #{
                msg => "bad_plugin_running_status",
                hint => "stop_the_plugin_first"
            }};
        {ok, #{config_status := enabled}} ->
            {error, #{
                msg => "bad_plugin_config_status",
                hint => "disable_the_plugin_first"
            }};
        {ok, Plugin} ->
            ok = emqx_plugins_apps:unload(Plugin),
            ok = purge(NameVsn),
            ensure_delete_state(NameVsn);
        {error, _Reason} ->
            ensure_delete_state(NameVsn)
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
    ?SLOG(debug, #{msg => "purge_plugin", name_vsn => NameVsn}),
    ok = delete_cached_config(NameVsn),
    emqx_plugins_fs:purge_installed(NameVsn).

%% @doc Write the package file.
-spec write_package(name_vsn(), binary()) -> ok.
write_package(NameVsn, Bin) ->
    emqx_plugins_fs:write_tar(NameVsn, Bin).

%% @doc Check if the package file is present.
-spec is_package_present(name_vsn()) -> false | {true, [file:filename()]}.
is_package_present(NameVsn) ->
    emqx_plugins_fs:is_tar_present(NameVsn).

-spec purge_other_versions(name_vsn()) -> ok.
purge_other_versions(NameVsn) ->
    {AppName, AppVsn} = emqx_plugins_utils:parse_name_vsn(NameVsn),
    AppNameBin = bin(AppName),
    ?SLOG(debug, #{
        msg => "purge_plugin_other_versions",
        keep_plugin => NameVsn,
        reason => "cluster_sync"
    }),
    lists:foreach(
        fun
            (#{name := Name, rel_vsn := RelVsn}) when
                AppNameBin =:= Name, AppVsn =:= RelVsn
            ->
                ok;
            (#{name := Name, rel_vsn := RelVsn}) ->
                case AppNameBin =:= Name of
                    true ->
                        NameVsn1 = emqx_plugins_utils:make_name_vsn_string(Name, RelVsn),
                        maybe
                            ok ?= ensure_stopped(NameVsn1),
                            ok ?= ensure_uninstalled(NameVsn1)
                        else
                            {error, Reason} ->
                                ?SLOG(error, #{
                                    msg => "failed_to_purge_plugin",
                                    name_vsn => NameVsn1,
                                    reason => Reason
                                })
                        end;
                    false ->
                        ok
                end
        end,
        emqx_plugins:list()
    ).

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
    case emqx_plugins_info:read(NameVsn) of
        {ok, Plugin} ->
            emqx_plugins_apps:stop(Plugin);
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_config(name_vsn()) -> plugin_config_map().
get_config(NameVsn) ->
    get_config(NameVsn, #{}).

-spec get_config(name_vsn(), term()) -> plugin_config_map() | term().
get_config(NameVsn, Default) ->
    get_cached_config(NameVsn, Default).

%% @doc Update plugin's config.
%% RPC call from Management API or CLI.
%% NOTE
%% This function assumes that the config is already validated
%% by avro schema in case of its presence.
update_config(NameVsn, Config) ->
    maybe
        {ok, Plugin} ?= emqx_plugins_info:read(NameVsn, #{}),
        ok ?= request_config_change(NameVsn, Plugin, Config),
        ok = emqx_plugins_local_config:backup_and_update(NameVsn, Config),
        ok = put_cached_config(NameVsn, Config)
    end.

%% @doc Stop and then start the plugin.
restart(NameVsn) ->
    case ensure_stopped(NameVsn) of
        ok -> ensure_started(NameVsn);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Return Name-Vsn list of currently running plugins.
-spec list_active() -> [binary()].
list_active() ->
    lists:sort(
        lists:foldl(
            fun
                (#{running_status := running} = Plugin, Acc) ->
                    #{name := Name, rel_vsn := Vsn} = Plugin,
                    [name_vsn(Name, Vsn) | Acc];
                (_, Acc) ->
                    Acc
            end,
            [],
            list()
        )
    ).

%% @doc List all installed plugins.
%% Including the ones that are installed, but not enabled in config.
-spec list() -> [emqx_plugins_info:t()].
list() ->
    list(normal).

-spec list(all | normal | hidden) -> [emqx_plugins_info:t()].
list(Type) ->
    list(Type, #{}).

-spec list(all | normal | hidden, emqx_plugins_info:read_options()) -> [emqx_plugins_info:t()].
list(Type, Options) ->
    All = lists:filtermap(
        fun(NameVsn) ->
            case read_plugin_info(NameVsn, Options) of
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
filter_plugin_of_type(normal, #{hidden := true}) ->
    false;
filter_plugin_of_type(normal, Info) ->
    {true, Info};
filter_plugin_of_type(hidden, #{hidden := true} = Info) ->
    {true, Info};
filter_plugin_of_type(hidden, _Info) ->
    false.

%%--------------------------------------------------------------------
%% Package utils

-spec decode_plugin_config_map(name_vsn(), map() | binary()) ->
    {ok, map() | ?plugin_without_config_schema}
    | {error, term()}.
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
        {ok, #{with_config_schema := WithAvsc}} when is_boolean(WithAvsc) ->
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

ensure_delete_state(NameVsn0) ->
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

%% Make sure configured ones are ordered in front.
do_list([], All) ->
    All;
do_list([#{name_vsn := NameVsn} | Rest], All) ->
    SplitF = fun(#{name := Name, rel_vsn := Vsn}) ->
        name_vsn(Name, Vsn) =/= bin(NameVsn)
    end,
    case lists:splitwith(SplitF, All) of
        {_, []} ->
            do_list(Rest, All);
        {Front, [I | Rear]} ->
            [I | do_list(Rest, Front ++ Rear)]
    end.

do_ensure_started(NameVsn) ->
    maybe
        ok ?= install(NameVsn, ?normal),
        ok ?= load_config_schema(NameVsn),
        {ok, Plugin} ?= emqx_plugins_info:read(NameVsn),
        ok ?= emqx_plugins_apps:start(Plugin)
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
    emqx_plugins_info:read(NameVsn, Options).

ensure_installed_locally(NameVsn) ->
    maybe
        ok ?=
            emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() ->
                validate_installation(NameVsn)
            end),
        {ok, Plugin} ?= emqx_plugins_info:read(NameVsn),
        emqx_plugins_apps:load(Plugin, emqx_plugins_fs:lib_dir(NameVsn))
    end.

validate_installation(NameVsn) ->
    case emqx_plugins_info:read(NameVsn) of
        {ok, _Plugin} ->
            ok;
        {error, _} = Error ->
            Error
    end.

install_and_configure(NameVsn, Mode, RunningSt) ->
    maybe
        ok ?= install(NameVsn, Mode),
        configure(NameVsn, Mode, RunningSt)
    end.

configure(NameVsn, Mode, RunningSt) ->
    ok = load_config_schema(NameVsn),
    maybe
        ok ?= ensure_local_config(NameVsn, Mode),
        configure_from_local_config(NameVsn, RunningSt)
    end,
    ensure_state(NameVsn).

%% Install from local tarball or get tarball from cluster
install(NameVsn, Mode) ->
    case {ensure_installed_locally(NameVsn), Mode} of
        {ok, _} ->
            ok;
        {{error, #{reason := plugin_tarball_not_found}}, ?normal} ->
            case get_from_cluster(NameVsn) of
                ok ->
                    ensure_installed_locally(NameVsn);
                {error, Reason} ->
                    {error, Reason}
            end;
        {{error, Reason}, _} ->
            {error, Reason}
    end.

get_from_cluster(NameVsn) ->
    Nodes = [N || N <- mria:running_nodes(), N /= node()],
    case get_package_from_any_node(Nodes, NameVsn, []) of
        {ok, TarContent} ->
            emqx_plugins_fs:write_tar(NameVsn, TarContent);
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

get_package_from_node(Node, NameVsn) ->
    get_package_from_any_node([Node], NameVsn, []).

get_package_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_package_from_any_node([Node | T], NameVsn, Errors) ->
    case emqx_plugins_proto_v2:get_tar(Node, NameVsn, infinity) of
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_tar_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_package_from_any_node(T, NameVsn, [{Node, Err} | Errors])
    end.

get_config_from_node(Node, NameVsn) ->
    get_config_from_any_node([Node], NameVsn, []).

get_config_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_config_from_any_node([Node | RestNodes], NameVsn, Errors) ->
    case
        emqx_plugins_proto_v2:get_config(
            Node, NameVsn, ?CONFIG_FORMAT_MAP, ?plugin_conf_not_found, 5_000
        )
    of
        {ok, ?plugin_conf_not_found} ->
            Err = {error, {config_not_found_on_node, Node, NameVsn}},
            get_config_from_any_node(RestNodes, NameVsn, [{Node, Err} | Errors]);
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_config_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_config_from_any_node(RestNodes, NameVsn, [{Node, Err} | Errors])
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

ensure_state(NameVsn) ->
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

ensure_local_config(NameVsn, Mode) ->
    case emqx_plugins_fs:ensure_config_dir(NameVsn) of
        ok ->
            %% get config from other nodes or get from tarball
            do_ensure_local_config(NameVsn, Mode);
        {error, _} = Error ->
            ?SLOG(warning, #{
                msg => "failed_to_ensure_config_dir", name_vsn => NameVsn, reason => Error
            }),
            Error
    end.

do_ensure_local_config(NameVsn, ?fresh_install) ->
    emqx_plugins_local_config:copy_default(NameVsn);
do_ensure_local_config(NameVsn, ?normal) ->
    Nodes = mria:running_nodes(),
    case get_config_from_any_node(Nodes, NameVsn, []) of
        {ok, Config} when is_map(Config) ->
            emqx_plugins_local_config:update(NameVsn, Config);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_get_plugin_config_from_cluster",
                name_vsn => NameVsn,
                reason => Reason
            }),
            emqx_plugins_local_config:copy_default(NameVsn)
    end.

configure_from_local_config(NameVsn, RunningSt) ->
    case validated_local_config(NameVsn) of
        {ok, NewConfig} ->
            OldConfig = get_cached_config(NameVsn),
            ok = notify_config_change(NameVsn, OldConfig, NewConfig, RunningSt),
            ok = put_cached_config(NameVsn, NewConfig);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_validate_plugin_config", name_vsn => NameVsn, reason => Reason
            }),
            ok
    end.

notify_config_change(_NameVsn, _OldConfig, _NewConfig, stopped) ->
    ok;
notify_config_change(NameVsn, OldConfig, NewConfig, RunningSt) when
    RunningSt =:= running orelse RunningSt =:= loaded
->
    %% NOTE
    %% The new config here is the local config, so it is
    %% * either vendored with the plugin;
    %% * or fetched from another node where it was validated by the plugin before being updated.
    %% In both cases, we do not expect the plugin to reject the config.
    %% Even if it does, there is little that can be done.
    %% So we ignore the result of the callback.
    _ = emqx_plugins_apps:on_config_changed(NameVsn, OldConfig, NewConfig),
    ok.

request_config_change(NameVsn, #{running_status := stopped}, _Config) ->
    {error, {plugin_apps_not_loaded, NameVsn}};
request_config_change(NameVsn, #{running_status := RunningSt}, Config) when
    RunningSt =:= running orelse RunningSt =:= loaded
->
    emqx_plugins_apps:on_config_changed(NameVsn, get_cached_config(NameVsn), Config).

load_config_schema(NameVsn) ->
    case emqx_plugins_fs:read_avsc_bin(NameVsn) of
        {ok, AvscBin} ->
            _ = emqx_plugins_serde:add_schema(bin(NameVsn), AvscBin),
            ok;
        {error, _} ->
            ok
    end.

validated_local_config(NameVsn) ->
    case emqx_plugins_local_config:read(NameVsn) of
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

get_cached_config(NameVsn) ->
    get_cached_config(NameVsn, #{}).

get_cached_config(NameVsn, Default) ->
    persistent_term:get(?PLUGIN_CONFIG_PT_KEY(bin(NameVsn)), Default).

put_cached_config(NameVsn, Config) ->
    persistent_term:put(?PLUGIN_CONFIG_PT_KEY(bin(NameVsn)), Config).

delete_cached_config(NameVsn) ->
    _ = persistent_term:erase(?PLUGIN_CONFIG_PT_KEY(bin(NameVsn))),
    ok.

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

name_vsn(Name, Vsn) ->
    emqx_plugins_utils:make_name_vsn_binary(Name, Vsn).
