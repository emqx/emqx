%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_cli_utils).

-export([load/0, unload/0]).
-export([plugins/1]).

-export([
    list/1,
    describe/2,
    ensure_installed/2,
    ensure_installed_cluster/2,
    ensure_uninstalled/2,
    ensure_started/2,
    ensure_stopped/2,
    restart/2,
    ensure_disabled/2,
    ensure_enabled/3,
    allow_installation/2,
    allow_installation/3,
    disallow_installation/2
]).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").

-define(BPAPI_NAME, emqx_plugins).

-define(PRINT(EXPR, LOG_FUN),
    print(NameVsn, fun() -> EXPR end(), LOG_FUN, ?FUNCTION_NAME)
).

load() ->
    emqx_ctl:register_command(plugins, {?MODULE, plugins}, []),
    ok.

unload() ->
    emqx_ctl:unregister_command(plugins),
    ok.

plugins(["list"]) ->
    list(fun emqx_ctl:print/2);
plugins(["describe", NameVsn]) ->
    describe(NameVsn, fun emqx_ctl:print/2);
plugins(["allow", NameVsn]) ->
    allow_installation(NameVsn, fun emqx_ctl:print/2);
plugins(["allow", NameVsn, "sha256:" ++ Hex]) ->
    case parse_sha256_hex(Hex) of
        {ok, Sha256} ->
            allow_installation(NameVsn, Sha256, fun emqx_ctl:print/2);
        error ->
            emqx_ctl:print(
                "sha256 must be 64 lowercase hex characters, e.g. sha256:abc...~n"
            )
    end;
plugins(["disallow", NameVsn]) ->
    disallow_installation(NameVsn, fun emqx_ctl:print/2);
plugins(["install", NameVsn]) ->
    ensure_installed(NameVsn, fun emqx_ctl:print/2);
plugins(["uninstall", NameVsn]) ->
    ensure_uninstalled(NameVsn, fun emqx_ctl:print/2);
plugins(["start", NameVsn]) ->
    ensure_started(NameVsn, fun emqx_ctl:print/2);
plugins(["stop", NameVsn]) ->
    ensure_stopped(NameVsn, fun emqx_ctl:print/2);
plugins(["restart", NameVsn]) ->
    restart(NameVsn, fun emqx_ctl:print/2);
plugins(["disable", NameVsn]) ->
    ensure_disabled(NameVsn, fun emqx_ctl:print/2);
plugins(["enable", NameVsn]) ->
    ensure_enabled(NameVsn, no_move, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "front"]) ->
    ensure_enabled(NameVsn, front, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "rear"]) ->
    ensure_enabled(NameVsn, rear, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "before", Other]) ->
    ensure_enabled(NameVsn, {before, Other}, fun emqx_ctl:print/2);
plugins(_) ->
    emqx_ctl:usage(
        [
            {"plugins <command> [Name-Vsn]", "e.g. 'start emqx_plugin_template-5.0-rc.1'"},
            {"plugins list", "List all installed plugins"},
            {"plugins describe  Name-Vsn", "Describe an installed plugins"},
            {"plugins allow     Name-Vsn [sha256:HEX]",
                "Allows installation of a plugin in the cluster from Dashboard or API.\n"
                "The grant expires 5 minutes after issue.\n"
                "If sha256:HEX (64 lowercase hex chars) is given, the upload bytes\n"
                "must hash to that value or the install is rejected."},
            {"plugins disallow  Name-Vsn",
                "Disallows installation of a plugin in the cluster from Dashboard or API"},
            {"plugins install   Name-Vsn",
                "Install a plugin package placed\n"
                "in plugin's install_dir"},
            {"plugins uninstall Name-Vsn",
                "Uninstall a plugin. NOTE: it deletes\n"
                "all files in install_dir/Name-Vsn"},
            {"plugins start     Name-Vsn", "Start a plugin"},
            {"plugins stop      Name-Vsn", "Stop a plugin"},
            {"plugins restart   Name-Vsn", "Stop then start a plugin"},
            {"plugins disable   Name-Vsn", "Disable auto-boot"},
            {"plugins enable    Name-Vsn [Position]",
                "Enable auto-boot at Position in the boot list, where Position could be\n"
                "'front', 'rear', or 'before Other-Vsn' to specify a relative position.\n"
                "The Position parameter can be used to adjust the boot order.\n"
                "If no Position is given, an already configured plugin\n"
                "will stay at is old position; a newly plugin is appended to the rear\n"
                "e.g. plugins disable foo-0.1.0 front\n"
                "     plugins enable bar-0.2.0 before foo-0.1.0"}
        ]
    ).

parse_sha256_hex(Hex) when length(Hex) =:= 64 ->
    case re:run(Hex, "^[0-9a-f]{64}$", [{capture, none}]) of
        match -> {ok, list_to_binary(Hex)};
        nomatch -> error
    end;
parse_sha256_hex(_) ->
    error.

list(LogFun) ->
    LogFun("~ts~n", [to_json(emqx_plugins:list())]).

describe(NameVsn, LogFun) ->
    case emqx_plugins:describe(NameVsn) of
        {ok, Plugin} ->
            LogFun("~ts~n", [to_json(Plugin)]);
        {error, Reason} ->
            %% this should not happen unless the package is manually installed
            %% corrupted packages installed from emqx_plugins:ensure_installed
            %% should not leave behind corrupted files
            ?SLOG(error, #{
                msg => "failed_to_describe_plugin",
                name_vsn => NameVsn,
                cause => Reason
            }),
            %% do nothing to the CLI console
            ok
    end.

allow_installation(NameVsn, LogFun) ->
    allow_installation(NameVsn, undefined, LogFun).

allow_installation(NameVsn, Sha256, LogFun) ->
    try emqx_plugins_utils:parse_name_vsn(NameVsn) of
        {_AppName, _Vsn} ->
            do_allow_installation(NameVsn, Sha256, LogFun)
    catch
        error:bad_name_vsn ->
            ?PRINT({error, bad_name_vsn}, LogFun)
    end.

do_allow_installation(NameVsn, undefined, LogFun) ->
    %% No sha256 binding — use proto v3 to remain compatible with older nodes
    %% in a rolling upgrade.
    Nodes = nodes_supporting_bpapi_version(3),
    Results = emqx_plugins_proto_v3:allow_installation(Nodes, NameVsn),
    print_allow_result(Nodes, Results, NameVsn, LogFun);
do_allow_installation(NameVsn, Sha256, LogFun) when is_binary(Sha256) ->
    %% sha256 binding — needs every running node on proto v4 so the binding is
    %% enforced everywhere. Refuse rather than silently allow on old nodes.
    Running = emqx:running_nodes(),
    V4Nodes = nodes_supporting_bpapi_version(4),
    case Running -- V4Nodes of
        [] ->
            Results = emqx_plugins_proto_v4:allow_installation(V4Nodes, NameVsn, Sha256),
            print_allow_result(V4Nodes, Results, NameVsn, LogFun);
        Missing ->
            Reason = #{
                hint => <<"sha256 binding requires all nodes to be upgraded">>,
                nodes_missing_v4 => Missing
            },
            ?PRINT({error, Reason}, LogFun)
    end.

print_allow_result(Nodes, Results, NameVsn, LogFun) ->
    Errors =
        lists:filter(
            fun
                ({_Node, {ok, ok}}) -> false;
                ({_Node, _}) -> true
            end,
            lists:zip(Nodes, Results)
        ),
    Result =
        case Errors of
            [] -> {ok, #{expires_in_ms => emqx_plugins:allow_ttl_ms()}};
            _ -> {error, maps:from_list(Errors)}
        end,
    print(NameVsn, Result, LogFun, allow_installation).

print_cluster_result(Nodes, Results, NameVsn, LogFun) ->
    Errors =
        lists:filter(
            fun
                ({_Node, {ok, ok}}) -> false;
                ({_Node, _}) -> true
            end,
            lists:zip(Nodes, Results)
        ),
    Result =
        case Errors of
            [] -> ok;
            _ -> {error, maps:from_list(Errors)}
        end,
    print(NameVsn, Result, LogFun, ensure_installed_cluster),
    Result.

disallow_installation(NameVsn, LogFun) ->
    try emqx_plugins_utils:parse_name_vsn(NameVsn) of
        {_AppName, _Vsn} ->
            do_disallow_installation(NameVsn, LogFun)
    catch
        error:bad_name_vsn ->
            ?PRINT({error, bad_name_vsn}, LogFun)
    end.

do_disallow_installation(NameVsn, LogFun) ->
    Nodes = nodes_supporting_bpapi_version(3),
    Results = emqx_plugins_proto_v3:disallow_installation(Nodes, NameVsn),
    Errors =
        lists:filter(
            fun
                ({_Node, {ok, ok}}) ->
                    false;
                ({_Node, _Error}) ->
                    true
            end,
            lists:zip(Nodes, Results)
        ),
    Result =
        case Errors of
            [] -> ok;
            _ -> {error, maps:from_list(Errors)}
        end,
    ?PRINT(Result, LogFun).

ensure_installed(NameVsn, LogFun) ->
    %% The CLI path must enforce the same allow gate as the HTTP upload path:
    %% a tarball that landed in the install dir (e.g. via cluster replication
    %% or manual placement) must not be installable without an explicit
    %% `plugins allow' grant from the admin.
    case emqx_plugins:is_allowed_installation(NameVsn) of
        true ->
            Result = do_ensure_installed(NameVsn),
            maybe_forget_grant(NameVsn, Result),
            ?PRINT(Result, LogFun);
        false ->
            ?PRINT({error, not_allowed}, LogFun)
    end.

do_ensure_installed(NameVsn) ->
    case emqx_plugins:describe(NameVsn, #{}) of
        {ok, _} ->
            {error, #{
                msg => "plugin_already_installed", name_vsn => NameVsn
            }};
        {error, _} ->
            case check_local_tar_sha256(NameVsn) of
                ok ->
                    emqx_plugins:ensure_installed(NameVsn, ?fresh_install);
                {error, _} = Error ->
                    Error
            end
    end.

%% If the allow entry recorded a sha256 for the package, verify that the
%% tarball already present in the install dir matches before installing.
%% A tarball fetched from another node (no local copy yet) cannot be
%% checked here; the download path performs its own validation.
check_local_tar_sha256(NameVsn) ->
    case emqx_plugins_fs:get_tar(NameVsn) of
        {ok, TarBin} ->
            emqx_plugins:is_allowed_installation(NameVsn, TarBin);
        {error, _} ->
            ok
    end.

ensure_installed_cluster(NameVsn, LogFun) ->
    %% Same allow gate as the single-node CLI install: the tarball is read
    %% from the local install dir and pushed to all nodes, so it must not be
    %% installable without an explicit `plugins allow' grant either.
    case emqx_plugins:is_allowed_installation(NameVsn) of
        true ->
            Result = do_ensure_installed_cluster(NameVsn, LogFun),
            maybe_forget_grant(NameVsn, Result),
            Result;
        false ->
            ?PRINT({error, not_allowed}, LogFun)
    end.

%% Consume the grant only on success; retain it on failure so the admin can
%% retry after fixing the underlying problem. Mirroring the HTTP upload
%% path, the grant is revoked cluster-wide because `plugins allow' is issued
%% cluster-wide — a local-only forget would leave the grant reusable on
%% other nodes until its TTL expires.
maybe_forget_grant(NameVsn, ok) ->
    Nodes = emqx:running_nodes(),
    _ = emqx_plugins_proto_v3:disallow_installation(Nodes, NameVsn),
    ok;
maybe_forget_grant(_NameVsn, _Result) ->
    ok.

do_ensure_installed_cluster(NameVsn, LogFun) ->
    case emqx_plugins_fs:get_tar(NameVsn) of
        {ok, TarBin} ->
            case emqx_plugins:is_allowed_installation(NameVsn, TarBin) of
                ok ->
                    Running = emqx:running_nodes(),
                    V5Nodes = nodes_supporting_bpapi_version(5),
                    case Running -- V5Nodes of
                        [] ->
                            Results = emqx_plugins_proto_v5:install_package(
                                V5Nodes, NameVsn, TarBin
                            ),
                            print_cluster_result(V5Nodes, Results, NameVsn, LogFun);
                        Missing ->
                            Reason = #{
                                hint =>
                                    <<"cluster install requires all nodes to support proto v5">>,
                                nodes_missing_v5 => Missing
                            },
                            ?PRINT({error, Reason}, LogFun),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ?PRINT({error, Reason}, LogFun),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?PRINT({error, Reason}, LogFun),
            {error, Reason}
    end.

ensure_uninstalled(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_uninstalled(NameVsn), LogFun).

ensure_started(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_started(NameVsn), LogFun).

ensure_stopped(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_stopped(NameVsn), LogFun).

restart(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:restart(NameVsn), LogFun).

ensure_enabled(NameVsn, Position, LogFun) ->
    ?PRINT(emqx_plugins:ensure_enabled(NameVsn, Position, _ConfLocation = global), LogFun).

ensure_disabled(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_disabled(NameVsn), LogFun).

%% erlang cannot distinguish between "" and [], so best_effort_json is also helpless.
to_json([]) ->
    <<"[]">>;
to_json(Input) ->
    emqx_utils_json:best_effort_json(Input).

print(NameVsn, Res, LogFun, Action) ->
    Obj = #{
        action => Action,
        name_vsn => NameVsn
    },
    JsonReady =
        case Res of
            ok ->
                Obj#{result => ok};
            {ok, Extra} when is_map(Extra) ->
                maps:merge(Obj#{result => ok}, Extra);
            {error, Reason} ->
                Obj#{
                    result => not_ok,
                    cause => Reason
                }
        end,
    LogFun("~ts~n", [to_json(JsonReady)]).

nodes_supporting_bpapi_version(Vsn) ->
    [
        N
     || N <- emqx:running_nodes(),
        case emqx_bpapi:supported_version(N, ?BPAPI_NAME) of
            undefined -> false;
            NVsn when is_number(NVsn) -> NVsn >= Vsn
        end
    ].
