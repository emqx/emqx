%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_cli).

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
    print(NameVsn, Result, LogFun, ensure_installed_cluster).

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
    ?PRINT(emqx_plugins:ensure_installed(NameVsn, ?fresh_install), LogFun).

ensure_installed_cluster(NameVsn, LogFun) ->
    case emqx_plugins_fs:get_tar(NameVsn) of
        {ok, TarBin} ->
            Running = emqx:running_nodes(),
            V5Nodes = nodes_supporting_bpapi_version(5),
            case Running -- V5Nodes of
                [] ->
                    Results = emqx_plugins_proto_v5:install_package(V5Nodes, NameVsn, TarBin),
                    print_cluster_result(V5Nodes, Results, NameVsn, LogFun);
                Missing ->
                    Reason = #{
                        hint => <<"cluster install requires all nodes to support proto v5">>,
                        nodes_missing_v5 => Missing
                    },
                    ?PRINT({error, Reason}, LogFun)
            end;
        {error, Reason} ->
            ?PRINT({error, Reason}, LogFun)
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
