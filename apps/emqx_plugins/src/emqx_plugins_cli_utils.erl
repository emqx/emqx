%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_cli_utils).

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
    case emqx_plugins:install_state(NameVsn) of
        installed ->
            ?PRINT(
                {error, #{
                    msg => "plugin_already_installed", name_vsn => NameVsn
                }},
                LogFun
            );
        _IncompleteOrAbsent ->
            %% Nothing usable is installed: leftovers of an interrupted
            %% installation are replaced by the package.
            ?PRINT(emqx_plugins:ensure_installed(NameVsn, ?fresh_install), LogFun)
    end.

ensure_installed_cluster(NameVsn, LogFun) ->
    %% The package file is shared with the uploads and with the installations
    %% which run on this node: reading it takes the same lock which writes it, so
    %% a concurrent upload of the same name-vsn can not make this node read (and
    %% then install on every node) half of its content.  The lock is released
    %% before the per-node calls below, each of which takes it for its own
    %% installation.
    case local_package_snapshot(NameVsn) of
        {ok, TarBin} ->
            Running = emqx:running_nodes(),
            %% A node which can not take the installation lock would install
            %% without it, so the cluster install only runs when every node can.
            LockNodes = [
                Node
             || Node <- Running,
                emqx_plugins:node_supports_install_lock(Node)
            ],
            case Running -- LockNodes of
                [] ->
                    {CalledNodes, Results} = install_on_nodes_until_refused(
                        LockNodes, NameVsn, TarBin
                    ),
                    print_cluster_result(CalledNodes, Results, NameVsn, LogFun);
                Missing ->
                    Reason = #{
                        hint => <<
                            "cluster install requires all nodes to support "
                            "the cluster wide installation lock"
                        >>,
                        nodes_without_install_lock => Missing
                    },
                    ?PRINT({error, Reason}, LogFun)
            end;
        {error, Reason} ->
            ?PRINT({error, Reason}, LogFun)
    end.

%% Install the snapshot on the nodes in order, and stop at the first node which
%% could not run the installation: it could not be reached, or it could not take
%% the cluster wide installation lock (`emqx_plugins:installation_refused/1').
%% Installing on the remaining nodes after one of them was refused would leave
%% the cluster with two different packages, and a retry of the same command can
%% then be refused by a node which was already updated, so the refused node
%% could not be repaired through this command any more.  The nodes which were
%% not reached keep what they had, and the caller retries when the lock is free.
%%
%% `emqx_plugins_proto_v5:install_package/3' is an `erpc' multicall: a node which
%% ran the call answers `{ok, Result}', and one which could not run it at all
%% answers with the caught call exception instead (`erpc:caught_call_exception/0',
%% for example `{error, {erpc, noconnection}}' for an unreachable node, or
%% `{error, {exception, _, _}}' for one which raised).  Only the first shape
%% carries an installation result; every other shape means the node did not run
%% the installation, so it counts as a refusal.
install_on_nodes_until_refused(Nodes, NameVsn, TarBin) ->
    install_on_nodes_until_refused(Nodes, NameVsn, TarBin, [], []).

install_on_nodes_until_refused([], _NameVsn, _TarBin, Called, Results) ->
    {lists:reverse(Called), lists:append(lists:reverse(Results))};
install_on_nodes_until_refused([Node | Rest], NameVsn, TarBin, Called, Results) ->
    NodeResults = emqx_plugins_proto_v5:install_package([Node], NameVsn, TarBin),
    Called1 = [Node | Called],
    Results1 = [NodeResults | Results],
    case lists:any(fun rpc_result_refused/1, NodeResults) of
        true -> install_on_nodes_until_refused([], NameVsn, TarBin, Called1, Results1);
        false -> install_on_nodes_until_refused(Rest, NameVsn, TarBin, Called1, Results1)
    end.

rpc_result_refused({ok, Result}) -> emqx_plugins:installation_refused(Result);
rpc_result_refused(_DidNotRun) -> true.

%% The package of this node as one immutable snapshot: the read has to be in
%% the same critical section as the writes of the shared package file.
local_package_snapshot(NameVsn) ->
    emqx_plugins:with_installation_lock(NameVsn, fun() ->
        emqx_plugins_fs:get_tar(NameVsn)
    end).

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
