%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_cli).
-feature(maybe_expr, enable).

-include("emqx_conf.hrl").
-include_lib("emqx_auth/include/emqx_authn_chains.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_schema.hrl").

-export([
    load/0,
    admins/1,
    conf/1,
    audit/3,
    unload/0,
    mark_fix_log/2
]).

-export([keys/0, get_config/0, get_config/1, load_config/2]).

-include_lib("hocon/include/hoconsc.hrl").

%% kept cluster_call for compatibility
-define(CLUSTER_CALL, cluster_call).
-define(CONF, conf).
-define(AUDIT_MOD, audit).
-define(UPDATE_READONLY_KEYS_PROHIBITED, <<"Cannot update read-only key '~s'.">>).
-define(SCHEMA_VALIDATION_CONF_ROOT_BIN, <<"schema_validation">>).
-define(MESSAGE_TRANSFORMATION_CONF_ROOT_BIN, <<"message_transformation">>).
-define(LOCAL_OPTIONS, #{rawconf_with_defaults => true, persistent => false}).
-define(TIMEOUT, 30000).

%% All 'cluster.*' keys, except for 'cluster.link', should also be treated as read-only.
-define(READONLY_ROOT_KEYS, [rpc, node]).

-dialyzer({no_match, [load/0]}).

load() ->
    emqx_ctl:register_command(?CLUSTER_CALL, {?MODULE, admins}, [hidden]),
    emqx_ctl:register_command(?CONF, {?MODULE, conf}, []),
    case emqx_release:edition() of
        ee -> emqx_ctl:register_command(?AUDIT_MOD, {?MODULE, audit}, [hidden]);
        ce -> ok
    end,
    ok.

unload() ->
    emqx_ctl:unregister_command(?CLUSTER_CALL),
    emqx_ctl:unregister_command(?CONF),
    emqx_ctl:unregister_command(?AUDIT_MOD).

conf(["show_keys" | _]) ->
    print_keys(keys());
conf(["show"]) ->
    print_hocon(get_config());
conf(["show", Key]) ->
    print_hocon(get_config(list_to_binary(Key)));
conf(["load", "--replace", Path]) ->
    load_config(Path, #{mode => replace});
conf(["load", "--merge", Path]) ->
    load_config(Path, #{mode => merge});
conf(["load", Path]) ->
    load_config(Path, #{mode => merge});
conf(["cluster_sync" | Args]) ->
    admins(Args);
conf(["reload", "--merge"]) ->
    reload_etc_conf_on_local_node(#{mode => merge});
conf(["reload", "--replace"]) ->
    reload_etc_conf_on_local_node(#{mode => replace});
conf(["reload"]) ->
    conf(["reload", "--merge"]);
conf(_) ->
    emqx_ctl:usage(usage_conf() ++ usage_sync()).

admins(["status"]) ->
    status();
admins(["skip"]) ->
    status(),
    Nodes = mria:running_nodes(),
    lists:foreach(fun emqx_cluster_rpc:skip_failed_commit/1, Nodes),
    status();
admins(["skip", Node0]) ->
    status(),
    Node = list_to_existing_atom(Node0),
    emqx_cluster_rpc:skip_failed_commit(Node),
    status();
admins(["inspect", TnxId0]) ->
    TnxId = list_to_integer(TnxId0),
    print(emqx_cluster_rpc:query(TnxId));
admins(["fix"]) ->
    {atomic, Status} = emqx_cluster_rpc:status(),
    case mria_rlog:role() of
        core ->
            #{stopped_nodes := StoppedNodes} = emqx_mgmt_cli:cluster_info(),
            maybe_fix_lagging(Status, #{fix => true}),
            StoppedNodes =/= [] andalso
                emqx_ctl:warning("Found stopped nodes: ~p~n", [StoppedNodes]),
            ok;
        Role ->
            Leader = emqx_cluster_rpc:find_leader(),
            emqx_ctl:print("Run fix command on ~p(core) node, but current is ~p~n", [Leader, Role])
    end;
admins(["fast_forward"]) ->
    status(),
    Nodes = mria:running_nodes(),
    TnxId = emqx_cluster_rpc:latest_tnx_id(),
    lists:foreach(fun(N) -> emqx_cluster_rpc:fast_forward_to_commit(N, TnxId) end, Nodes),
    status();
admins(["fast_forward", ToTnxId]) ->
    status(),
    Nodes = mria:running_nodes(),
    TnxId = list_to_integer(ToTnxId),
    lists:foreach(fun(N) -> emqx_cluster_rpc:fast_forward_to_commit(N, TnxId) end, Nodes),
    status();
admins(["fast_forward", Node0, ToTnxId]) ->
    status(),
    TnxId = list_to_integer(ToTnxId),
    Node = list_to_existing_atom(Node0),
    emqx_cluster_rpc:fast_forward_to_commit(Node, TnxId),
    status();
admins(_) ->
    emqx_ctl:usage(usage_sync()).

fix_lagging_with_raw(ToTnxId, Node, Keys) ->
    Confs = lists:foldl(
        fun(Key, Acc) ->
            KeyRaw = atom_to_binary(Key),
            Acc#{KeyRaw => emqx_conf_proto_v4:get_raw_config(Node, [Key])}
        end,
        #{},
        Keys
    ),
    case mark_fix_begin(Node, ToTnxId) of
        ok ->
            case load_config_from_raw(Confs, #{mode => replace}) of
                ok -> waiting_for_fix_finish();
                Error0 -> Error0
            end;
        {error, Reason} ->
            emqx_ctl:warning("mark fix begin failed: ~s~n", [Reason])
    end.

mark_fix_begin(Node, TnxId) ->
    {atomic, Status} = emqx_cluster_rpc:status(),
    MFA = {?MODULE, mark_fix_log, [Status]},
    emqx_cluster_rpc:update_mfa(Node, MFA, TnxId).

mark_fix_log(Status, Opts) ->
    ?SLOG(warning, #{
        msg => cluster_config_sync_triggered,
        status => emqx_utils:redact(Status),
        opts => Opts
    }),
    ok.

audit(Level, From, Log) ->
    ?AUDIT(Level, redact(Log#{from => From})).

redact(Logs = #{cmd := admins, args := [<<"add">>, Username, _Password | Rest]}) ->
    Logs#{args => [<<"add">>, Username, <<"******">> | Rest]};
redact(Logs = #{cmd := admins, args := [<<"passwd">>, Username, _Password]}) ->
    Logs#{args => [<<"passwd">>, Username, <<"******">>]};
redact(Logs = #{cmd := license, args := [<<"update">>, _License]}) ->
    Logs#{args => [<<"update">>, "******"]};
redact(Logs) ->
    Logs.

usage_conf() ->
    [
        {"conf reload --replace|--merge", "reload etc/emqx.conf on local node"},
        {"", "The new configuration values will be overlaid on the existing values by default."},
        {"", "use the --replace flag to replace existing values with the new ones instead."},
        {"----------------------------------", "------------"},
        {"conf show_keys", "print all the currently used configuration keys."},
        {"conf show [<key>]",
            "Print in-use configs (including default values) under the given key."},
        {"", "Print ALL keys if key is not provided"},
        {"conf load --replace|--merge <path>", "Load a HOCON format config file."},
        {"", "The new configuration values will be overlaid on the existing values by default."},
        {"", "use the --replace flag to replace existing values with the new ones instead."},
        {"", "The current node will initiate a cluster wide config change"},
        {"", "transaction to sync the changes to other nodes in the cluster. "},
        {"", "NOTE: do not make runtime config changes during rolling upgrade."},
        {"----------------------------------", "------------"}
    ].

usage_sync() ->
    [
        {"conf cluster_sync status", "Show cluster config sync status summary for all nodes."},
        {"conf cluster_sync inspect <ID>",
            "Inspect detailed information of the config change transaction at the given commit ID"},
        {"conf cluster_sync skip [node]",
            "Increment the (currently failing) commit on the given node.\n"
            "WARNING: This results in inconsistent configs among the clustered nodes."},
        {"conf cluster_sync fast_forward [node] <ID>",
            "Fast-forward config change to the given commit ID on the given node.\n"
            "WARNING: This results in inconsistent configs among the clustered nodes."},
        {"conf cluster_sync fix",
            "Sync the node with the most comprehensive configuration to other node.\n"
            "WARNING: typically the config leader(with the highest tnxid)."}
    ].

status() ->
    {atomic, Status} = emqx_cluster_rpc:status(),
    status(Status).

status(Status) ->
    emqx_ctl:print("-----------------------------------------------\n"),
    #{stopped_nodes := StoppedNodes} = emqx_mgmt_cli:cluster_info(),
    maybe_fix_lagging(Status, #{fix => false}),
    StoppedNodes =/= [] andalso emqx_ctl:warning("Found stopped nodes: ~p~n", [StoppedNodes]),
    emqx_ctl:print("-----------------------------------------------\n").

maybe_fix_lagging(Status, #{fix := Fix}) ->
    %% find inconsistent in conf, but fix in raw way.
    %% because the raw conf is hard to be compared. (e.g, 1000ms vs 1s)
    AllConfs = find_running_confs(),
    case find_lagging(Status, AllConfs) of
        {inconsistent_tnx_id_key, ToTnxId, Target, InconsistentKeys} when Fix ->
            _ = fix_lagging_with_raw(ToTnxId, Target, InconsistentKeys),
            ok;
        {inconsistent_tnx_id_key, _ToTnxId, Target, InconsistentKeys} ->
            emqx_ctl:warning("Inconsistent keys: ~p~n", [InconsistentKeys]),
            print_inconsistent_conf(InconsistentKeys, Target, Status, AllConfs);
        {inconsistent_tnx_id, ToTnxId, Target} when Fix ->
            print_tnx_id_status(Status),
            case mark_fix_begin(Target, ToTnxId) of
                ok ->
                    waiting_for_fix_finish(),
                    emqx_ctl:print("Forward tnxid to ~w successfully~n", [ToTnxId + 1]);
                Error ->
                    Error
            end;
        {inconsistent_tnx_id, _ToTnxId, _Target} ->
            print_tnx_id_status(Status),
            Leader = emqx_cluster_rpc:find_leader(),
            emqx_ctl:print(?SUGGESTION(Leader));
        {inconsistent_key, ToTnxId, InconsistentKeys} ->
            [{Target, _} | _] = AllConfs,
            print_inconsistent_conf(InconsistentKeys, Target, Status, AllConfs),
            emqx_ctl:warning("All configuration synchronized(tnx_id=~w)~n", [
                ToTnxId
            ]),
            emqx_ctl:warning(
                "but inconsistent keys were found: ~p, which come from environment variables or etc/emqx.conf.~n",
                [InconsistentKeys]
            ),
            emqx_ctl:warning(
                "Configuring different values (excluding node.name) through environment variables and etc/emqx.conf"
                " is allowed but not recommended.~n"
            ),
            Fix andalso emqx_ctl:warning("So this fix will not make any changes.~n"),
            ok;
        {consistent, Msg} ->
            emqx_ctl:print(Msg)
    end.

print_tnx_id_status(List0) ->
    emqx_ctl:print("No inconsistent configuration found but has inconsistent tnxId ~n"),
    List1 = lists:map(fun(#{node := Node, tnx_id := TnxId}) -> {Node, TnxId} end, List0),
    emqx_ctl:print("~p~n", [List1]).

print_keys(Keys) ->
    SortKeys = lists:sort(Keys),
    emqx_ctl:print("~1p~n", [[binary_to_existing_atom(K) || K <- SortKeys]]).

print(Json) ->
    emqx_ctl:print("~ts~n", [emqx_logger_jsonfmt:best_effort_json(Json)]).

print_hocon(Hocon) when is_map(Hocon) ->
    emqx_ctl:print("~ts~n", [hocon_pp:do(Hocon, #{})]);
print_hocon(undefined) ->
    emqx_ctl:print("No value~n", []);
print_hocon({error, Error}) ->
    emqx_ctl:warning("~ts~n", [Error]).

get_config() ->
    AllConf = fill_defaults(emqx:get_raw_config([])),
    drop_hidden_roots(AllConf).

keys() ->
    emqx_config:get_root_names() -- hidden_roots().

drop_hidden_roots(Conf) ->
    maps:without(hidden_roots(), Conf).

hidden_roots() ->
    [
        <<"trace">>,
        <<"stats">>,
        <<"broker">>,
        <<"plugins">>,
        <<"zones">>
    ].

get_config(Key) ->
    case emqx:get_raw_config([Key], undefined) of
        undefined -> {error, "key_not_found"};
        Value -> fill_defaults(#{Key => Value})
    end.

-define(OPTIONS, #{rawconf_with_defaults => true, override_to => cluster}).
load_config(Path, Opts) when is_list(Path) ->
    case hocon:files([Path]) of
        {ok, RawConf} when RawConf =:= #{} ->
            case filelib:is_regular(Path) of
                true ->
                    emqx_ctl:warning("load ~ts is empty~n", [Path]),
                    {error, #{cause => empty_hocon_file, path => Path}};
                false ->
                    emqx_ctl:warning("~ts file is not found~n", [Path]),
                    {error, #{cause => not_a_file, path => Path}}
            end;
        {ok, RawConf} ->
            load_config_from_raw(RawConf, Opts);
        {error, Reason} ->
            emqx_ctl:warning("load ~ts failed~n~p~n", [Path, Reason]),
            {error, bad_hocon_file}
    end;
load_config(Bin, Opts) when is_binary(Bin) ->
    case hocon:binary(Bin) of
        {ok, RawConf} ->
            load_config_from_raw(RawConf, Opts);
        %% Type is scan_error, parse_error...
        {error, {Type, Meta = #{reason := Reason}}} ->
            {error, Meta#{
                reason => unicode:characters_to_binary(Reason),
                type => Type
            }};
        {error, Reason} ->
            {error, Reason}
    end.

load_config_from_raw(RawConf0, Opts) ->
    SchemaMod = emqx_conf:schema_module(),
    RawConf1 = emqx_config:upgrade_raw_conf(SchemaMod, RawConf0),
    case check_config(RawConf1, Opts) of
        {ok, RawConf} ->
            case update_cluster_links(cluster, RawConf, Opts) of
                ok ->
                    %% It has been ensured that the connector is always the first configuration to be updated.
                    %% However, when deleting the connector, we need to clean up the dependent actions/sources first;
                    %% otherwise, the deletion will fail.
                    %% notice: we can't create a action/sources before connector.
                    uninstall(<<"actions">>, RawConf, Opts),
                    uninstall(<<"sources">>, RawConf, Opts),
                    Error = update_config_cluster(Opts, RawConf),
                    case iolist_to_binary(Error) of
                        <<"">> -> ok;
                        ErrorBin -> {error, ErrorBin}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, ?UPDATE_READONLY_KEYS_PROHIBITED, ReadOnlyKeyStr} ->
            Reason = iolist_to_binary(
                io_lib:format(
                    ?UPDATE_READONLY_KEYS_PROHIBITED,
                    [ReadOnlyKeyStr]
                )
            ),
            warning(Opts, "load config failed~n~ts~n", [Reason]),
            warning(
                Opts,
                "Maybe try `emqx_ctl conf reload` to reload etc/emqx.conf on local node~n",
                []
            ),
            {error, Reason};
        {error, Errors} ->
            warning(Opts, "load schema check failed~n", []),
            lists:foreach(
                fun({Key, Error}) ->
                    warning(Opts, "~ts: ~p~n", [Key, Error])
                end,
                Errors
            ),
            {error, Errors}
    end.

update_config_cluster(Opts, RawConf) ->
    lists:filtermap(
        fun({K, V}) ->
            case update_config_cluster(K, V, Opts) of
                ok -> false;
                {error, Msg} -> {true, Msg}
            end
        end,
        to_sorted_list(RawConf)
    ).

update_cluster_links(cluster, #{<<"cluster">> := #{<<"links">> := Links}}, Opts) ->
    Res = emqx_conf:update([<<"cluster">>, <<"links">>], Links, ?OPTIONS),
    check_res(<<"cluster.links">>, Res, Links, Opts);
update_cluster_links(local, #{<<"cluster">> := #{<<"links">> := Links}}, Opts) ->
    Res = emqx:update_config([<<"cluster">>, <<"links">>], Links, ?LOCAL_OPTIONS),
    check_res(node(), <<"cluster.links">>, Res, Links, Opts);
update_cluster_links(_, _, _) ->
    ok.

uninstall(ActionOrSource, Conf, #{mode := replace}) ->
    case maps:find(ActionOrSource, Conf) of
        {ok, New} ->
            Old = emqx_conf:get_raw([ActionOrSource], #{}),
            ActionOrSourceAtom = binary_to_existing_atom(ActionOrSource),
            #{removed := Removed} = emqx_bridge_v2:diff_confs(New, Old),
            maps:foreach(
                fun({Type, Name}, _) ->
                    case emqx_bridge_v2:remove(ActionOrSourceAtom, Type, Name) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "failed_to_remove",
                                type => Type,
                                name => Name,
                                error => Reason
                            })
                    end
                end,
                Removed
            );
        error ->
            ok
    end;
%% we don't delete things when in merge mode or without actions/sources key.
uninstall(_, _RawConf, _) ->
    ok.

update_config_cluster(
    ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_BINARY = Key,
    Conf,
    #{mode := merge} = Opts
) ->
    check_res(Key, emqx_authz:merge(Conf), Conf, Opts);
update_config_cluster(
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY = Key,
    Conf,
    #{mode := merge} = Opts
) ->
    check_res(Key, emqx_authn:merge_config(Conf), Conf, Opts);
update_config_cluster(?SCHEMA_VALIDATION_CONF_ROOT_BIN = Key, NewConf, #{mode := merge} = Opts) ->
    check_res(Key, emqx_conf:update([Key], {merge, NewConf}, ?OPTIONS), NewConf, Opts);
update_config_cluster(?SCHEMA_VALIDATION_CONF_ROOT_BIN = Key, NewConf, #{mode := replace} = Opts) ->
    check_res(Key, emqx_conf:update([Key], {replace, NewConf}, ?OPTIONS), NewConf, Opts);
update_config_cluster(
    ?MESSAGE_TRANSFORMATION_CONF_ROOT_BIN = Key, NewConf, #{mode := merge} = Opts
) ->
    check_res(Key, emqx_conf:update([Key], {merge, NewConf}, ?OPTIONS), NewConf, Opts);
update_config_cluster(
    ?MESSAGE_TRANSFORMATION_CONF_ROOT_BIN = Key, NewConf, #{mode := replace} = Opts
) ->
    check_res(Key, emqx_conf:update([Key], {replace, NewConf}, ?OPTIONS), NewConf, Opts);
update_config_cluster(Key, NewConf, #{mode := merge} = Opts) ->
    Merged = merge_conf(Key, NewConf),
    check_res(Key, emqx_conf:update([Key], Merged, ?OPTIONS), NewConf, Opts);
update_config_cluster(Key, Value, #{mode := replace} = Opts) ->
    check_res(Key, emqx_conf:update([Key], Value, ?OPTIONS), Value, Opts).

update_config_local(
    ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_BINARY = Key,
    Conf,
    #{mode := merge} = Opts
) ->
    check_res(node(), Key, emqx_authz:merge_local(Conf, ?LOCAL_OPTIONS), Conf, Opts);
update_config_local(
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY = Key,
    Conf,
    #{mode := merge} = Opts
) ->
    check_res(node(), Key, emqx_authn:merge_config_local(Conf, ?LOCAL_OPTIONS), Conf, Opts);
update_config_local(Key, NewConf, #{mode := merge} = Opts) ->
    Merged = merge_conf(Key, NewConf),
    check_res(node(), Key, emqx:update_config([Key], Merged, ?LOCAL_OPTIONS), NewConf, Opts);
update_config_local(Key, Value, #{mode := replace} = Opts) ->
    check_res(node(), Key, emqx:update_config([Key], Value, ?LOCAL_OPTIONS), Value, Opts).

check_res(Key, Res, Conf, Opts) -> check_res(cluster, Key, Res, Conf, Opts).
check_res(Node, Key, {ok, _}, _Conf, Opts) ->
    print(Opts, "load ~ts on ~p ok~n", [Key, Node]),
    ok;
check_res(_Node, Key, {error, Reason}, Conf, Opts = #{mode := Mode}) ->
    Warning =
        "Can't ~ts the new configurations!~n"
        "Root key: ~ts~n"
        "Reason: ~p~n",
    ActiveMsg0 =
        "The effective configurations:~n"
        "```~n"
        "~ts```~n~n",
    ActiveMsg = io_lib:format(ActiveMsg0, [hocon_pp:do(#{Key => emqx_conf:get_raw([Key])}, #{})]),
    FailedMsg0 =
        "Try to ~ts with:~n"
        "```~n"
        "~ts```~n",
    FailedMsg = io_lib:format(FailedMsg0, [Mode, hocon_pp:do(#{Key => Conf}, #{})]),
    SuggestMsg = suggest_msg(Reason, Mode),
    Msg = iolist_to_binary([ActiveMsg, FailedMsg, SuggestMsg]),
    print(Opts, "~ts~n", [Msg]),
    warning(Opts, Warning, [Mode, Key, Reason]),
    {error, iolist_to_binary([Msg, "\n", io_lib:format(Warning, [Mode, Key, Reason])])}.

%% The mix data failed validation, suggest the user to retry with another mode.
suggest_msg(#{kind := validation_error, reason := unknown_fields}, Mode) ->
    RetryMode =
        case Mode of
            merge -> "replace";
            replace -> "merge"
        end,
    io_lib:format(
        "Tips: There may be some conflicts in the new configuration under `~ts` mode,~n"
        "Please retry with the `~ts` mode.",
        [Mode, RetryMode]
    );
suggest_msg(_, _) ->
    <<"">>.

check_config(Conf0, Opts) ->
    maybe
        {ok, Conf1} ?= check_keys_is_not_readonly(Conf0, Opts),
        {ok, Conf2} ?= check_cluster_keys(Conf1, Opts),
        Conf3 = emqx_config:fill_defaults(Conf2),
        ok ?= check_config_schema(Conf3),
        {ok, Conf3}
    else
        Error -> Error
    end.

check_keys_is_not_readonly(Conf, Opts) ->
    IgnoreReadonly = maps:get(ignore_readonly, Opts, false),
    Keys = maps:keys(Conf),
    ReadOnlyKeys = [atom_to_binary(K) || K <- ?READONLY_ROOT_KEYS],
    case lists:filter(fun(K) -> lists:member(K, Keys) end, ReadOnlyKeys) of
        [] ->
            {ok, Conf};
        BadKeys when IgnoreReadonly ->
            ?SLOG(info, #{msg => "readonly_root_keys_ignored", keys => BadKeys}),
            {ok, maps:without(BadKeys, Conf)};
        BadKeys ->
            BadKeysStr = lists:join(<<",">>, BadKeys),
            {error, ?UPDATE_READONLY_KEYS_PROHIBITED, BadKeysStr}
    end.

check_cluster_keys(Conf = #{<<"cluster">> := Cluster}, Opts) ->
    IgnoreReadonly = maps:get(ignore_readonly, Opts, false),
    case maps:keys(Cluster) -- [<<"links">>] of
        [] ->
            {ok, Conf};
        Keys when IgnoreReadonly ->
            ?SLOG(info, #{msg => "readonly_root_keys_ignored", keys => Keys}),
            {ok, Conf#{<<"cluster">> => maps:with([<<"links">>], Cluster)}};
        Keys ->
            BadKeys = [<<"cluster.", K/binary>> || K <- Keys],
            BadKeysStr = lists:join(<<",">>, BadKeys),
            {error, ?UPDATE_READONLY_KEYS_PROHIBITED, BadKeysStr}
    end;
check_cluster_keys(Conf, _Opts) ->
    {ok, Conf}.

check_config_schema(Conf) ->
    SchemaMod = emqx_conf:schema_module(),
    Fold = fun({Key, Value}, Acc) ->
        case check_config(SchemaMod, Key, Value) of
            {ok, _} -> Acc;
            {error, Reason} -> [{Key, Reason} | Acc]
        end
    end,
    sorted_fold(Fold, Conf).

%% @doc Reload etc/emqx.conf to runtime config except for the readonly config
-spec reload_etc_conf_on_local_node(#{mode => replace | merge}) -> ok | {error, term()}.
reload_etc_conf_on_local_node(Opts) ->
    case load_etc_config_file() of
        {ok, RawConf} ->
            case filter_readonly_config(RawConf) of
                {ok, Reloaded} ->
                    reload_config(Reloaded, Opts);
                {error, Error} ->
                    warning(Opts, "check config failed~n~p~n", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            warning(Opts, "bad_hocon_file~n ~p~n", [Error]),
            {error, bad_hocon_file}
    end.

%% @doc Merge etc/emqx.conf on top of cluster.hocon.
%% For example:
%% `authorization.sources` will be merged into cluster.hocon when updated via dashboard,
%% but `authorization.sources` in not in the default emqx.conf file.
%% To make sure all root keys in emqx.conf has a fully merged value.
load_etc_config_file() ->
    ConfFiles = emqx_config:config_files(),
    Opts = #{format => map, include_dirs => emqx_config:include_dirs()},
    case hocon:files(ConfFiles, Opts) of
        {ok, RawConf} ->
            HasDeprecatedFile = emqx_config:has_deprecated_file(),
            %% Merge etc.conf on top of cluster.hocon,
            %% Don't use map deep_merge, use hocon files merge instead.
            %% In order to have a chance to delete. (e.g. zones.zone1.mqtt = null)
            Keys = maps:keys(RawConf),
            MergedRaw = emqx_config:load_config_files(HasDeprecatedFile, ConfFiles),
            {ok, maps:with(Keys, MergedRaw)};
        {error, Error} ->
            ?SLOG(error, #{
                msg => "failed_to_read_etc_config",
                files => ConfFiles,
                error => Error
            }),
            {error, Error}
    end.

filter_readonly_config(Raw) ->
    SchemaMod = emqx_conf:schema_module(),
    try
        RawDefault = fill_defaults(Raw),
        _ = emqx_config:check_config(SchemaMod, RawDefault),
        {ok, maps:without([atom_to_binary(K) || K <- ?READONLY_ROOT_KEYS], Raw)}
    catch
        throw:Error ->
            ?SLOG(error, #{
                msg => "bad_etc_config_schema_found",
                error => Error
            }),
            {error, Error}
    end.

reload_config(AllConf, Opts) ->
    case update_cluster_links(local, AllConf, Opts) of
        ok ->
            uninstall(<<"actions">>, AllConf, Opts),
            uninstall(<<"sources">>, AllConf, Opts),
            Fold = fun({Key, Conf}, Acc) ->
                case update_config_local(Key, Conf, Opts) of
                    ok ->
                        Acc;
                    Error ->
                        ?SLOG(error, #{
                            msg => "failed_to_reload_etc_config",
                            key => Key,
                            value => Conf,
                            error => Error
                        }),
                        [{Key, Error} | Acc]
                end
            end,
            sorted_fold(Fold, AllConf);
        {error, Reason} ->
            {error, Reason}
    end.

sorted_fold(Func, Conf) ->
    case lists:foldl(Func, [], to_sorted_list(Conf)) of
        [] -> ok;
        Error -> {error, Error}
    end.

to_sorted_list(Conf0) ->
    Conf1 = maps:remove(<<"cluster">>, Conf0),
    %% connectors > actions/bridges > rule_engine
    Keys = [<<"connectors">>, <<"actions">>, <<"bridges">>, <<"rule_engine">>],
    {HighPriorities, Conf2} = split_high_priority_conf(Keys, Conf1, []),
    HighPriorities ++ lists:keysort(1, maps:to_list(Conf2)).

split_high_priority_conf([], Conf0, Acc) ->
    {lists:reverse(Acc), Conf0};
split_high_priority_conf([Key | Keys], Conf0, Acc) ->
    case maps:take(Key, Conf0) of
        error ->
            split_high_priority_conf(Keys, Conf0, Acc);
        {Value, Conf1} ->
            split_high_priority_conf(Keys, Conf1, [{Key, Value} | Acc])
    end.

merge_conf(Key, NewConf) ->
    OldConf = emqx_conf:get_raw([Key]),
    do_merge_conf(OldConf, NewConf).

do_merge_conf(OldConf = #{}, NewConf = #{}) ->
    emqx_utils_maps:deep_merge(OldConf, NewConf);
do_merge_conf(_OldConf, NewConf) ->
    NewConf.

fill_defaults(Conf) ->
    Conf1 = emqx_config:fill_defaults(Conf),
    filter_cluster_conf(Conf1).

-define(ALL_STRATEGY, [<<"manual">>, <<"static">>, <<"dns">>, <<"etcd">>, <<"k8s">>]).

filter_cluster_conf(#{<<"cluster">> := #{<<"discovery_strategy">> := Strategy} = Cluster} = Conf) ->
    Cluster1 = maps:without(lists:delete(Strategy, ?ALL_STRATEGY), Cluster),
    Conf#{<<"cluster">> => Cluster1};
filter_cluster_conf(Conf) ->
    Conf.

check_config(SchemaMod, Key, Value) ->
    try
        Schema = emqx_config_handler:schema(SchemaMod, [Key]),
        {_AppEnvs, CheckedConf} = emqx_config:check_config(Schema, #{Key => Value}),
        {ok, CheckedConf}
    catch
        throw:Error ->
            {error, Error}
    end.

warning(#{log := none}, _, _) -> ok;
warning(_, Format, Args) -> emqx_ctl:warning(Format, Args).

print(#{log := none}, _, _) -> ok;
print(_, Format, Args) -> emqx_ctl:print(Format, Args).

waiting_for_fix_finish() ->
    timer:sleep(1000),
    waiting_for_sync_finish(1).

waiting_for_sync_finish(10) ->
    emqx_ctl:warning(
        "Config is still not in sync after 10s.~n"
        "It may need more time, and check the logs in the lagging nodes.~n"
    );
waiting_for_sync_finish(Sec) ->
    {atomic, Status} = emqx_cluster_rpc:status(),
    case lists:usort([TnxId || #{tnx_id := TnxId} <- Status]) of
        [_] ->
            emqx_ctl:warning("sync successfully in ~ws ~n", [Sec]);
        _ ->
            Res = lists:sort([{TnxId, Node} || #{node := Node, tnx_id := TnxId} <- Status]),
            emqx_ctl:warning("sync status: ~p~n", [Res]),
            timer:sleep(1000),
            waiting_for_sync_finish(Sec + 1)
    end.

find_lagging(Status, AllConfs) ->
    case find_highest_node(Status) of
        {same_tnx_id, TnxId} ->
            %% check the conf is the same or not
            [{_, TargetConf} | OtherConfs] = AllConfs,
            case find_inconsistent_key(TargetConf, OtherConfs) of
                [] ->
                    Msg =
                        <<"All configuration synchronized(tnx_id=",
                            (integer_to_binary(TnxId))/binary, ") successfully\n">>,
                    {consistent, Msg};
                InconsistentKeys ->
                    {inconsistent_key, TnxId, InconsistentKeys}
            end;
        {ok, TargetId, Target} ->
            {value, {_, TargetConf}, OtherConfs} = lists:keytake(Target, 1, AllConfs),
            case find_inconsistent_key(TargetConf, OtherConfs) of
                [] -> {inconsistent_tnx_id, TargetId, Target};
                ChangedKeys -> {inconsistent_tnx_id_key, TargetId, Target, ChangedKeys}
            end
    end.

find_inconsistent_key(TargetConf, OtherConfs) ->
    lists:usort(
        lists:foldl(
            fun({_Node, OtherConf}, Changed) ->
                lists:filtermap(
                    fun({K, V}) -> changed(K, V, TargetConf) end,
                    maps:to_list(OtherConf)
                ) ++ Changed
            end,
            [],
            OtherConfs
        )
    ).

find_highest_node([]) ->
    {same_tnx_id, 0};
find_highest_node(Status) ->
    Ids = [{Id, Node} || #{tnx_id := Id, node := Node} <- Status],
    case lists:usort(fun({A, _}, {B, _}) -> A >= B end, Ids) of
        [{TnxId, _}] ->
            {same_tnx_id, TnxId};
        [{TnxId, Target} | _] ->
            {ok, TnxId, Target}
    end.

changed(K, V, Conf) ->
    case maps:find(K, Conf) of
        {ok, V1} when V =:= V1 -> false;
        _ -> {true, K}
    end.

find_running_confs() ->
    lists:map(
        fun(Node) ->
            Conf = emqx_conf_proto_v4:get_config(Node, []),
            {Node, maps:without(?READONLY_ROOT_KEYS, Conf)}
        end,
        mria:running_nodes()
    ).

print_inconsistent_conf(Keys, Target, Status, AllConfs) ->
    {value, {_, TargetConf}, OtherConfs} = lists:keytake(Target, 1, AllConfs),
    TargetTnxId = get_tnx_id(Target, Status),
    lists:foreach(
        fun(Key) ->
            lists:foreach(
                fun({Node, OtherConf}) ->
                    TargetV = maps:get(Key, TargetConf, undefined),
                    PrevV = maps:get(Key, OtherConf, undefined),
                    NodeTnxId = get_tnx_id(Node, Status),
                    Options = #{
                        key => Key,
                        node => {Node, NodeTnxId},
                        target => {Target, TargetTnxId}
                    },
                    print_inconsistent_conf(TargetV, PrevV, Options)
                end,
                OtherConfs
            )
        end,
        Keys
    ).

get_tnx_id(Node, Status) ->
    case lists:filter(fun(#{node := Node0}) -> Node0 =:= Node end, Status) of
        [] -> 0;
        [#{tnx_id := TnxId}] -> TnxId
    end.

print_inconsistent_conf(SameConf, SameConf, _Options) ->
    ok;
print_inconsistent_conf(New = #{}, Old = #{}, Options) ->
    #{
        added := Added,
        removed := Removed,
        changed := Changed
    } = emqx_utils_maps:diff_maps(New, Old),
    RemovedFmt = "~ts(~w)'s ~s has deleted certain keys, but they are still present on ~ts(~w).~n",
    print_inconsistent(Removed, RemovedFmt, Options),
    AddedFmt = "~ts(~w)'s ~s has new setting, but it has not been applied to ~ts(~w).~n",
    print_inconsistent(Added, AddedFmt, Options),
    ChangedFmt =
        "~ts(~w)'s ~s has been updated, but the changes have not been applied to ~ts(~w).~n",
    print_inconsistent(Changed, ChangedFmt, Options);
%% authentication rewrite topic_metrics is list(not map).
print_inconsistent_conf(New, Old, Options) ->
    #{
        key := Key,
        target := {Target, TargetTnxId},
        node := {Node, NodeTnxId}
    } = Options,
    emqx_ctl:print("~ts(tnx_id=~w)'s ~s is different from ~ts(tnx_id=~w).~n", [
        Node, NodeTnxId, Key, Target, TargetTnxId
    ]),
    emqx_ctl:print("~ts:~n", [Node]),
    print_hocon(Old),
    emqx_ctl:print("~ts:~n", [Target]),
    print_hocon(New).

print_inconsistent(Conf, Fmt, Options) when Conf =/= #{} ->
    #{
        key := Key,
        target := {Target, TargetTnxId},
        node := {Node, NodeTnxId}
    } = Options,
    emqx_ctl:warning(Fmt, [Target, TargetTnxId, Key, Node, NodeTnxId]),
    NodeRawConf = emqx_conf_proto_v4:get_raw_config(Node, [Key]),
    TargetRawConf = emqx_conf_proto_v4:get_raw_config(Target, [Key]),
    {TargetConf, NodeConf} =
        maps:fold(
            fun(SubKey, _, {NewAcc, OldAcc}) ->
                SubNew0 = maps:get(atom_to_binary(SubKey), NodeRawConf, undefined),
                SubOld0 = maps:get(atom_to_binary(SubKey), TargetRawConf, undefined),
                {SubNew1, SubOld1} = remove_identical_value(SubNew0, SubOld0),
                {NewAcc#{SubKey => SubNew1}, OldAcc#{SubKey => SubOld1}}
            end,
            {#{}, #{}},
            Conf
        ),
    %% zones.default is a virtual zone. It will be changed when mqtt changes,
    %% so we can't retrieve the raw data for zones.default(always undefined).
    case TargetConf =:= NodeConf of
        true -> ok;
        false -> print_hocon(#{Target => #{Key => TargetConf}, Node => #{Key => NodeConf}})
    end;
print_inconsistent(_Conf, _Format, _Options) ->
    ok.

remove_identical_value(New = #{}, Old = #{}) ->
    maps:fold(
        fun(K, NewV, {Acc1, Acc2}) ->
            case maps:find(K, Old) of
                {ok, NewV} ->
                    {maps:remove(K, Acc1), maps:remove(K, Acc2)};
                {ok, OldV} ->
                    {NewV1, OldV1} = remove_identical_value(NewV, OldV),
                    {maps:put(K, NewV1, Acc1), maps:put(K, OldV1, Acc2)}
            end
        end,
        {New, Old},
        New
    );
remove_identical_value(New, Old) ->
    {New, Old}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

find_inconsistent_test() ->
    Status = [
        #{node => 'node1', tnx_id => 1},
        #{node => 'node2', tnx_id => 3},
        #{node => 'node3', tnx_id => 2}
    ],
    Stats = #{<<"enable">> => false},

    %% chose the highest tnx_id node
    Mqtt = #{
        <<"await_rel_timeout">> => <<"300s">>,
        <<"exclusive_subscription">> => false,
        <<"idle_timeout">> => <<"15s">>
    },
    TargetMqtt1 = Mqtt#{<<"idle_timeout">> => <<"16s">>},
    Confs0 = [
        {node1, #{<<"mqtt">> => Mqtt#{<<"idle_timeout">> => <<"11s">>}, <<"stats">> => Stats}},
        {node3, #{<<"mqtt">> => Mqtt#{<<"idle_timeout">> => <<"17s">>}, <<"stats">> => Stats}},
        {node2, #{<<"mqtt">> => TargetMqtt1, <<"stats">> => Stats}}
    ],
    ?assertEqual(
        {inconsistent_tnx_id_key, 3, node2, [<<"mqtt">>]}, find_lagging(Status, Confs0)
    ),

    %% conf is the same, no changed
    NoDiffConfs = [
        {node1, #{<<"mqtt">> => Mqtt, <<"stats">> => Stats}},
        {node2, #{<<"mqtt">> => Mqtt, <<"stats">> => Stats}},
        {node3, #{<<"mqtt">> => Mqtt, <<"stats">> => Stats}}
    ],
    ?assertEqual({inconsistent_tnx_id, 3, node2}, find_lagging(Status, NoDiffConfs)),

    %% same tnx_id
    SameStatus = [
        #{node => 'node1', tnx_id => 3},
        #{node => 'node2', tnx_id => 3},
        #{node => 'node3', tnx_id => 3}
    ],
    %% same conf
    ?assertEqual(
        {consistent, <<"All configuration synchronized(tnx_id=3) successfully\n">>},
        find_lagging(SameStatus, NoDiffConfs)
    ),
    %% diff conf same tnx_id use the first one
    ?assertEqual(
        {inconsistent_key, 3, [<<"mqtt">>]},
        find_lagging(SameStatus, Confs0)
    ),
    ok.

-endif.
