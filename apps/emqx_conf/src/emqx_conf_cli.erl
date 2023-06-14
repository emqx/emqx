%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_conf.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").

-export([
    load/0,
    admins/1,
    conf/1,
    unload/0
]).

-include_lib("hocon/include/hoconsc.hrl").

%% kept cluster_call for compatibility
-define(CLUSTER_CALL, cluster_call).
-define(CONF, conf).
-define(UPDATE_READONLY_KEYS_PROHIBITED, "update_readonly_keys_prohibited").

load() ->
    emqx_ctl:register_command(?CLUSTER_CALL, {?MODULE, admins}, [hidden]),
    emqx_ctl:register_command(?CONF, {?MODULE, conf}, []).

unload() ->
    emqx_ctl:unregister_command(?CLUSTER_CALL),
    emqx_ctl:unregister_command(?CONF).

conf(["show_keys" | _]) ->
    print_keys(get_config());
conf(["show"]) ->
    print_hocon(get_config());
conf(["show", Key]) ->
    print_hocon(get_config(Key));
conf(["load", "--replace", Path]) ->
    load_config(Path, replace);
conf(["load", "--merge", Path]) ->
    load_config(Path, merge);
conf(["load", Path]) ->
    load_config(Path, merge);
conf(["cluster_sync" | Args]) ->
    admins(Args);
conf(["reload", "--merge"]) ->
    reload_etc_conf_on_local_node(merge);
conf(["reload", "--replace"]) ->
    reload_etc_conf_on_local_node(replace);
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
admins(["tnxid", TnxId0]) ->
    TnxId = list_to_integer(TnxId0),
    print(emqx_cluster_rpc:query(TnxId));
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
        {"conf cluster_sync status", "Show cluster config sync status summary"},
        {"conf cluster_sync skip [node]", "Increase one commit on specific node"},
        {"conf cluster_sync tnxid <TnxId>",
            "Display detailed information of the config change transaction at TnxId"},
        {"conf cluster_sync fast_forward [node] [tnx_id]",
            "Fast-forward config change transaction to tnx_id on the given node."
            "WARNING: This results in inconsistent configs among the clustered nodes."}
    ].

status() ->
    emqx_ctl:print("-----------------------------------------------\n"),
    {atomic, Status} = emqx_cluster_rpc:status(),
    lists:foreach(
        fun(S) ->
            #{
                node := Node,
                tnx_id := TnxId,
                mfa := {M, F, A},
                created_at := CreatedAt
            } = S,
            emqx_ctl:print(
                "~p:[~w] CreatedAt:~p ~p:~p/~w\n",
                [Node, TnxId, CreatedAt, M, F, length(A)]
            )
        end,
        Status
    ),
    emqx_ctl:print("-----------------------------------------------\n").

print_keys(Config) ->
    Keys = lists:sort(maps:keys(Config)),
    emqx_ctl:print("~1p~n", [[binary_to_existing_atom(K) || K <- Keys]]).

print(Json) ->
    emqx_ctl:print("~ts~n", [emqx_logger_jsonfmt:best_effort_json(Json)]).

print_hocon(Hocon) when is_map(Hocon) ->
    emqx_ctl:print("~ts~n", [hocon_pp:do(Hocon, #{})]);
print_hocon({error, Error}) ->
    emqx_ctl:warning("~ts~n", [Error]).

get_config() ->
    AllConf = emqx_config:fill_defaults(emqx:get_raw_config([])),
    drop_hidden_roots(AllConf).

drop_hidden_roots(Conf) ->
    lists:foldl(fun(K, Acc) -> maps:remove(K, Acc) end, Conf, hidden_roots()).

hidden_roots() ->
    [<<"trace">>, <<"stats">>, <<"broker">>, <<"persistent_session_store">>].

get_config(Key) ->
    case emqx:get_raw_config([Key], undefined) of
        undefined -> {error, "key_not_found"};
        Value -> emqx_config:fill_defaults(#{Key => Value})
    end.

-define(OPTIONS, #{rawconf_with_defaults => true, override_to => cluster}).
load_config(Path, ReplaceOrMerge) ->
    case hocon:files([Path]) of
        {ok, RawConf} when RawConf =:= #{} ->
            emqx_ctl:warning("load ~ts is empty~n", [Path]),
            {error, empty_hocon_file};
        {ok, RawConf} ->
            case check_config(RawConf) of
                ok ->
                    lists:foreach(
                        fun({K, V}) -> update_config_cluster(K, V, ReplaceOrMerge) end,
                        to_sorted_list(RawConf)
                    );
                {error, ?UPDATE_READONLY_KEYS_PROHIBITED = Reason} ->
                    emqx_ctl:warning("load ~ts failed~n~ts~n", [Path, Reason]),
                    emqx_ctl:warning(
                        "Maybe try `emqx_ctl conf reload` to reload etc/emqx.conf on local node~n"
                    ),
                    {error, Reason};
                {error, Errors} ->
                    emqx_ctl:warning("load ~ts schema check failed~n", [Path]),
                    lists:foreach(
                        fun({Key, Error}) ->
                            emqx_ctl:warning("~ts: ~p~n", [Key, Error])
                        end,
                        Errors
                    ),
                    {error, Errors}
            end;
        {error, Reason} ->
            emqx_ctl:warning("load ~ts failed~n~p~n", [Path, Reason]),
            {error, bad_hocon_file}
    end.

update_config_cluster(?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME = Key, Conf, merge) ->
    check_res(Key, emqx_authz:merge(Conf));
update_config_cluster(?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME = Key, Conf, merge) ->
    check_res(Key, emqx_authn:merge_config(Conf));
update_config_cluster(Key, NewConf, merge) ->
    Merged = merge_conf(Key, NewConf),
    check_res(Key, emqx_conf:update([Key], Merged, ?OPTIONS));
update_config_cluster(Key, Value, replace) ->
    check_res(Key, emqx_conf:update([Key], Value, ?OPTIONS)).

-define(LOCAL_OPTIONS, #{rawconf_with_defaults => true, persistent => false}).
update_config_local(?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME = Key, Conf, merge) ->
    check_res(node(), Key, emqx_authz:merge_local(Conf, ?LOCAL_OPTIONS));
update_config_local(?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME = Key, Conf, merge) ->
    check_res(node(), Key, emqx_authn:merge_config_local(Conf, ?LOCAL_OPTIONS));
update_config_local(Key, NewConf, merge) ->
    Merged = merge_conf(Key, NewConf),
    check_res(node(), Key, emqx:update_config([Key], Merged, ?LOCAL_OPTIONS));
update_config_local(Key, Value, replace) ->
    check_res(node(), Key, emqx:update_config([Key], Value, ?LOCAL_OPTIONS)).

check_res(Key, Res) -> check_res(cluster, Key, Res).
check_res(Mode, Key, {ok, _} = Res) ->
    emqx_ctl:print("load ~ts in ~p ok~n", [Key, Mode]),
    Res;
check_res(_Mode, Key, {error, Reason} = Res) ->
    emqx_ctl:warning("load ~ts failed~n~p~n", [Key, Reason]),
    Res.

check_config(Conf) ->
    case check_keys_is_not_readonly(Conf) of
        ok -> check_config_schema(Conf);
        Error -> Error
    end.

check_keys_is_not_readonly(Conf) ->
    Keys = maps:keys(Conf),
    ReadOnlyKeys = [atom_to_binary(K) || K <- ?READONLY_KEYS],
    case ReadOnlyKeys -- Keys of
        ReadOnlyKeys -> ok;
        _ -> {error, ?UPDATE_READONLY_KEYS_PROHIBITED}
    end.

check_config_schema(Conf) ->
    SchemaMod = emqx_conf:schema_module(),
    Fold = fun({Key, Value}, Acc) ->
        Schema = emqx_config_handler:schema(SchemaMod, [Key]),
        case emqx_conf:check_config(Schema, #{Key => Value}) of
            {ok, _} -> Acc;
            {error, Reason} -> [{Key, Reason} | Acc]
        end
    end,
    sorted_fold(Fold, Conf).

%% @doc Reload etc/emqx.conf to runtime config except for the readonly config
-spec reload_etc_conf_on_local_node(replace | merge) -> ok | {error, term()}.
reload_etc_conf_on_local_node(ReplaceOrMerge) ->
    case load_etc_config_file() of
        {ok, RawConf} ->
            case filter_readonly_config(RawConf) of
                {ok, Reloaded} ->
                    reload_config(Reloaded, ReplaceOrMerge);
                {error, Error} ->
                    emqx_ctl:warning("check config failed~n~p~n", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            emqx_ctl:warning("bad_hocon_file~n ~p~n", [Error]),
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
    RawDefault = emqx_config:fill_defaults(Raw),
    case emqx_conf:check_config(SchemaMod, RawDefault) of
        {ok, _CheckedConf} ->
            ReadOnlyKeys = [atom_to_binary(K) || K <- ?READONLY_KEYS],
            {ok, maps:without(ReadOnlyKeys, Raw)};
        {error, Error} ->
            ?SLOG(error, #{
                msg => "bad_etc_config_schema_found",
                error => Error
            }),
            {error, Error}
    end.

reload_config(AllConf, ReplaceOrMerge) ->
    Fold = fun({Key, Conf}, Acc) ->
        case update_config_local(Key, Conf, ReplaceOrMerge) of
            {ok, _} ->
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
    sorted_fold(Fold, AllConf).

sorted_fold(Func, Conf) ->
    case lists:foldl(Func, [], to_sorted_list(Conf)) of
        [] -> ok;
        Error -> {error, Error}
    end.

to_sorted_list(Conf) ->
    lists:keysort(1, maps:to_list(Conf)).

merge_conf(Key, NewConf) ->
    OldConf = emqx_conf:get_raw([Key]),
    do_merge_conf(OldConf, NewConf).

do_merge_conf(OldConf = #{}, NewConf = #{}) ->
    emqx_utils_maps:deep_merge(OldConf, NewConf);
do_merge_conf(_OldConf, NewConf) ->
    NewConf.
