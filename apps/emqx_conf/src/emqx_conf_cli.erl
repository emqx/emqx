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
-include_lib("emqx/include/emqx.hrl").
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
conf(["load", Path]) ->
    load_config(Path);
conf(["cluster_sync" | Args]) ->
    admins(Args);
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
        {"conf reload", "reload etc/emqx.conf on local node"},
        {"conf show_keys", "Print all config keys"},
        {"conf show [<key>]",
            "Print in-use configs (including default values) under the given key. "
            "Print ALL keys if key is not provided"},
        {"conf load <path>",
            "Load a HOCON format config file."
            "The config is overlay on top of the existing configs. "
            "The current node will initiate a cluster wide config change "
            "transaction to sync the changes to other nodes in the cluster. "
            "NOTE: do not make runtime config changes during rolling upgrade."}
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
    print(lists:sort(maps:keys(Config))).

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
    Hidden = hidden_roots(),
    maps:without(Hidden, Conf).

hidden_roots() ->
    SchemaModule = emqx_conf:schema_module(),
    Roots = hocon_schema:roots(SchemaModule),
    lists:filtermap(
        fun({BinName, {_RefName, Schema}}) ->
            case hocon_schema:field_schema(Schema, importance) =/= ?IMPORTANCE_HIDDEN of
                true ->
                    false;
                false ->
                    {true, BinName}
            end
        end,
        Roots
    ).

get_config(Key) ->
    case emqx:get_raw_config(Key, undefined) of
        undefined -> {error, "key_not_found"};
        Value -> emqx_config:fill_defaults(#{Key => Value})
    end.

-define(OPTIONS, #{rawconf_with_defaults => true, override_to => cluster}).
load_config(Path) ->
    case hocon:files([Path]) of
        {ok, RawConf} ->
            case check_config_keys(RawConf) of
                ok ->
                    maps:foreach(fun update_config/2, RawConf);
                {error, Reason} ->
                    emqx_ctl:warning("load ~ts failed~n~ts~n", [Path, Reason]),
                    emqx_ctl:warning(
                        "Maybe try `emqx_ctl conf reload` to reload etc/emqx.conf on local node~n"
                    ),
                    {error, Reason}
            end;
        {error, Reason} ->
            emqx_ctl:warning("load ~ts failed~n~p~n", [Path, Reason]),
            {error, bad_hocon_file}
    end.

update_config(Key, Value) ->
    case emqx_conf:update([Key], Value, ?OPTIONS) of
        {ok, _} ->
            emqx_ctl:print("load ~ts in cluster ok~n", [Key]);
        {error, Reason} ->
            emqx_ctl:warning("load ~ts failed~n~p~n", [Key, Reason])
    end.
check_config_keys(Conf) ->
    Keys = maps:keys(Conf),
    ReadOnlyKeys = [atom_to_binary(K) || K <- ?READ_ONLY_KEYS],
    case ReadOnlyKeys -- Keys of
        ReadOnlyKeys -> ok;
        _ -> {error, "update_read_only_keys_prohibited"}
    end.
