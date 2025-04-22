%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_config).

-feature(maybe_expr, enable).

-behaviour(emqx_config_backup).
-behaviour(emqx_db_backup).

%% API
-export([
    get_max_sessions/1,
    get_allow_only_managed_namespaces/0,
    set_allow_only_managed_namespaces/1,

    create_managed_ns/1,
    delete_managed_ns/1,
    is_known_managed_ns/1,

    get_managed_ns_config/1,
    update_managed_ns_config/2,

    bulk_import_configs/1
]).

%% BPAPI RPC Targets
-export([
    execute_side_effects_v1/1,
    execute_side_effects_v1/2,
    cleanup_managed_ns_configs_v1/2,
    cleanup_managed_ns_configs_v1/3
]).

%% Internal exports for `mt' application
-export([
    get_tenant_limiter_config/1,
    get_client_limiter_config/1
]).

%% Internal APIs for tests
-export([
    tmp_set_default_max_sessions/1
]).

-export_type([
    root_config/0,
    side_effect/0
]).

%% `emqx_config_backup' API
-export([import_config/1]).

%% `emqx_db_backup' API
-export([backup_tables/0, on_backup_table_imported/2]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mt.hrl").
-include("emqx_mt_internal.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Root configuration keys
-define(limiter, limiter).
-define(session, session).

-define(tenant, tenant).
-define(client, client).

-define(disabled, disabled).

-type limiter_config() :: #{
    ?tenant => disabled | tenant_config(),
    ?client => disabled | client_config()
}.
-type tenant_config() :: emqx_mt_limiter:tenant_config().
-type client_config() :: emqx_mt_limiter:client_config().

-define(max_sessions, max_sessions).
-type session_config() :: #{
    ?max_sessions => infinity | non_neg_integer()
}.

-type root_config() :: #{
    ?session => session_config(),
    ?limiter => limiter_config()
}.

-define(new, new).
-define(changed, changed).
-define(op, op).
-define(path, path).

-type side_effect() :: #{?op := {function(), [term()]}, ?path := [atom()]}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Get the maximum number of sessions allowed for the given namespace.
-spec get_max_sessions(emqx_mt:tns()) -> non_neg_integer() | infinity.
get_max_sessions(Ns) ->
    maybe
        {ok, #{?session := #{?max_sessions := Max}}} ?= get_managed_ns_config(Ns),
        Max
    else
        _ -> emqx_config:get([multi_tenancy, default_max_sessions])
    end.

-doc """
When `allow_only_managed_namespaces = true`, we don't allow clients from non-managed
namespaces to connect
""".
-spec get_allow_only_managed_namespaces() -> boolean().
get_allow_only_managed_namespaces() ->
    emqx_config:get([multi_tenancy, allow_only_managed_namespaces]).

-doc """
When `allow_only_managed_namespaces = true`, we don't allow clients from non-managed
namespaces to connect
""".
-spec set_allow_only_managed_namespaces(boolean()) -> ok.
set_allow_only_managed_namespaces(Bool) ->
    maybe
        {ok, _} ?=
            emqx_conf:update(
                [multi_tenancy, allow_only_managed_namespaces],
                Bool,
                #{override_to => cluster}
            ),
        ok
    end.

-spec get_managed_ns_config(emqx_mt:tns()) ->
    {ok, root_config()} | {error, not_found}.
get_managed_ns_config(Ns) ->
    emqx_mt_state:get_root_configs(Ns).

-spec update_managed_ns_config(emqx_mt:tns(), root_config()) ->
    {ok, #{
        configs := root_config(),
        errors := [#{?path := [atom()], error := {atom(), term()}}]
    }}
    | {error, not_found}.
update_managed_ns_config(Ns, Configs) ->
    handle_root_configs_update(Ns, Configs).

-spec bulk_import_configs([#{ns := emqx_mt:tns(), config := root_config()}]) ->
    {ok, #{
        errors := [
            #{
                ?path := [atom()],
                error := {atom(), term()}
            }
        ]
    }}
    | {error, {duplicated_nss, [emqx_mt:tns()]}}
    | {error, {aborted, term()}}.
bulk_import_configs(Entries) ->
    handle_bulk_import_configs(Entries).

-spec create_managed_ns(emqx_mt:tns()) ->
    ok | {error, {aborted, _}} | {error, table_is_full}.
create_managed_ns(Ns) ->
    emqx_mt_state:create_managed_ns(Ns).

-spec delete_managed_ns(emqx_mt:tns()) ->
    ok | {error, {aborted, _}}.
delete_managed_ns(Ns) ->
    maybe
        {ok, Configs} ?= emqx_mt_state:delete_managed_ns(Ns),
        emqx_mt_config_proto_v1:cleanup_managed_ns_configs(Ns, maps:to_list(Configs)),
        _ = emqx_mt_client_kicker:start_kicking(Ns),
        ok
    end.

-spec is_known_managed_ns(emqx_mt:tns()) -> boolean().
is_known_managed_ns(Ns) ->
    emqx_mt_state:is_known_managed_ns(Ns).

-spec get_tenant_limiter_config(emqx_mt:tns()) ->
    {ok, tenant_config()} | {error, not_found}.
get_tenant_limiter_config(Ns) ->
    emqx_mt_state:get_limiter_config(Ns, ?tenant).

-spec get_client_limiter_config(emqx_mt:tns()) ->
    {ok, client_config()} | {error, not_found}.
get_client_limiter_config(Ns) ->
    emqx_mt_state:get_limiter_config(Ns, ?client).

%%------------------------------------------------------------------------------
%% BPAPI RPC targets
%%------------------------------------------------------------------------------

%% Only to please BPAPI static checks...  Arity 2 version will be called directly by
%% `emqx_cluster_rpc:multicall'.
-spec execute_side_effects_v1([side_effect()]) -> {ok, [map()]}.
execute_side_effects_v1(SideEffects) ->
    execute_side_effects_v1(SideEffects, _ClusterRPCOpts = #{}).

-spec execute_side_effects_v1([side_effect()], emqx_config:cluster_rpc_opts()) -> {ok, [map()]}.
execute_side_effects_v1(SideEffects, _ClusterRPCOpts) ->
    Errors = lists:foldl(
        fun(#{?op := {Fn, Args}, ?path := Path}, Acc) ->
            try
                _ = apply(Fn, Args),
                Acc
            catch
                Class:Reason:Stacktrace ->
                    ?SLOG(warning, #{
                        msg => "failed_to_apply_multi_tenancy_config_change_runtime_effect",
                        ?path => Path,
                        ?op => Fn,
                        error => {Class, Reason},
                        stacktrace => Stacktrace
                    }),
                    [
                        #{
                            ?path => Path,
                            ?op => Fn,
                            error => {Class, Reason}
                        }
                        | Acc
                    ]
            end
        end,
        [],
        SideEffects
    ),
    %% Cluster RPC must see `{ok, _}', or else it will treat the operation as a failure...
    {ok, Errors}.

%% Only to please BPAPI static checks...  Arity 3 version will be called directly by
%% `emqx_cluster_rpc:multicall'.
-spec cleanup_managed_ns_configs_v1(emqx_mt:tns(), [{atom(), map()}]) -> ok.
cleanup_managed_ns_configs_v1(Ns, ConfigsList) ->
    cleanup_managed_ns_configs_v1(Ns, ConfigsList, _ClusterRPCOpts = #{}).

-spec cleanup_managed_ns_configs_v1(
    emqx_mt:tns(),
    [{atom(), map()}],
    emqx_config:cluster_rpc_opts()
) -> ok.
cleanup_managed_ns_configs_v1(Ns, ConfigsList, _ClusterRPCOpts) ->
    do_cleanup_managed_ns_configs(Ns, ConfigsList).

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{?CONF_ROOT_KEY_BIN := #{} = RawConf}) ->
    Result = emqx_conf:update(
        [?CONF_ROOT_KEY],
        RawConf,
        #{override_to => cluster, rawconf_with_defaults => true}
    ),
    case Result of
        {error, Reason} ->
            {error, #{root_key => ?CONF_ROOT_KEY, reason => Reason}};
        {ok, _} ->
            Keys0 = maps:keys(RawConf),
            Keys = lists:map(fun(K) -> [K] end, Keys0),
            {ok, #{root_key => ?CONF_ROOT_KEY, changed => Keys}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_ROOT_KEY, changed => []}}.

%%------------------------------------------------------------------------------
%% `emqx_db_backup' API
%%------------------------------------------------------------------------------

backup_tables() -> {<<"mt">>, emqx_mt_state:tables_to_backup()}.

on_backup_table_imported(?CONFIG_TAB, _Opts) ->
    Errors =
        emqx_mt_state:fold_managed_nss(
            fun(#{ns := Ns, configs := Configs}, Acc) ->
                emqx_mt_state:ensure_ns_added(Ns),
                %% By this point, the tables have already been overwritten...  So we assume
                %% that all NSs remaining have "just been added"...
                Diffs = #{added => Configs, changed => #{}},
                SideEffects = compute_config_update_side_effects(Diffs, Ns),
                Errors = execute_side_effects(SideEffects),
                case Errors of
                    [] ->
                        Acc;
                    _ ->
                        Acc#{Ns => Errors}
                end
            end,
            #{}
        ),
    case map_size(Errors) == 0 of
        true ->
            ok;
        false ->
            {error, Errors}
    end;
on_backup_table_imported(_Tab, _Opts) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% @doc Temporarily set the maximum number of sessions allowed for the given namespace.
-spec tmp_set_default_max_sessions(non_neg_integer() | infinity) -> ok.
tmp_set_default_max_sessions(Max) ->
    emqx_config:put([multi_tenancy, default_max_sessions], Max).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_cleanup_managed_ns_configs(_Ns, []) ->
    ok;
do_cleanup_managed_ns_configs(Ns, [{?limiter, Configs} | Rest]) ->
    ok = emqx_mt_limiter:cleanup_configs(Ns, Configs),
    do_cleanup_managed_ns_configs(Ns, Rest);
do_cleanup_managed_ns_configs(Ns, [{_RootKey, _Configs} | Rest]) ->
    do_cleanup_managed_ns_configs(Ns, Rest).

handle_root_configs_update(Ns, NewConfig) ->
    maybe
        {ok, #{diffs := Diffs, configs := Configs}} ?=
            emqx_mt_state:update_root_configs(Ns, NewConfig),
        SideEffects = compute_config_update_side_effects(Diffs, Ns),
        Errors = execute_side_effects(SideEffects),
        {ok, #{configs => Configs, errors => Errors}}
    end.

execute_side_effects(SideEffects) ->
    %% Foreced to wrap errors in `{ok, _}' by cluster RPC...
    {ok, Errors} = emqx_mt_config_proto_v1:execute_side_effects(SideEffects),
    Errors.

compute_config_update_side_effects(Diffs, Ns) ->
    #{added := Added0, changed := Changed0} = Diffs,
    Added = maps:map(fun(_RootKey, Cfg) -> {?new, Cfg} end, Added0),
    Changed = maps:map(fun(_RootKey, {OldCfg, NewCfg}) -> {?changed, OldCfg, NewCfg} end, Changed0),
    Changes = maps:merge(Added, Changed),
    maps:fold(
        fun
            (?limiter, Op, Acc) ->
                SideEffects = limiter_update_side_effects(Ns, Op),
                SideEffects ++ Acc;
            (_OtherRootKey, _Op, Acc) ->
                Acc
        end,
        [],
        Changes
    ).

-spec limiter_update_side_effects(emqx_mt:tns(), {?new, limiter_config()}) -> [side_effect()].
limiter_update_side_effects(Ns, {?new, Config}) ->
    lists:map(
        fun
            ({?tenant = Kind, ?disabled}) ->
                #{
                    ?op => {fun emqx_mt_limiter:ensure_tenant_limiter_group_absent/1, [Ns]},
                    ?path => [?limiter, Kind]
                };
            ({?tenant = Kind, #{} = Cfg}) ->
                #{
                    ?op => {fun emqx_mt_limiter:create_tenant_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                };
            ({?client = Kind, ?disabled}) ->
                #{
                    ?op => {fun emqx_mt_limiter:ensure_client_limiter_group_absent/1, [Ns]},
                    ?path => [?limiter, Kind]
                };
            ({?client = Kind, #{} = Cfg}) ->
                #{
                    ?op => {fun emqx_mt_limiter:create_client_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                }
        end,
        maps:to_list(Config)
    );
limiter_update_side_effects(Ns, {?changed, OldConfig, NewConfig}) ->
    #{
        added := Added,
        changed := Changed
    } = emqx_utils_maps:diff_maps(NewConfig, OldConfig),
    Changes0 = maps:merge(Added, Changed),
    Changes =
        maps:map(
            fun
                (_K, ?disabled = V) -> V;
                (_K, {_, ?disabled = V}) -> V;
                (_K, #{} = Cfg) -> {?new, Cfg};
                (_K, {?disabled, #{} = Cfg}) -> {?new, Cfg};
                (_K, {_, #{} = Cfg}) -> {?changed, Cfg}
            end,
            Changes0
        ),
    lists:map(
        fun
            ({?tenant = Kind, ?disabled}) ->
                #{
                    ?op => {fun emqx_mt_limiter:ensure_tenant_limiter_group_absent/1, [Ns]},
                    ?path => [?limiter, Kind]
                };
            ({?tenant = Kind, {?new, #{} = Cfg}}) ->
                #{
                    ?op => {fun emqx_mt_limiter:create_tenant_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                };
            ({?tenant = Kind, {?changed, #{} = Cfg}}) ->
                #{
                    ?op => {fun emqx_mt_limiter:update_tenant_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                };
            ({?client = Kind, ?disabled}) ->
                #{
                    ?op => {fun emqx_mt_limiter:ensure_client_limiter_group_absent/1, [Ns]},
                    ?path => [?limiter, Kind]
                };
            ({?client = Kind, {?new, #{} = Cfg}}) ->
                #{
                    ?op => {fun emqx_mt_limiter:create_client_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                };
            ({?client = Kind, {?changed, #{} = Cfg}}) ->
                #{
                    ?op => {fun emqx_mt_limiter:update_client_limiter_group/2, [Ns, Cfg]},
                    ?path => [?limiter, Kind]
                }
        end,
        maps:to_list(Changes)
    ).

handle_bulk_import_configs(Entries) ->
    maybe
        ok ?= validate_unique_nss(Entries),
        {ok, Changes} ?= emqx_mt_state:bulk_import_configs(Entries),
        SideEffects = compute_bulk_side_effects(Changes),
        Errors = execute_side_effects(SideEffects),
        {ok, #{errors => Errors}}
    end.

validate_unique_nss(Entries) ->
    NSs0 = lists:map(fun(#{ns := Ns}) -> Ns end, Entries),
    NSs = lists:sort(NSs0),
    UniqueNSs = lists:usort(NSs0),
    Duplicated = NSs -- UniqueNSs,
    case Duplicated of
        [] ->
            ok;
        _ ->
            {error, {duplicated_nss, Duplicated}}
    end.

compute_bulk_side_effects(Changes) ->
    maps:fold(
        fun(Ns, #{old := Old, new := New}, Acc) ->
            Diffs = emqx_utils_maps:diff_maps(New, Old),
            SEs0 = compute_config_update_side_effects(Diffs, Ns),
            SEs = lists:map(fun(#{?path := Path} = SE) -> SE#{?path := [Ns | Path]} end, SEs0),
            SEs ++ Acc
        end,
        [],
        Changes
    ).
