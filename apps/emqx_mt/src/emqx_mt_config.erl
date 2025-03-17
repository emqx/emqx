%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_config).

-feature(maybe_expr, enable).

%% API
-export([
    get_max_sessions/1,

    create_explicit_ns/1,
    delete_explicit_ns/1,
    is_known_explicit_ns/1,

    get_explicit_ns_config/1,
    update_explicit_ns_config/2
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
    root_config/0
]).

-include_lib("emqx/include/logger.hrl").

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
        {ok, #{?session := #{?max_sessions := Max}}} ?= get_explicit_ns_config(Ns),
        Max
    else
        _ -> emqx_config:get([multi_tenancy, default_max_sessions])
    end.

-spec get_explicit_ns_config(emqx_mt:tns()) ->
    {ok, root_config()} | {error, not_found}.
get_explicit_ns_config(Ns) ->
    emqx_mt_state:get_root_configs(Ns).

-spec update_explicit_ns_config(emqx_mt:tns(), root_config()) ->
    {ok, #{
        configs := root_config(),
        errors := [#{?path := [atom()], error := {atom(), term()}}]
    }}
    | {error, not_found}.
update_explicit_ns_config(Ns, Configs) ->
    handle_root_configs_update(Ns, Configs).

-spec create_explicit_ns(emqx_mt:tns()) ->
    ok | {error, {aborted, _}} | {error, table_is_full}.
create_explicit_ns(Ns) ->
    emqx_mt_state:create_explicit_ns(Ns).

-spec delete_explicit_ns(emqx_mt:tns()) ->
    ok | {error, {aborted, _}}.
delete_explicit_ns(Ns) ->
    maybe
        {ok, Configs} ?= emqx_mt_state:delete_explicit_ns(Ns),
        cleanup_explicit_ns_configs(Ns, maps:to_list(Configs))
    end.

-spec is_known_explicit_ns(emqx_mt:tns()) -> boolean().
is_known_explicit_ns(Ns) ->
    emqx_mt_state:is_known_explicit_ns(Ns).

-spec get_tenant_limiter_config(emqx_mt:tns()) ->
    {ok, tenant_config()} | {error, not_found}.
get_tenant_limiter_config(Ns) ->
    emqx_mt_state:get_limiter_config(Ns, ?tenant).

-spec get_client_limiter_config(emqx_mt:tns()) ->
    {ok, client_config()} | {error, not_found}.
get_client_limiter_config(Ns) ->
    emqx_mt_state:get_limiter_config(Ns, ?client).

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% @doc Temporarily set the maximum number of sessions allowed for the given namespace.
-spec tmp_set_default_max_sessions(non_neg_integer() | infinity) -> ok.
tmp_set_default_max_sessions(Max) ->
    emqx_config:put([multi_tenancy, default_max_sessions], Max).

cleanup_explicit_ns_configs(_Ns, []) ->
    ok;
cleanup_explicit_ns_configs(Ns, [{?limiter, Configs} | Rest]) ->
    ok = emqx_mt_limiter:cleanup_configs(Ns, Configs),
    cleanup_explicit_ns_configs(Ns, Rest);
cleanup_explicit_ns_configs(Ns, [{_RootKey, _Configs} | Rest]) ->
    cleanup_explicit_ns_configs(Ns, Rest).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_root_configs_update(Ns, NewConfig) ->
    maybe
        {ok, #{diffs := Diffs, configs := Configs}} ?=
            emqx_mt_state:update_root_configs(Ns, NewConfig),
        SideEffects = compute_config_update_side_effects(Diffs, Ns),
        Errors = execute_side_effects(SideEffects),
        {ok, #{configs => Configs, errors => Errors}}
    end.

execute_side_effects(SideEffects) ->
    lists:foldl(
        fun(#{?op := {Fn, Args}, ?path := Path}, Acc) ->
            try
                _ = apply(Fn, Args),
                Acc
            catch
                Class:Reason:Stacktrace ->
                    ?SLOG(warning, #{
                        msg => "failed_to_execute_mt_side_effect",
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
    ).

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
