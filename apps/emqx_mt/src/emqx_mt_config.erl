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

    set_tenant_limiter_config/2,
    get_tenant_limiter_config/1,
    delete_tenant_limiter_config/1,

    set_client_limiter_config/2,
    get_client_limiter_config/1,
    delete_client_limiter_config/1
]).

%% Internal APIs for tests
-export([
    tmp_set_default_max_sessions/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(tenant, tenant).
-define(client, client).

-type tenant_config() :: emqx_mt_limiter:tenant_config().
-type client_config() :: emqx_mt_limiter:client_config().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Get the maximum number of sessions allowed for the given namespace.
%% TODO: support per-ns configs
-spec get_max_sessions(emqx_mt:tns()) -> non_neg_integer() | infinity.
get_max_sessions(_Ns) ->
    emqx_config:get([multi_tenancy, default_max_sessions]).

%% TODO: validate NS exists and is explicitly defined.
-spec set_tenant_limiter_config(emqx_mt:tns(), tenant_config()) ->
    ok | {error, {aborted, _}} | {error, not_found}.
set_tenant_limiter_config(Ns, Config) ->
    case emqx_mt_state:set_limiter_config(Ns, ?tenant, Config) of
        {ok, new} ->
            emqx_mt_limiter:create_tenant_limiter_group(Ns, Config);
        {ok, updated} ->
            emqx_mt_limiter:update_tenant_limiter_group(Ns, Config);
        {error, Reason} ->
            {error, Reason}
    end.

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

-spec delete_tenant_limiter_config(emqx_mt:tns()) ->
    ok | {error, not_found} | {error, {aborted, _}}.
delete_tenant_limiter_config(Ns) ->
    maybe
        ok ?= emqx_mt_state:delete_limiter_config(Ns, ?tenant),
        emqx_mt_limiter:delete_tenant_limiter_group(Ns)
    end.

%% TODO: validate NS exists and is explicitly defined.
-spec set_client_limiter_config(emqx_mt:tns(), tenant_config()) ->
    ok | {error, {aborted, _}} | {error, not_found}.
set_client_limiter_config(Ns, Config) ->
    case emqx_mt_state:set_limiter_config(Ns, ?client, Config) of
        {ok, new} ->
            emqx_mt_limiter:create_client_limiter_group(Ns, Config);
        {ok, updated} ->
            emqx_mt_limiter:update_client_limiter_group(Ns, Config);
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_client_limiter_config(emqx_mt:tns()) ->
    {ok, client_config()} | {error, not_found}.
get_client_limiter_config(Ns) ->
    emqx_mt_state:get_limiter_config(Ns, ?client).

-spec delete_client_limiter_config(emqx_mt:tns()) ->
    ok | {error, not_found} | {error, {aborted, _}}.
delete_client_limiter_config(Ns) ->
    maybe
        ok ?= emqx_mt_state:delete_limiter_config(Ns, ?client),
        emqx_mt_limiter:delete_client_limiter_group(Ns)
    end.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% @doc Temporarily set the maximum number of sessions allowed for the given namespace.
-spec tmp_set_default_max_sessions(non_neg_integer() | infinity) -> ok.
tmp_set_default_max_sessions(Max) ->
    emqx_config:put([multi_tenancy, default_max_sessions], Max).

cleanup_explicit_ns_configs(_Ns, []) ->
    ok;
cleanup_explicit_ns_configs(Ns, [{limiter, Configs} | Rest]) ->
    ok = emqx_mt_limiter:cleanup_configs(Ns, Configs),
    cleanup_explicit_ns_configs(Ns, Rest).
