%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_connector).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eldap/include/eldap.hrl").
-include_lib("emqx_ldap.hrl").

-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-define(POOL_SEARCH, search).
-define(POOL_BIND, bind).

-type state() :: #{
    ?POOL_SEARCH := binary(),
    ?POOL_BIND := binary()
}.

%%-------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

resource_type() -> ldap.

callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, term()}.
on_start(InstId, Config) ->
    ?SLOG(info, #{
        msg => "starting_ldap_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    PoolOptions = emqx_ldap:pool_options(Config),

    %% NOTE
    %% We need two pools.
    %% * One for search requests. We authorize the search requests
    %% making bind with the config-provided credentials. So we cannot make bind
    %% requests to verify the user-provided credentials because we may loose
    %% authorization.
    %% * One for bind requests using the user-provided credentials.
    SearchPoolName = pool_name(InstId, <<"search">>),
    BindPoolName = pool_name(InstId, <<"bind">>),
    maybe
        ok = emqx_resource:allocate_resource(InstId, ?MODULE, ?POOL_SEARCH, SearchPoolName),
        ok ?=
            emqx_resource_pool:start(
                SearchPoolName,
                emqx_ldap,
                [{log_tag, "eldap_search_info"} | PoolOptions]
            ),
        ok = emqx_resource:allocate_resource(InstId, ?MODULE, ?POOL_BIND, BindPoolName),
        ok ?=
            emqx_resource_pool:start(
                BindPoolName,
                emqx_ldap,
                [{log_tag, "eldap_bind_info"} | PoolOptions]
            ),
        {ok, #{
            ?POOL_SEARCH => SearchPoolName,
            ?POOL_BIND => BindPoolName
        }}
    else
        {error, Reason} ->
            ?tp(
                ldap_pool_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_query(
    InstId,
    {bind, DN0, Password},
    #{?POOL_BIND := BindPoolName}
) ->
    LogMeta = #{connector => InstId},
    ?TRACE("QUERY", "ldap_connector_bind", LogMeta),
    maybe
        {ok, DN} ?= format_dn(DN0),
        ok ?=
            ecpool:pick_and_do(
                BindPoolName,
                {emqx_ldap, simple_bind, [DN, Password]},
                handover
            ),
        ?tp(
            ldap_bind_connector_query_return,
            #{result => ok}
        ),
        {ok, #{result => ok}}
    else
        {error, 'invalidCredentials'} ->
            {ok, #{result => 'invalidCredentials'}};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "ldap_bind_failed", reason => emqx_utils:redact(Reason)}
            ),
            {error, {unrecoverable_error, Reason}}
    end;
on_query(
    InstId,
    {query, Base0, Filter0, SearchOptions0},
    #{?POOL_SEARCH := SearchPoolName}
) ->
    LogMeta = #{connector => InstId, search => SearchOptions0, base => Base0, filter => Filter0},
    ?TRACE("QUERY", "ldap_connector_query", LogMeta),
    maybe
        {ok, Filter} ?= format_filter(Filter0),
        {ok, Base} ?= format_dn(Base0),
        SearchOptions = [{base, Base}, {filter, Filter} | SearchOptions0],
        {ok, Result} ?=
            ecpool:pick_and_do(
                SearchPoolName,
                {emqx_ldap, search, [SearchOptions]},
                handover
            ),
        {ok, _Entries} ?= search_result_to_entries(Result)
    else
        {error, 'noSuchObject'} ->
            {ok, []};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{
                    msg => "ldap_connector_query_failed",
                    reason => emqx_utils:redact(Reason)
                }
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_get_status(_InstId, #{?POOL_BIND := BindPoolName, ?POOL_SEARCH := SearchPoolName}) ->
    maybe
        ?status_connected ?= emqx_ldap:get_status_with_poolname(BindPoolName),
        ?status_connected ?= emqx_ldap:get_status_with_poolname(SearchPoolName)
    end.

on_stop(InstId, _State) ->
    _ = stop_resource_pool(InstId, ?POOL_BIND),
    _ = stop_resource_pool(InstId, ?POOL_SEARCH),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

format_dn(DN) when is_binary(DN) ->
    format_dn(binary_to_list(DN));
format_dn(DN) when is_list(DN) ->
    %% Assume that string DN is already valid
    {ok, DN};
format_dn(#ldap_dn{} = DN) ->
    {ok, emqx_ldap_dn:to_string(DN)};
format_dn(DN) ->
    {error, {invalid_dn, DN}}.

format_filter(Filter) when is_binary(Filter) ->
    format_filter(binary_to_list(Filter));
format_filter(Filter) when is_list(Filter) ->
    maybe
        {ok, ParsedFilter} ?= emqx_ldap_filter:parse(Filter),
        {ok, emqx_ldap_filter:to_eldap(ParsedFilter)}
    end;
format_filter(#ldap_search_filter{} = Filter) ->
    {ok, emqx_ldap_filter:to_eldap(Filter)};
%% Assume eldap filter
format_filter(Filter) ->
    {ok, Filter}.

search_result_to_entries(#eldap_search_result{entries = Entries} = Result) ->
    ?tp(
        ldap_connector_query_return,
        #{result => Result}
    ),
    Count = length(Entries),
    %% NOTE
    %% Accept only a single exact match.
    %% Multiple matches likely indicate:
    %% 1. A misconfiguration in EMQX, resulting into making too broad queries.
    %% 2. Indistinguishable entries in the LDAP database.
    %% Neither scenario should be allowed to proceed.
    case Count =< 1 of
        true ->
            {ok, Entries};
        false ->
            {error, ldap_query_found_more_than_one_match}
    end.

pool_name(InstId, Suffix) ->
    <<InstId/binary, "-", Suffix/binary>>.

stop_resource_pool(InstId, Pool) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{Pool := PoolName} ->
            ?SLOG(info, #{
                msg => "stopping_ldap_pool",
                pool => Pool,
                pool_name => PoolName
            }),
            emqx_resource_pool:stop(PoolName);
        _ ->
            ok
    end.
