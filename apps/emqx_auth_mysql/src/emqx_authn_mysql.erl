%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mysql).

-behaviour(emqx_authn_provider).

-define(PREPARE_KEY, ?MODULE).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include("emqx_auth_mysql.hrl").

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    maybe
        ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config0),
        ok ?=
            emqx_authn_utils:create_resource(
                emqx_mysql,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

update(Config0, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config0),
        ok ?=
            emqx_authn_utils:update_resource(
                emqx_mysql,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    #{password := Password} = Credential,
    #{
        tmpl_token := TmplToken,
        query_timeout := Timeout,
        resource_id := ResourceId,
        password_hash_algorithm := Algorithm,
        cache_key_template := CacheKeyTemplate
    }
) ->
    Params = emqx_auth_template:render_sql_params(TmplToken, Credential),
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    Result = emqx_authn_utils:cached_simple_sync_query(
        CacheKey, ResourceId, {prepared_query, ?PREPARE_KEY, Params, Timeout}
    ),
    case Result of
        {ok, _Columns, []} ->
            ignore;
        {ok, Columns, [Row | _]} ->
            Selected = maps:from_list(lists:zip(Columns, Row)),
            case
                emqx_authn_utils:check_password_from_selected_map(
                    Algorithm, Selected, Password
                )
            of
                ok ->
                    {ok, authn_result(Selected)};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "mysql_query_failed", #{
                resource => ResourceId,
                tmpl_token => TmplToken,
                params => Params,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.

create_state(
    ResourceId,
    #{
        password_hash_algorithm := Algorithm,
        query := Query0,
        query_timeout := QueryTimeout
    } = Config
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    {Vars, PrepareSql, TmplToken} = emqx_authn_utils:parse_sql(Query0, '?'),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    State = emqx_authn_utils:init_state(Config, #{
        password_hash_algorithm => Algorithm,
        tmpl_token => TmplToken,
        query_timeout => QueryTimeout,
        cache_key_template => CacheKeyTemplate,
        resource_id => ResourceId
    }),
    ResourceConfig = emqx_authn_utils:cleanup_resource_config(
        [query, query_timeout, password_hash_algorithm], Config
    ),
    {ok, ResourceConfig#{prepare_statement => #{?PREPARE_KEY => PrepareSql}}, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

authn_result(Selected) ->
    Res0 = emqx_authn_utils:is_superuser(Selected),
    Res1 = emqx_authn_utils:clientid_override(Selected),
    maps:merge(Res0, Res1).
