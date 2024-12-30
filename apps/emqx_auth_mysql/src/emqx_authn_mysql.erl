%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mysql).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-behaviour(emqx_authn_provider).

-define(PREPARE_KEY, ?MODULE).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {Config, State} = parse_config(Config0),
    {ok, _Data} = emqx_authn_utils:create_resource(ResourceId, emqx_mysql, Config),
    {ok, State#{resource_id => ResourceId}}.

update(Config0, #{resource_id := ResourceId} = _State) ->
    {Config, NState} = parse_config(Config0),
    case emqx_authn_utils:update_resource(emqx_mysql, Config, ResourceId) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}}
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
                    {ok, emqx_authn_utils:is_superuser(Selected)};
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

parse_config(
    #{
        password_hash_algorithm := Algorithm,
        query := Query0,
        query_timeout := QueryTimeout
    } = Config
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    {Vars, PrepareSql, TmplToken} = emqx_authn_utils:parse_sql(Query0, '?'),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    State = #{
        password_hash_algorithm => Algorithm,
        tmpl_token => TmplToken,
        query_timeout => QueryTimeout,
        cache_key_template => CacheKeyTemplate
    },
    {Config#{prepare_statement => #{?PREPARE_KEY => PrepareSql}}, State}.
