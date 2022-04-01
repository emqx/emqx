%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    roots/0,
    fields/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-mysql".

roots() -> [?CONF_NS].

fields(?CONF_NS) ->
    [
        {mechanism, emqx_authn_schema:mechanism('password_based')},
        {backend, emqx_authn_schema:backend(mysql)},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1},
        {query, fun query/1},
        {query_timeout, fun query_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++
        emqx_connector_mysql:fields(config).

query(type) -> string();
query(_) -> undefined.

query_timeout(type) -> emqx_schema:duration_ms();
query_timeout(default) -> "5s";
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ?CONF_NS)].

create(_AuthenticatorID, Config) ->
    create(Config).

create(
    #{
        password_hash_algorithm := Algorithm,
        query := Query0,
        query_timeout := QueryTimeout
    } = Config
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    {Query, PlaceHolders} = emqx_authn_utils:parse_sql(Query0, '?'),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    State = #{
        password_hash_algorithm => Algorithm,
        query => Query,
        placeholders => PlaceHolders,
        query_timeout => QueryTimeout,
        resource_id => ResourceId
    },
    case
        emqx_resource:create_local(
            ResourceId,
            ?RESOURCE_GROUP,
            emqx_connector_mysql,
            Config,
            #{}
        )
    of
        {ok, already_created} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

update(Config, State) ->
    case create(Config) of
        {ok, NewState} ->
            ok = destroy(State),
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    #{password := Password} = Credential,
    #{
        placeholders := PlaceHolders,
        query := Query,
        query_timeout := Timeout,
        resource_id := ResourceId,
        password_hash_algorithm := Algorithm
    }
) ->
    Params = emqx_authn_utils:render_sql_params(PlaceHolders, Credential),
    case emqx_resource:query(ResourceId, {sql, Query, Params, Timeout}) of
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
            ?SLOG(error, #{
                msg => "mysql_query_failed",
                resource => ResourceId,
                query => Query,
                params => Params,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.
