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
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-define(PREPARE_KEY, ?MODULE).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
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
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(mysql)},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1},
        {query, fun query/1},
        {query_timeout, fun query_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++
        proplists:delete(prepare_statement, emqx_connector_mysql:fields(config)).

desc(?CONF_NS) ->
    ?DESC(?CONF_NS);
desc(_) ->
    undefined.

query(type) -> string();
query(desc) -> ?DESC(?FUNCTION_NAME);
query(required) -> true;
query(_) -> undefined.

query_timeout(type) -> emqx_schema:duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> "5s";
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ?CONF_NS)].

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {Config, State} = parse_config(Config0),
    {ok, _Data} = emqx_authn_utils:create_resource(ResourceId, emqx_connector_mysql, Config),
    {ok, State#{resource_id => ResourceId}}.

update(Config0, #{resource_id := ResourceId} = _State) ->
    {Config, NState} = parse_config(Config0),
    case emqx_authn_utils:update_resource(emqx_connector_mysql, Config, ResourceId) of
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
authenticate(
    #{password := Password} = Credential,
    #{
        tmpl_token := TmplToken,
        query_timeout := Timeout,
        resource_id := ResourceId,
        password_hash_algorithm := Algorithm
    }
) ->
    Params = emqx_authn_utils:render_sql_params(TmplToken, Credential),
    case emqx_resource:query(ResourceId, {prepared_query, ?PREPARE_KEY, Params, Timeout}) of
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
    {PrepareSql, TmplToken} = emqx_authn_utils:parse_sql(Query0, '?'),
    State = #{
        password_hash_algorithm => Algorithm,
        tmpl_token => TmplToken,
        query_timeout => QueryTimeout
    },
    {Config#{prepare_statement => #{?PREPARE_KEY => PrepareSql}}, State}.
