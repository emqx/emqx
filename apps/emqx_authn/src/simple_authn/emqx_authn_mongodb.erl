%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mongodb).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

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

namespace() -> "authn-mongodb".

roots() ->
    [
        {?CONF_NS,
            hoconsc:mk(
                hoconsc:union(refs()),
                #{}
            )}
    ].

fields(standalone) ->
    common_fields() ++ emqx_connector_mongo:fields(single);
fields('replica-set') ->
    common_fields() ++ emqx_connector_mongo:fields(rs);
fields('sharded-cluster') ->
    common_fields() ++ emqx_connector_mongo:fields(sharded).

desc(standalone) ->
    "Configuration for a standalone MongoDB instance.";
desc('replica-set') ->
    "Configuration for a replica set.";
desc('sharded-cluster') ->
    "Configuration for a sharded cluster.";
desc(_) ->
    undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism('password_based')},
        {backend, emqx_authn_schema:backend(mongodb)},
        {collection, fun collection/1},
        {selector, fun selector/1},
        {password_hash_field, fun password_hash_field/1},
        {salt_field, fun salt_field/1},
        {is_superuser_field, fun is_superuser_field/1},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1}
    ] ++ emqx_authn_schema:common_fields().

collection(type) -> binary();
collection(desc) -> "Collection used to store authentication data.";
collection(_) -> undefined.

selector(type) ->
    map();
selector(desc) ->
    "Statement that is executed during the authentication process. "
    "Commands can support following wildcards:\n"
    " - `${username}`: substituted with client's username\n"
    " - `${clientid}`: substituted with the clientid";
selector(_) ->
    undefined.

password_hash_field(type) -> binary();
password_hash_field(desc) -> "Document field that contains password hash.";
password_hash_field(_) -> undefined.

salt_field(type) -> binary();
salt_field(desc) -> "Document field that contains the password salt.";
salt_field(required) -> false;
salt_field(_) -> undefined.

is_superuser_field(type) -> binary();
is_superuser_field(desc) -> "Document field that defines if the user has superuser privileges.";
is_superuser_field(required) -> false;
is_superuser_field(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [
        hoconsc:ref(?MODULE, standalone),
        hoconsc:ref(?MODULE, 'replica-set'),
        hoconsc:ref(?MODULE, 'sharded-cluster')
    ].

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{selector := Selector} = Config) ->
    SelectorTemplate = emqx_authn_utils:parse_deep(Selector),
    State = maps:with(
        [
            collection,
            password_hash_field,
            salt_field,
            is_superuser_field,
            password_hash_algorithm,
            salt_position
        ],
        Config
    ),
    #{password_hash_algorithm := Algorithm} = State,
    ok = emqx_authn_password_hashing:init(Algorithm),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    NState = State#{
        selector_template => SelectorTemplate,
        resource_id => ResourceId
    },
    case
        emqx_resource:create_local(
            ResourceId,
            ?RESOURCE_GROUP,
            emqx_connector_mongo,
            Config,
            #{}
        )
    of
        {ok, already_created} ->
            {ok, NState};
        {ok, _} ->
            {ok, NState};
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
        collection := Collection,
        selector_template := SelectorTemplate,
        resource_id := ResourceId
    } = State
) ->
    Selector = emqx_authn_utils:render_deep(SelectorTemplate, Credential),
    case emqx_resource:query(ResourceId, {find_one, Collection, Selector, #{}}) of
        undefined ->
            ignore;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "mongodb_query_failed",
                resource => ResourceId,
                collection => Collection,
                selector => Selector,
                reason => Reason
            }),
            ignore;
        Doc ->
            case check_password(Password, Doc, State) of
                ok ->
                    {ok, is_superuser(Doc, State)};
                {error, {cannot_find_password_hash_field, PasswordHashField}} ->
                    ?SLOG(error, #{
                        msg => "cannot_find_password_hash_field",
                        resource => ResourceId,
                        collection => Collection,
                        selector => Selector,
                        password_hash_field => PasswordHashField
                    }),
                    ignore;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

check_password(undefined, _Selected, _State) ->
    {error, bad_username_or_password};
check_password(
    Password,
    Doc,
    #{
        password_hash_algorithm := Algorithm,
        password_hash_field := PasswordHashField
    } = State
) ->
    case maps:get(PasswordHashField, Doc, undefined) of
        undefined ->
            {error, {cannot_find_password_hash_field, PasswordHashField}};
        Hash ->
            Salt =
                case maps:get(salt_field, State, undefined) of
                    undefined -> <<>>;
                    SaltField -> maps:get(SaltField, Doc, <<>>)
                end,
            case emqx_authn_password_hashing:check_password(Algorithm, Salt, Hash, Password) of
                true -> ok;
                false -> {error, bad_username_or_password}
            end
    end.

is_superuser(Doc, #{is_superuser_field := IsSuperuserField}) ->
    IsSuperuser = maps:get(IsSuperuserField, Doc, false),
    emqx_authn_utils:is_superuser(#{<<"is_superuser">> => IsSuperuser});
is_superuser(_, _) ->
    emqx_authn_utils:is_superuser(#{<<"is_superuser">> => false}).
