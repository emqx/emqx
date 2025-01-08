%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mnesia_schema).

-include("emqx_auth_mnesia.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    fields/1,
    desc/1,
    refs/1,
    select_union_member/2,
    namespace/0
]).

namespace() -> "authn".

refs(api_write) ->
    [?R_REF(builtin_db_api)];
refs(_) ->
    [?R_REF(builtin_db)].

select_union_member(Kind, #{
    <<"mechanism">> := ?AUTHN_MECHANISM_SIMPLE_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
}) ->
    refs(Kind);
select_union_member(_Kind, _Value) ->
    undefined.

fields(builtin_db) ->
    [
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_rw/1}
    ] ++ common_fields();
fields(builtin_db_api) ->
    [
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_rw_api/1}
    ] ++ common_fields().

desc(builtin_db) ->
    ?DESC(builtin_db);
desc(_) ->
    undefined.

user_id_type(type) -> hoconsc:enum([clientid, username]);
user_id_type(desc) -> ?DESC(?FUNCTION_NAME);
user_id_type(default) -> <<"username">>;
user_id_type(required) -> true;
user_id_type(_) -> undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_SIMPLE)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {user_id_type, fun user_id_type/1}
    ] ++ bootstrap_fields() ++
        emqx_authn_schema:common_fields().

bootstrap_fields() ->
    [
        {bootstrap_file,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bootstrap_file),
                    required => false,
                    default => <<"${EMQX_ETC_DIR}/auth-built-in-db-bootstrap.csv">>
                }
            )},
        {bootstrap_type,
            ?HOCON(
                ?ENUM([hash, plain]), #{
                    desc => ?DESC(bootstrap_type),
                    required => false,
                    default => <<"plain">>
                }
            )}
    ].
