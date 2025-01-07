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

-module(emqx_authn_mongodb_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_auth_mongodb.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() ->
    [
        ?R_REF(mongo_single),
        ?R_REF(mongo_rs),
        ?R_REF(mongo_sharded)
    ].

select_union_member(
    #{
        <<"mechanism">> := ?AUTHN_MECHANISM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
    } = Value
) ->
    refs(Value);
select_union_member(#{<<"backend">> := ?AUTHN_BACKEND_BIN}) ->
    throw(#{
        reason => "unknown_mechanism",
        expected => ?AUTHN_MECHANISM
    });
select_union_member(_) ->
    undefined.

refs(#{<<"mongo_type">> := <<"single">>}) ->
    [?R_REF(mongo_single)];
refs(#{<<"mongo_type">> := <<"rs">>}) ->
    [?R_REF(mongo_rs)];
refs(#{<<"mongo_type">> := <<"sharded">>}) ->
    [?R_REF(mongo_sharded)];
refs(_) ->
    throw(#{
        field_name => mongo_type,
        expected => "single | rs | sharded"
    }).

fields(mongo_single) ->
    common_fields() ++ emqx_mongodb:fields(single);
fields(mongo_rs) ->
    common_fields() ++ emqx_mongodb:fields(rs);
fields(mongo_sharded) ->
    common_fields() ++ emqx_mongodb:fields(sharded).

desc(mongo_single) ->
    ?DESC(single);
desc(mongo_rs) ->
    ?DESC('replica-set');
desc(mongo_sharded) ->
    ?DESC('sharded-cluster');
desc(_) ->
    undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(mongodb)},
        {collection, fun collection/1},
        {filter, fun filter/1},
        {password_hash_field, fun password_hash_field/1},
        {salt_field, fun salt_field/1},
        {is_superuser_field, fun is_superuser_field/1},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1}
    ] ++ emqx_authn_schema:common_fields().

collection(type) -> binary();
collection(desc) -> ?DESC(?FUNCTION_NAME);
collection(required) -> true;
collection(_) -> undefined.

filter(type) ->
    map();
filter(desc) ->
    ?DESC(?FUNCTION_NAME);
filter(required) ->
    false;
filter(default) ->
    #{};
filter(_) ->
    undefined.

password_hash_field(type) -> binary();
password_hash_field(desc) -> ?DESC(?FUNCTION_NAME);
password_hash_field(required) -> false;
password_hash_field(default) -> <<"password_hash">>;
password_hash_field(_) -> undefined.

salt_field(type) -> binary();
salt_field(desc) -> ?DESC(?FUNCTION_NAME);
salt_field(required) -> false;
salt_field(default) -> <<"salt">>;
salt_field(_) -> undefined.

is_superuser_field(type) -> binary();
is_superuser_field(desc) -> ?DESC(?FUNCTION_NAME);
is_superuser_field(required) -> false;
is_superuser_field(default) -> <<"is_superuser">>;
is_superuser_field(_) -> undefined.
