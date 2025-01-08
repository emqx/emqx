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

-module(emqx_authn_redis_schema).

-include("emqx_auth_redis.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

namespace() -> "authn".

refs() ->
    [
        ?R_REF(redis_single),
        ?R_REF(redis_cluster),
        ?R_REF(redis_sentinel)
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

refs(#{<<"redis_type">> := <<"single">>}) ->
    [?R_REF(redis_single)];
refs(#{<<"redis_type">> := <<"cluster">>}) ->
    [?R_REF(redis_cluster)];
refs(#{<<"redis_type">> := <<"sentinel">>}) ->
    [?R_REF(redis_sentinel)];
refs(_) ->
    throw(#{
        field_name => redis_type,
        expected => "single | cluster | sentinel"
    }).

fields(Type) ->
    common_fields() ++ emqx_redis:fields(Type).

desc(redis_single) ->
    ?DESC(single);
desc(redis_cluster) ->
    ?DESC(cluster);
desc(redis_sentinel) ->
    ?DESC(sentinel);
desc(_) ->
    "".

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(redis)},
        {cmd, fun cmd/1},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1}
    ] ++ emqx_authn_schema:common_fields().

cmd(type) -> binary();
cmd(desc) -> ?DESC(?FUNCTION_NAME);
cmd(required) -> true;
cmd(_) -> undefined.
