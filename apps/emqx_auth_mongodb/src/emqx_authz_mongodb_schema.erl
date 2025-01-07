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

-module(emqx_authz_mongodb_schema).

-behaviour(emqx_authz_schema).

-export([
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    select_union_member/2,
    namespace/0
]).

-include("emqx_auth_mongodb.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

source_refs() ->
    [?R_REF(mongo_single), ?R_REF(mongo_rs), ?R_REF(mongo_sharded)].

fields(mongo_single) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        mongo_common_fields() ++
        emqx_mongodb:fields(single);
fields(mongo_rs) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        mongo_common_fields() ++
        emqx_mongodb:fields(rs);
fields(mongo_sharded) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        mongo_common_fields() ++
        emqx_mongodb:fields(sharded).

desc(mongo_single) ->
    ?DESC(mongo_single);
desc(mongo_rs) ->
    ?DESC(mongo_rs);
desc(mongo_sharded) ->
    ?DESC(mongo_sharded);
desc(_) ->
    undefined.

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN} = Value, _) ->
    MongoType = maps:get(<<"mongo_type">>, Value, undefined),
    case MongoType of
        <<"single">> ->
            ?R_REF(mongo_single);
        <<"rs">> ->
            ?R_REF(mongo_rs);
        <<"sharded">> ->
            ?R_REF(mongo_sharded);
        Else ->
            throw(#{
                reason => "unknown_mongo_type",
                expected => "single | rs | sharded",
                got => Else
            })
    end;
select_union_member(_Value, _) ->
    undefined.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

mongo_common_fields() ->
    [
        {collection,
            ?HOCON(binary(), #{
                required => true,
                desc => ?DESC(collection)
            })},
        {filter,
            ?HOCON(map(), #{
                required => false,
                default => #{},
                desc => ?DESC(filter)
            })},
        {limit,
            ?HOCON(pos_integer(), #{
                required => false,
                default => 1000,
                desc => ?DESC(limit)
            })},
        {skip,
            ?HOCON(non_neg_integer(), #{
                required => false,
                default => 0,
                desc => ?DESC(skip)
            })}
    ].
