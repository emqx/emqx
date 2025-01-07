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

-module(emqx_authz_mnesia_schema).

-include("emqx_auth_mnesia.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authz_schema).

-export([
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    select_union_member/2,
    namespace/0
]).

-define(MAX_RULES, 100).

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

fields(builtin_db) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        [
            {max_rules,
                ?HOCON(
                    pos_integer(),
                    #{
                        default => ?MAX_RULES,
                        desc => ?DESC(max_rules)
                    }
                )}
        ].

source_refs() ->
    [?R_REF(builtin_db)].

desc(builtin_db) ->
    ?DESC(builtin_db);
desc(_) ->
    undefined.

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, _) ->
    ?R_REF(builtin_db);
select_union_member(_Value, _) ->
    undefined.
