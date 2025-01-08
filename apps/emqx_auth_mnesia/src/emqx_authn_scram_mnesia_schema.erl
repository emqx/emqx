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

-module(emqx_authn_scram_mnesia_schema).

-include("emqx_auth_mnesia.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-export([algorithm/1, iteration_count/1]).

namespace() -> "authn".

refs() ->
    [?R_REF(scram)].

select_union_member(#{
    <<"mechanism">> := ?AUTHN_MECHANISM_SCRAM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
}) ->
    refs();
select_union_member(_) ->
    undefined.

fields(scram) ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_SCRAM)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {algorithm, fun algorithm/1},
        {iteration_count, fun iteration_count/1}
    ] ++ emqx_authn_schema:common_fields().

desc(scram) ->
    "Settings for Salted Challenge Response Authentication Mechanism\n"
    "(SCRAM) authentication.";
desc(_) ->
    undefined.

algorithm(type) -> hoconsc:enum([sha256, sha512]);
algorithm(desc) -> "Hashing algorithm.";
algorithm(default) -> sha256;
algorithm(_) -> undefined.

iteration_count(type) -> non_neg_integer();
iteration_count(desc) -> "Iteration count.";
iteration_count(default) -> 4096;
iteration_count(_) -> undefined.
