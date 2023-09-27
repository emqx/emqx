%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_ldap_bind_schema).

-include("emqx_auth_ldap.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

refs() ->
    [?R_REF(ldap_bind)].

select_union_member(#{
    <<"mechanism">> := ?AUTHN_MECHANISM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIND_BIN
}) ->
    refs();
select_union_member(#{<<"backend">> := ?AUTHN_BACKEND_BIND_BIN}) ->
    throw(#{
        reason => "unknown_mechanism",
        expected => ?AUTHN_MECHANISM
    });
select_union_member(_) ->
    undefined.

fields(ldap_bind) ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND_BIND)},
        {query_timeout, fun query_timeout/1}
    ] ++
        emqx_authn_schema:common_fields() ++
        emqx_ldap:fields(config) ++ emqx_ldap:fields(bind_opts).

desc(ldap_bind) ->
    ?DESC(ldap_bind);
desc(_) ->
    undefined.

query_timeout(type) -> emqx_schema:timeout_duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.
