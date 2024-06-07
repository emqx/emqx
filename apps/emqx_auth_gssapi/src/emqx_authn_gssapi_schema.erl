%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_gssapi_schema).

-include("emqx_auth_gssapi.hrl").
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
    [?R_REF(gssapi)].

select_union_member(#{
    <<"mechanism">> := ?AUTHN_MECHANISM_GSSAPI_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
}) ->
    refs();
select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_GSSAPI_BIN}) ->
    throw(#{
        reason => "unknown_backend",
        expected => ?AUTHN_BACKEND
    });
select_union_member(_) ->
    undefined.

fields(gssapi) ->
    emqx_authn_schema:common_fields() ++
        [
            {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_GSSAPI)},
            {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
            {principal,
                ?HOCON(binary(), #{
                    required => true,
                    desc => ?DESC(emqx_bridge_kafka_consumer_schema, auth_kerberos_principal)
                })},
            {keytab_file,
                ?HOCON(binary(), #{
                    required => true,
                    desc => ?DESC(emqx_bridge_kafka_consumer_schema, auth_kerberos_keytab_file)
                })}
        ].

desc(gssapi) ->
    "Settings for GSSAPI authentication.";
desc(_) ->
    undefined.
