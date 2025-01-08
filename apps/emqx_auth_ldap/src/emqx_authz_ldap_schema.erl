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

-module(emqx_authz_ldap_schema).

-include("emqx_auth_ldap.hrl").
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

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

fields(ldap) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        [
            {publish_attribute, attribute_meta(publish_attribute, <<"mqttPublishTopic">>)},
            {subscribe_attribute, attribute_meta(subscribe_attribute, <<"mqttSubscriptionTopic">>)},
            {all_attribute, attribute_meta(all_attribute, <<"mqttPubSubTopic">>)},
            {query_timeout,
                ?HOCON(
                    emqx_schema:timeout_duration_ms(),
                    #{
                        desc => ?DESC(query_timeout),
                        default => <<"5s">>
                    }
                )}
        ] ++
        emqx_ldap:fields(config).

desc(ldap) ->
    emqx_authz_ldap:description();
desc(_) ->
    undefined.

source_refs() ->
    [?R_REF(ldap)].

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, _) ->
    ?R_REF(ldap);
select_union_member(_Value, _) ->
    undefined.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

attribute_meta(Name, Default) ->
    ?HOCON(
        string(),
        #{
            default => Default,
            desc => ?DESC(Name)
        }
    ).
