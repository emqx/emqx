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

-module(emqx_authn_ldap_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_auth_ldap.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() ->
    [?R_REF(ldap), ?R_REF(ldap_deprecated)].

select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN}) ->
    refs();
select_union_member(#{<<"backend">> := ?AUTHN_BACKEND_BIN}) ->
    throw(#{
        reason => "unknown_mechanism",
        expected => ?AUTHN_MECHANISM
    });
select_union_member(_) ->
    undefined.

fields(ldap_deprecated) ->
    common_fields() ++
        [
            {password_attribute, password_attribute()},
            {is_superuser_attribute, is_superuser_attribute()}
        ];
fields(ldap) ->
    common_fields() ++
        [
            {method,
                ?HOCON(
                    hoconsc:union(fun method_union_member_selector/1),
                    #{desc => ?DESC(method)}
                )}
        ];
fields(hash_method) ->
    [
        {type, method_type(hash)},
        {password_attribute, password_attribute()},
        {is_superuser_attribute, is_superuser_attribute()}
    ];
fields(bind_method) ->
    [{type, method_type(bind)}] ++ emqx_ldap:fields(bind_opts).

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {query_timeout, fun query_timeout/1}
    ] ++
        emqx_authn_schema:common_fields() ++
        emqx_ldap:fields(config).

desc(ldap) ->
    ?DESC(ldap);
desc(ldap_deprecated) ->
    ?DESC(ldap_deprecated);
desc(hash_method) ->
    ?DESC(hash_method);
desc(bind_method) ->
    ?DESC(bind_method);
desc(_) ->
    undefined.

method_union_member_selector(all_union_members) ->
    [?R_REF(hash_method), ?R_REF(bind_method)];
method_union_member_selector({value, Val}) ->
    Val2 =
        case is_map(Val) of
            true -> emqx_utils_maps:binary_key_map(Val);
            false -> Val
        end,
    case Val2 of
        #{<<"type">> := <<"bind">>} ->
            [?R_REF(bind_method)];
        #{<<"type">> := <<"hash">>} ->
            [?R_REF(hash_method)];
        _ ->
            throw(#{
                field_name => method,
                expected => [bind_method, hash_method]
            })
    end.

method_type(Type) ->
    ?HOCON(?ENUM([Type]), #{desc => ?DESC(?FUNCTION_NAME), default => Type}).

password_attribute() ->
    ?HOCON(
        string(),
        #{
            desc => ?DESC(?FUNCTION_NAME),
            default => <<"userPassword">>
        }
    ).

is_superuser_attribute() ->
    ?HOCON(
        string(),
        #{
            desc => ?DESC(?FUNCTION_NAME),
            default => <<"isSuperuser">>
        }
    ).

query_timeout(type) -> emqx_schema:timeout_duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.
