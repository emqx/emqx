%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_ldap_bind).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eldap/include/eldap.hrl").

-export([
    authenticate/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
authenticate(
    #{password := _Password} = Credential0,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate
    } = _State
) ->
    Credential = emqx_auth_template:rename_client_info_vars(Credential0),
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    Result = emqx_authn_utils:cached_simple_sync_query(
        CacheKey,
        ResourceId,
        {query, Credential, [], Timeout}
    ),
    case Result of
        {ok, []} ->
            ignore;
        {ok, [Entry]} ->
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, Entry#eldap_entry.object_name, Credential}
                )
            of
                {ok, #{result := ok}} ->
                    {ok, #{is_superuser => false}};
                {ok, #{result := 'invalidCredentials'}} ->
                    ?TRACE_AUTHN_PROVIDER(info, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => 'invalidCredentials'
                    }),
                    {error, bad_username_or_password};
                {error, Reason} ->
                    ?TRACE_AUTHN_PROVIDER(error, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => Reason
                    }),
                    {error, bad_username_or_password}
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.
