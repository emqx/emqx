%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_ldap).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1,
    do_create/2
]).

-import(proplists, [get_value/2, get_value/3]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    do_create(?MODULE, Config).

do_create(Module, Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(Module),
    Config = filter_placeholders(Config0),
    State = parse_config(Config),
    {ok, _Data} = emqx_authn_utils:create_resource(ResourceId, emqx_ldap, Config),
    {ok, State#{resource_id => ResourceId}}.

update(Config, #{resource_id := ResourceId} = _State) ->
    NState = parse_config(Config),
    case emqx_authn_utils:update_resource(emqx_ldap, Config, ResourceId) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(Credential, #{method := #{type := Type}} = State) ->
    case Type of
        hash ->
            emqx_authn_ldap_hash:authenticate(Credential, State);
        bind ->
            emqx_authn_ldap_bind:authenticate(Credential, State)
    end.

%% it used the deprecated config form
parse_config(
    #{password_attribute := PasswordAttr, is_superuser_attribute := IsSuperuserAttr} = Config0
) ->
    Config = maps:without([password_attribute, is_superuser_attribute], Config0),
    parse_config(Config#{
        method => #{
            type => hash,
            password_attribute => PasswordAttr,
            is_superuser_attribute => IsSuperuserAttr
        }
    });
parse_config(Config) ->
    maps:with([query_timeout, method], Config).

filter_placeholders(#{base_dn := BaseDN0, filter := Filter0} = Config0) ->
    BaseDN = emqx_auth_template:escape_disallowed_placeholders_str(
        BaseDN0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Filter = emqx_auth_template:escape_disallowed_placeholders_str(
        Filter0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Config1 = filter_bind_password_placeholders(Config0),
    Config1#{base_dn => BaseDN, filter => Filter}.

filter_bind_password_placeholders(#{method := #{bind_password := Password0} = Method} = Config0) ->
    Password = emqx_auth_template:escape_disallowed_placeholders_str(
        Password0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Config0#{method => Method#{bind_password => Password}};
filter_bind_password_placeholders(Config) ->
    Config.
