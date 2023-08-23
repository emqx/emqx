%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_enterprise).

-include("emqx_authn_schema.hrl").

-export([provider_schema_mods/0]).

-if(?EMQX_RELEASE_EDITION == ee).

% providers() ->
%     [
%         {{password_based, ldap}, emqx_authn_ldap},
%         {{password_based, ldap_bind}, emqx_ldap_authn_bind},
%         {gcp_device, emqx_gcp_device_authn}
%     ].

% resource_provider() ->
%     [emqx_authn_ldap, emqx_ldap_authn_bind].

provider_schema_mods() ->
    ?EE_PROVIDER_SCHEMA_MODS.

-else.

provider_schema_mods() ->
    [].

% providers() ->
%     [].

% resource_provider() ->
%      [].
-endif.
