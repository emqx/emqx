%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_enterprise).

-export([providers/0, resource_provider/0]).

-if(?EMQX_RELEASE_EDITION == ee).

providers() ->
    [
        {{password_based, ldap}, emqx_ldap_authn},
        {{password_based, ldap_bind}, emqx_ldap_authn_bind},
        {gcp_device, emqx_gcp_device_authn}
    ].

resource_provider() ->
    [emqx_ldap_authn, emqx_ldap_authn_bind].

-else.

providers() ->
    [].

resource_provider() ->
    [].
-endif.
