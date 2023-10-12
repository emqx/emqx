%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_enterprise).

-export([providers/0, resource_provider/0]).

-if(?EMQX_RELEASE_EDITION == ee).

providers() ->
    [
        {gcp_device, emqx_gcp_device_authn}
    ].

resource_provider() ->
    [].

-else.

providers() ->
    [].

resource_provider() ->
    [].
-endif.
