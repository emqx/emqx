%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_enterprise).

-include("emqx_authn_schema.hrl").

-export([provider_schema_mods/0]).

-if(?EMQX_RELEASE_EDITION == ee).

provider_schema_mods() ->
    ?EE_PROVIDER_SCHEMA_MODS.

-else.

provider_schema_mods() ->
    [].

-endif.
