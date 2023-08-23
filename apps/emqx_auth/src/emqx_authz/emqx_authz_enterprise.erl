%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authz_enterprise).

-include("emqx_authz_schema.hrl").

-export([
    source_schema_mods/0
]).

-if(?EMQX_RELEASE_EDITION == ee).

source_schema_mods() ->
    ?EE_SOURCE_SCHEMA_MODS.

-else.

-dialyzer({nowarn_function, [source_schema_mods/0]}).

source_schema_mods() ->
    [].

-endif.
