%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_types).

-include("emqx_a2a_registry_internal.hrl").

-export_type([
    org_id/0,
    unit_id/0,
    agent_id/0
]).

-type org_id() :: binary().
-type unit_id() :: binary().
-type agent_id() :: binary().
