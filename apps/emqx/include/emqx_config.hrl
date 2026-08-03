%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_CONFIG_HRL).
-define(EMQX_CONFIG_HRL, true).

-define(global_ns, global).

%% Default for `multi_tenancy.deny_namespaces': names that collide with
%% internal sentinels once collapsed to their textual form.
-define(DEFAULT_DENY_NAMESPACES, [<<"global">>, <<"undefined">>, <<"null">>, <<"none">>]).

-endif.
