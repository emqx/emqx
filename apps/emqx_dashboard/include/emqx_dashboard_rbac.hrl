%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DASHBOARD_RBAC).
-define(EMQX_DASHBOARD_RBAC, true).

%% TODO:
%% The predefined roles of the preliminary RBAC implementation,
%% these may be removed when developing the full RBAC feature.
%% In full RBAC feature, the role may be customised created and deleted,
%% a predefined configuration would replace these macros.
-define(ROLE_VIEWER, <<"viewer">>).
-define(ROLE_SUPERUSER, <<"administrator">>).
-define(ROLE_DEFAULT, ?ROLE_SUPERUSER).

-define(ROLE_API_VIEWER, <<"viewer">>).
-define(ROLE_API_SUPERUSER, <<"administrator">>).
-define(ROLE_API_PUBLISHER, <<"publisher">>).
-define(ROLE_API_DEFAULT, ?ROLE_API_SUPERUSER).

-define(actor, actor).
-define(role, role).
-define(namespace, namespace).

-endif.
