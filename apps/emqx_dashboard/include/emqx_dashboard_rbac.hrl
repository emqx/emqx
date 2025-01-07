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

-endif.
