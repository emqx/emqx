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
-ifndef(EMQX_SCHEMA_HRL).
-define(EMQX_SCHEMA_HRL, true).

-define(TOMBSTONE_TYPE, marked_for_deletion).
-define(TOMBSTONE_VALUE, <<"marked_for_deletion">>).
-define(TOMBSTONE_CONFIG_CHANGE_REQ, mark_it_for_deletion).
-define(CONFIG_NOT_FOUND_MAGIC, '$0tFound').

%%--------------------------------------------------------------------
%% EE injections
%%--------------------------------------------------------------------
-define(EMQX_SSL_FUN_MFA(Name), {emqx_ssl_fun_mfa, Name}).

-endif.
