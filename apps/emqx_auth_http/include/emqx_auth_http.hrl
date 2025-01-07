%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_AUTH_HTTP_HRL).
-define(EMQX_AUTH_HTTP_HRL, true).

-define(AUTHZ_TYPE, http).
-define(AUTHZ_TYPE_BIN, <<"http">>).

-define(AUTHN_MECHANISM, password_based).
-define(AUTHN_MECHANISM_BIN, <<"password_based">>).

-define(AUTHN_MECHANISM_SCRAM, scram).
-define(AUTHN_MECHANISM_SCRAM_BIN, <<"scram">>).

-define(AUTHN_BACKEND, http).
-define(AUTHN_BACKEND_BIN, <<"http">>).
-define(AUTHN_TYPE, {?AUTHN_MECHANISM, ?AUTHN_BACKEND}).
-define(AUTHN_TYPE_SCRAM, {?AUTHN_MECHANISM_SCRAM, ?AUTHN_BACKEND}).

-define(AUTHN_DATA_FIELDS, [is_superuser, client_attrs, expire_at, acl]).

-endif.
