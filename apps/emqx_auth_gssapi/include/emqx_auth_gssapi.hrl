%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_AUTH_GSSAPI_HRL).
-define(EMQX_AUTH_GSSAPI_HRL, true).

-define(AUTHN_MECHANISM_GSSAPI, gssapi).
-define(AUTHN_MECHANISM_GSSAPI_BIN, <<"gssapi">>).

-define(AUTHN_BACKEND, gssapi).
-define(AUTHN_BACKEND_BIN, <<"gssapi">>).

-define(AUTHN_TYPE_GSSAPI, {?AUTHN_MECHANISM_GSSAPI, ?AUTHN_BACKEND}).

-endif.
