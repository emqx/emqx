%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_AUTHN_HRL).
-define(EMQX_AUTHN_HRL, true).

-include_lib("emqx/include/emqx_authentication.hrl").

-define(APP, emqx_authn).

-define(AUTHN, emqx_authentication).

-define(GLOBAL, 'mqtt:global').

-define(RE_PLACEHOLDER, "\\$\\{[a-z0-9\\-]+\\}").

-define(AUTH_SHARD, emqx_authn_shard).

%% has to be the same as the root field name defined in emqx_schema
-define(CONF_NS, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME).
-define(CONF_NS_ATOM, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM).
-define(CONF_NS_BINARY, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY).

-type authenticator_id() :: binary().

-endif.

-define(RESOURCE_GROUP, <<"emqx_authn">>).
