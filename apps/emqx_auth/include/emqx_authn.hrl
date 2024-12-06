%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_authn_chains.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(AUTHN, emqx_authn_chains).

%% has to be the same as the root field name defined in emqx_schema
-define(CONF_NS, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME).
-define(CONF_NS_ATOM, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM).
-define(CONF_NS_BINARY, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY).

-type authenticator_id() :: binary().

-define(AUTHN_RESOURCE_GROUP, <<"authn">>).

%% VAR_NS_CLIENT_ATTRS is added here because it can be initialized before authn.
%% NOTE: authn return may add more to (or even overwrite) client_attrs.
-define(AUTHN_DEFAULT_ALLOWED_VARS, [
    ?VAR_USERNAME,
    ?VAR_CLIENTID,
    ?VAR_PASSWORD,
    ?VAR_PEERHOST,
    ?VAR_PEERPORT,
    ?VAR_CERT_SUBJECT,
    ?VAR_CERT_CN_NAME,
    ?VAR_CERT_PEM,
    ?VAR_ZONE,
    ?VAR_NS_CLIENT_ATTRS
]).

-endif.
