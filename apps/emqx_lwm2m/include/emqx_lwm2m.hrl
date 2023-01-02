%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_lwm2m).

-record(coap_mqtt_auth, { clientid
                        , username
                        , password
                        }).
-record(lwm2m_context, { epn
                       , location
                       }).

-define(OMA_ALTER_PATH_RT, <<"\"oma.lwm2m\"">>).

-define(MQ_COMMAND_ID,         <<"CmdID">>).
-define(MQ_COMMAND,            <<"requestID">>).
-define(MQ_BASENAME,           <<"BaseName">>).
-define(MQ_ARGS,               <<"Arguments">>).

-define(MQ_VALUE_TYPE,         <<"ValueType">>).
-define(MQ_VALUE,              <<"Value">>).
-define(MQ_ERROR,              <<"Error">>).
-define(MQ_RESULT,             <<"Result">>).

-define(ERR_NO_XML,             <<"No XML Definition">>).
-define(ERR_NOT_ACCEPTABLE,     <<"Not Acceptable">>).
-define(ERR_METHOD_NOT_ALLOWED, <<"Method Not Allowed">>).
-define(ERR_NOT_FOUND,          <<"Not Found">>).
-define(ERR_UNAUTHORIZED,       <<"Unauthorized">>).
-define(ERR_BAD_REQUEST,        <<"Bad Request">>).


-define(LWM2M_FORMAT_PLAIN_TEXT, 0).
-define(LWM2M_FORMAT_LINK,       40).
-define(LWM2M_FORMAT_OPAQUE,     42).
-define(LWM2M_FORMAT_TLV,        11542).
-define(LWMWM_FORMAT_JSON,       11543).
