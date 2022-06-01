%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% HTTP API Auth
-define(WRONG_USERNAME_OR_PWD, 'WRONG_USERNAME_OR_PWD').
-define(WRONG_USERNAME_OR_PWD_OR_API_KEY_OR_API_SECRET,
    'WRONG_USERNAME_OR_PWD_OR_API_KEY_OR_API_SECRET'
).

%% Bad Request
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_MATCH, 'NOT_MATCH').

-define(ALREADY_EXISTS, 'ALREADY_EXISTS').
-define(BAD_CONFIG_SCHEMA, 'BAD_CONFIG_SCHEMA').
-define(BAD_LISTENER_ID, 'BAD_LISTENER_ID').
-define(BAD_NODE_NAME, 'BAD_NODE_NAME').
-define(BAD_RPC, 'BAD_RPC').
-define(BAD_TOPIC, 'BAD_TOPIC').
-define(EXCEED_LIMIT, 'EXCEED_LIMIT').
-define(INVALID_PARAMETER, 'INVALID_PARAMETER').
-define(CONFLICT, 'CONFLICT').
-define(NO_DEFAULT_VALUE, 'NO_DEFAULT_VALUE').
-define(DEPENDENCY_EXISTS, 'DEPENDENCY_EXISTS').
-define(MESSAGE_ID_SCHEMA_ERROR, 'MESSAGE_ID_SCHEMA_ERROR').
-define(INVALID_ID, 'INVALID_ID').

%% Resource Not Found
-define(NOT_FOUND, 'NOT_FOUND').
-define(CLIENTID_NOT_FOUND, 'CLIENTID_NOT_FOUND').
-define(CLIENT_NOT_FOUND, 'CLIENT_NOT_FOUND').
-define(MESSAGE_ID_NOT_FOUND, 'MESSAGE_ID_NOT_FOUND').
-define(RESOURCE_NOT_FOUND, 'RESOURCE_NOT_FOUND').
-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').
-define(USER_NOT_FOUND, 'USER_NOT_FOUND').

%% Internal error
-define(INTERNAL_ERROR, 'INTERNAL_ERROR').
-define(SERVICE_UNAVAILABLE, 'SERVICE_UNAVAILABLE').
-define(SOURCE_ERROR, 'SOURCE_ERROR').
-define(UPDATE_FAILED, 'UPDATE_FAILED').
-define(REST_FAILED, 'REST_FAILED').
-define(CLIENT_NOT_RESPONSE, 'CLIENT_NOT_RESPONSE').

%% All codes
-define(ERROR_CODES, [
    {'WRONG_USERNAME_OR_PWD', <<"Wrong username or pwd">>},
    {'WRONG_USERNAME_OR_PWD_OR_API_KEY_OR_API_SECRET', <<"Wrong username & pwd or key & secret">>},
    {'BAD_REQUEST', <<"Request parameters are not legal">>},
    {'NOT_MATCH', <<"Conditions are not matched">>},
    {'ALREADY_EXISTS', <<"Resource already existed">>},
    {'BAD_CONFIG_SCHEMA', <<"Configuration data is not legal">>},
    {'BAD_LISTENER_ID', <<"Bad listener ID">>},
    {'BAD_NODE_NAME', <<"Bad Node Name">>},
    {'BAD_RPC', <<"RPC Failed. Check the cluster status and the requested node status">>},
    {'BAD_TOPIC', <<"Topic syntax error, Topic needs to comply with the MQTT protocol standard">>},
    {'EXCEED_LIMIT', <<"Create resources that exceed the maximum limit or minimum limit">>},
    {'INVALID_PARAMETER', <<"Request parameters is not legal and exceeds the boundary value">>},
    {'CONFLICT', <<"Conflicting request resources">>},
    {'NO_DEFAULT_VALUE', <<"Request parameters do not use default values">>},
    {'DEPENDENCY_EXISTS', <<"Resource is dependent by another resource">>},
    {'MESSAGE_ID_SCHEMA_ERROR', <<"Message ID parsing error">>},
    {'INVALID_ID', <<"Bad ID schema">>},
    {'MESSAGE_ID_NOT_FOUND', <<"Message ID does not exist">>},
    {'NOT_FOUND', <<"Resource was not found or does not exist">>},
    {'CLIENTID_NOT_FOUND', <<"Client ID was not found or does not exist">>},
    {'CLIENT_NOT_FOUND', <<"Client was not found or does not exist(usually not a MQTT client)">>},
    {'RESOURCE_NOT_FOUND', <<"Resource not found">>},
    {'TOPIC_NOT_FOUND', <<"Topic not found">>},
    {'USER_NOT_FOUND', <<"User not found">>},
    {'INTERNAL_ERROR', <<"Server inter error">>},
    {'SERVICE_UNAVAILABLE', <<"Service unavailable">>},
    {'SOURCE_ERROR', <<"Source error">>},
    {'UPDATE_FAILED', <<"Update failed">>},
    {'REST_FAILED', <<"Reset source or config failed">>},
    {'CLIENT_NOT_RESPONSE', <<"Client not responding">>}
]).
