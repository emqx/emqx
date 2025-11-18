%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_DASHBOARD_HTTP_API_HRL).
-define(EMQX_DASHBOARD_HTTP_API_HRL, true).

%% HTTP API Auth
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BAD_API_KEY_OR_SECRET, 'BAD_API_KEY_OR_SECRET').
-define(API_KEY_NOT_ALLOW, 'API_KEY_NOT_ALLOW').
-define(API_KEY_NOT_ALLOW_MSG, <<"This API Key don't have permission to access this resource">>).

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
-define(FORBIDDEN, 'FORBIDDEN').
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
    {'ALREADY_EXISTS', <<"Resource already existed">>},
    {'BAD_CONFIG_SCHEMA', <<"Configuration data is invalid">>},
    {'BAD_LISTENER_ID', <<"Bad listener ID">>},
    {'BAD_NODE_NAME', <<"Bad Node Name">>},
    {'BAD_REQUEST', <<"Request parameters are invalid">>},
    {'BAD_RPC', <<"RPC Failed. Check the cluster status and the requested node status">>},
    {'BAD_TOPIC', <<"Topic syntax error, Topic needs to comply with the MQTT protocol standard">>},
    {'CLIENTID_NOT_FOUND', <<"Client ID was not found or does not exist">>},
    {'CLIENT_NOT_FOUND', <<"Client was not found or does not exist(usually not a MQTT client)">>},
    {'CLIENT_NOT_RESPONSE', <<"Client not responding">>},
    {'CONFLICT', <<"Conflicting request resources">>},
    {'DEPENDENCY_EXISTS', <<"Resource is dependent by another resource">>},
    {'EXCEED_LIMIT', <<"Create resources that exceed the maximum limit or minimum limit">>},
    {'INTERNAL_ERROR', <<"Server inter error">>},
    {'INVALID_ID', <<"Bad ID schema">>},
    {'INVALID_PARAMETER', <<"Request parameters is invalid and exceeds the boundary value">>},
    {'MESSAGE_ID_NOT_FOUND', <<"Message ID does not exist">>},
    {'MESSAGE_ID_SCHEMA_ERROR', <<"Message ID parsing error">>},
    {'NOT_FOUND', <<"Resource was not found or does not exist">>},
    {'NOT_MATCH', <<"Conditions are not matched">>},
    {'NO_DEFAULT_VALUE', <<"Request parameters do not use default values">>},
    {'RESOURCE_NOT_FOUND', <<"Resource not found">>},
    {'REST_FAILED', <<"Reset source or config failed">>},
    {'SERVICE_UNAVAILABLE', <<"Service unavailable">>},
    {'SOURCE_ERROR', <<"Source error">>},
    {'TOPIC_NOT_FOUND', <<"Topic not found">>},
    {'UNSUPPORTED_MEDIA_TYPE', <<"Unsupported media type">>},
    {'UPDATE_FAILED', <<"Update failed">>},
    {'USER_NOT_FOUND', <<"User not found">>},
    {?BAD_API_KEY_OR_SECRET, <<"Bad API key or secret">>},
    {?BAD_USERNAME_OR_PWD, <<"Bad username or password">>},
    {?FORBIDDEN, <<"Operation not allowed for current user">>}
]).

-define(ERROR_MSG(CODE, REASON), #{code => CODE, message => emqx_utils:readable_error_msg(REASON)}).

-define(OK(CONTENT), {200, CONTENT}).

-define(CREATED(CONTENT), {201, CONTENT}).

-define(ACCEPTED, 202).

-define(NO_CONTENT, 204).

-define(BAD_REQUEST(CODE, REASON), {400, ?ERROR_MSG(CODE, REASON)}).
-define(BAD_REQUEST(REASON), ?BAD_REQUEST('BAD_REQUEST', REASON)).

-define(BAD_REQUEST_MAP(REASON, EXTRA_MAP),
    {400, maps:merge(?ERROR_MSG('BAD_REQUEST', REASON), EXTRA_MAP)}
).

-define(FORBIDDEN(REASON), {403, ?ERROR_MSG(?FORBIDDEN, REASON)}).

-define(NOT_FOUND(REASON), {404, ?ERROR_MSG('NOT_FOUND', REASON)}).

-define(METHOD_NOT_ALLOWED, 405).

-define(CONFLICT(CODE, REASON), {409, ?ERROR_MSG(CODE, REASON)}).
-define(CONFLICT(REASON), {409, ?ERROR_MSG('CONFLICT', REASON)}).

-define(INTERNAL_ERROR(CODE, REASON), {500, ?ERROR_MSG(CODE, REASON)}).
-define(INTERNAL_ERROR(REASON), {500, ?ERROR_MSG('INTERNAL_ERROR', REASON)}).

-define(NOT_IMPLEMENTED, 501).

-define(SERVICE_UNAVAILABLE(REASON), {503, ?ERROR_MSG('SERVICE_UNAVAILABLE', REASON)}).

-endif.
