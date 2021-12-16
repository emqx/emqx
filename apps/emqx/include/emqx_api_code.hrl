
%% API Response Code
%% EMQ X API error response body like:
%%
%% {
%%      "code": ${ResponseCode},
%%      "message": ${Error Message}
%% }
%%
%% ${ResponseCode} all uppercase, separated by underscores.

%% -------------------------------------------------------------------------------------------------
%% api auth
-define(API_CODE_ERROR_USERNAME_OR_PWD,      'ERROR_USERNAME_OR_PWD').
-define(API_CODE_BAD_TOKEN,                  'BAD_TOKEN').
-define(API_CODE_CANNOT_DELETE_ADMIN,        'CANNOT_DELETE_ADMIN').

%% -------------------------------------------------------------------------------------------------
%% resource not found & bad resource id
-define(API_CODE_NOT_FOUND,                  'NOT_FOUND').

-define(API_CODE_CLIENT_NOT_FOUND,           'CLIENT_NOT_FOUND').
-define(API_CODE_USER_NOT_FOUND,             'USER_NOT_FOUND').
-define(API_CODE_MESSAGE_ID_NOT_FOUND,       'MESSAGE_ID_NOT_FOUND').
-define(API_CODE_TOPIC_NOT_FOUND,            'TOPIC_NOT_FOUND').

-define(API_CODE_BRIDGE_NOT_FOUND,            'BRIDGE_NOT_FOUND').

-define(API_CODE_MESSAGE_ID_SCHEMA_ERROR,    'MESSAGE_ID_SCHEMA_ERROR').
-define(API_CODE_ERROR_TOPIC,                'ERROR_TOPIC').
-define(API_CODE_BAD_TOPIC,                  'BAD_TOPIC').
-define(API_CODE_BAD_NODE_NAME,              'BAD_NODE_NAME').
-define(API_CODE_BAD_LISTENER_ID,            'BAD_LISTENER_ID').


%% -------------------------------------------------------------------------------------------------
%% connector error
-define(API_CODE_BAD_CONNECTOR_ID,           'BAD_CONNECTOR_ID').
-define(API_CODE_TEST_CONNECTOR_FAILED,      'TEST_CONNECTOR_FAILED').

%% -------------------------------------------------------------------------------------------------
%% client error
-define(API_CODE_CLIENT_NOT_RESPONSE,        'CLIENT_NOT_RESPONSE').

%% -------------------------------------------------------------------------------------------------
%% internal error & bad req
-define(API_CODE_INTERNAL_ERROR,             'INTERNAL_ERROR').
-define(API_CODE_BAD_REQUEST,                'BAD_REQUEST').
-define(API_CODE_INVALID_PARAMETER,          'INVALID_PARAMETER').
-define(API_CODE_NO_DEFAULT_VALUE,           'NO_DEFAULT_VALUE').

%% -------------------------------------------------------------------------------------------------
%% config about
-define(API_CODE_CONFLICT,                   'CONFLICT').
-define(API_CODE_SOURCE_ERROR,               'SOURCE_ERROR').
-define(API_CODE_RESOURCE_NOT_FOUND,         'RESOURCE_NOT_FOUND').
-define(API_CODE_ALREADY_EXISTED,            'ALREADY_EXISTED').
-define(API_CODE_BAD_CONFIG_SCHEMA,          'BAD_CONFIG_SCHEMA').
-define(API_CODE_UPDATE_FAILED,              'UPDATE_FAILED').
-define(API_CODE_REST_FAILED,                'REST_FAILED').
-define(API_CODE_EXCEED_LIMIT,               'EXCEED_LIMIT').
