%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-include_lib("emqx/include/emqx_access_control.hrl").

-include("emqx_auth.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

%% authz_mnesia
-define(ACL_TABLE, emqx_acl).

%% authz_cmd
-define(CMD_REPLACE, replace).
-define(CMD_DELETE, delete).
-define(CMD_PREPEND, prepend).
-define(CMD_APPEND, append).
-define(CMD_MOVE, move).
-define(CMD_MERGE, merge).
-define(CMD_REORDER, reorder).

-define(CMD_MOVE_FRONT, front).
-define(CMD_MOVE_REAR, rear).
-define(CMD_MOVE_BEFORE(Before), {before, Before}).
-define(CMD_MOVE_AFTER(After), {'after', After}).

-define(ROOT_KEY, [authorization]).
-define(CONF_KEY_PATH, [authorization, sources]).

%% has to be the same as the root field name defined in emqx_schema
-define(CONF_NS, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME).
-define(CONF_NS_ATOM, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_ATOM).
-define(CONF_NS_BINARY, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_BINARY).

%% API examples
-define(USERNAME_RULES_EXAMPLE, #{
    username => user1,
    rules => [
        #{
            topic => <<"test/topic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/topic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        },
        #{
            topic => <<"test/topic/3">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"1">>],
            retain => <<"true">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>,
            username_re => <<"^u+$">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>,
            clientid_re => <<"^c+$">>,
            ipaddr => <<"192.168.1.0/24">>
        }
    ]
}).

-define(CLIENTID_RULES_EXAMPLE, #{
    clientid => client1,
    rules => [
        #{
            topic => <<"test/topic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/topic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        },
        #{
            topic => <<"test/topic/3">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"1">>],
            retain => <<"true">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>,
            username_re => <<"^u+$">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>,
            clientid_re => <<"^c+$">>,
            ipaddr => <<"192.168.1.0/24">>
        }
    ]
}).

-define(ALL_RULES_EXAMPLE, #{
    rules => [
        #{
            topic => <<"test/topic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/topic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        },
        #{
            topic => <<"test/topic/3">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"1">>],
            retain => <<"true">>
        },
        #{
            topic => <<"test/topic/4">>,
            permission => <<"allow">>,
            action => <<"publish">>,
            qos => [<<"0">>, <<"1">>, <<"2">>],
            retain => <<"all">>
        }
    ]
}).

-define(USERNAME_RULES_EXAMPLE_COUNT, length(maps:get(rules, ?USERNAME_RULES_EXAMPLE))).
-define(CLIENTID_RULES_EXAMPLE_COUNT, length(maps:get(rules, ?CLIENTID_RULES_EXAMPLE))).
-define(ALL_RULES_EXAMPLE_COUNT, length(maps:get(rules, ?ALL_RULES_EXAMPLE))).

-define(META_EXAMPLE, #{
    page => 1,
    limit => 100,
    count => 1
}).

-define(AUTHZ_RESOURCE_GROUP, <<"authz">>).

-define(AUTHZ_FEATURES, [rich_actions]).

-define(DEFAULT_RULE_QOS, [0, 1, 2]).
-define(DEFAULT_RULE_RETAIN, all).

-define(BUILTIN_SOURCES, [
    {client_info, emqx_authz_client_info},
    {file, emqx_authz_file}
]).

-define(AUTHZ_DEFAULT_ALLOWED_VARS, [
    ?VAR_CERT_CN_NAME,
    ?VAR_CERT_SUBJECT,
    ?VAR_PEERHOST,
    ?VAR_PEERPORT,
    ?VAR_CLIENTID,
    ?VAR_USERNAME,
    ?VAR_ZONE,
    ?VAR_NS_CLIENT_ATTRS,
    ?VAR_LISTENER
]).

-define(AUTHZ_CACHE, emqx_authz_cache).

-define(ALL_TOPICS, 'ALL_TOPICS').
