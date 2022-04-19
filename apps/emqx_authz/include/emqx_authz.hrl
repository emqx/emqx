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

-include_lib("emqx/include/emqx_access_control.hrl").

-define(APP, emqx_authz).

-define(ALLOW_DENY(A),
    ((A =:= allow) orelse (A =:= <<"allow">>) orelse
        (A =:= deny) orelse (A =:= <<"deny">>))
).
-define(PUBSUB(A),
    ((A =:= subscribe) orelse (A =:= <<"subscribe">>) orelse
        (A =:= publish) orelse (A =:= <<"publish">>) orelse
        (A =:= all) orelse (A =:= <<"all">>))
).

%% authz_mnesia
-define(ACL_TABLE, emqx_acl).

%% authz_cmd
-define(CMD_REPLACE, replace).
-define(CMD_DELETE, delete).
-define(CMD_PREPEND, prepend).
-define(CMD_APPEND, append).
-define(CMD_MOVE, move).

-define(CMD_MOVE_FRONT, front).
-define(CMD_MOVE_REAR, rear).
-define(CMD_MOVE_BEFORE(Before), {before, Before}).
-define(CMD_MOVE_AFTER(After), {'after', After}).

-define(CONF_KEY_PATH, [authorization, sources]).

-define(RE_PLACEHOLDER, "\\$\\{[a-z0-9_]+\\}").

%% has to be the same as the root field name defined in emqx_schema
-define(CONF_NS, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME).
-define(CONF_NS_ATOM, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_ATOM).
-define(CONF_NS_BINARY, ?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_BINARY).

%% API examples
-define(USERNAME_RULES_EXAMPLE, #{
    username => user1,
    rules => [
        #{
            topic => <<"test/toopic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/toopic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        }
    ]
}).
-define(CLIENTID_RULES_EXAMPLE, #{
    clientid => client1,
    rules => [
        #{
            topic => <<"test/toopic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/toopic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        }
    ]
}).
-define(ALL_RULES_EXAMPLE, #{
    rules => [
        #{
            topic => <<"test/toopic/1">>,
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topic => <<"test/toopic/2">>,
            permission => <<"allow">>,
            action => <<"subscribe">>
        },
        #{
            topic => <<"eq test/#">>,
            permission => <<"deny">>,
            action => <<"all">>
        }
    ]
}).
-define(META_EXAMPLE, #{
    page => 1,
    limit => 100,
    count => 1
}).

-define(RESOURCE_GROUP, <<"emqx_authz">>).
