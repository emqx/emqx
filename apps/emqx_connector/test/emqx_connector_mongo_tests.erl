%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_mongo_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_MONGO_PORT, 27017).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

to_servers_raw_test_() ->
    [
        {"single server, binary, no port",
            ?_test(
                ?assertEqual(
                    [{"localhost", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw(<<"localhost">>)
                )
            )},
        {"single server, string, no port",
            ?_test(
                ?assertEqual(
                    [{"localhost", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw("localhost")
                )
            )},
        {"single server, list(binary), no port",
            ?_test(
                ?assertEqual(
                    [{"localhost", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw([<<"localhost">>])
                )
            )},
        {"single server, list(string), no port",
            ?_test(
                ?assertEqual(
                    [{"localhost", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw(["localhost"])
                )
            )},
        %%%%%%%%%
        {"single server, binary, with port",
            ?_test(
                ?assertEqual(
                    [{"localhost", 9999}], emqx_connector_mongo:to_servers_raw(<<"localhost:9999">>)
                )
            )},
        {"single server, string, with port",
            ?_test(
                ?assertEqual(
                    [{"localhost", 9999}], emqx_connector_mongo:to_servers_raw("localhost:9999")
                )
            )},
        {"single server, list(binary), with port",
            ?_test(
                ?assertEqual(
                    [{"localhost", 9999}],
                    emqx_connector_mongo:to_servers_raw([<<"localhost:9999">>])
                )
            )},
        {"single server, list(string), with port",
            ?_test(
                ?assertEqual(
                    [{"localhost", 9999}], emqx_connector_mongo:to_servers_raw(["localhost:9999"])
                )
            )},
        %%%%%%%%%
        {"multiple servers, string, no port",
            ?_test(
                ?assertEqual(
                    [{"host1", ?DEFAULT_MONGO_PORT}, {"host2", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw("host1, host2")
                )
            )},
        {"multiple servers, binary, no port",
            ?_test(
                ?assertEqual(
                    [{"host1", ?DEFAULT_MONGO_PORT}, {"host2", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw(<<"host1, host2">>)
                )
            )},
        {"multiple servers, list(string), no port",
            ?_test(
                ?assertEqual(
                    [{"host1", ?DEFAULT_MONGO_PORT}, {"host2", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw(["host1", "host2"])
                )
            )},
        {"multiple servers, list(binary), no port",
            ?_test(
                ?assertEqual(
                    [{"host1", ?DEFAULT_MONGO_PORT}, {"host2", ?DEFAULT_MONGO_PORT}],
                    emqx_connector_mongo:to_servers_raw([<<"host1">>, <<"host2">>])
                )
            )},
        %%%%%%%%%
        {"multiple servers, string, with port",
            ?_test(
                ?assertEqual(
                    [{"host1", 1234}, {"host2", 2345}],
                    emqx_connector_mongo:to_servers_raw("host1:1234, host2:2345")
                )
            )},
        {"multiple servers, binary, with port",
            ?_test(
                ?assertEqual(
                    [{"host1", 1234}, {"host2", 2345}],
                    emqx_connector_mongo:to_servers_raw(<<"host1:1234, host2:2345">>)
                )
            )},
        {"multiple servers, list(string), with port",
            ?_test(
                ?assertEqual(
                    [{"host1", 1234}, {"host2", 2345}],
                    emqx_connector_mongo:to_servers_raw(["host1:1234", "host2:2345"])
                )
            )},
        {"multiple servers, list(binary), with port",
            ?_test(
                ?assertEqual(
                    [{"host1", 1234}, {"host2", 2345}],
                    emqx_connector_mongo:to_servers_raw([<<"host1:1234">>, <<"host2:2345">>])
                )
            )},
        %%%%%%%%
        {"multiple servers, invalid list(string)",
            ?_test(
                ?assertThrow(
                    _,
                    emqx_connector_mongo:to_servers_raw(["host1, host2"])
                )
            )},
        {"multiple servers, invalid list(binary)",
            ?_test(
                ?assertThrow(
                    _,
                    emqx_connector_mongo:to_servers_raw([<<"host1, host2">>])
                )
            )},
        %% TODO: handle this case??
        {"multiple servers, mixed list(binary|string)",
            ?_test(
                ?assertThrow(
                    _,
                    emqx_connector_mongo:to_servers_raw([<<"host1">>, "host2"])
                )
            )}
    ].
