%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_test_lib).

-export([
    bridge_type/0,
    bridge_name/0,
    make_bridge/1,
    bridge_async_config/1,
    init_http_success_server/1,
    success_http_handler/0
]).

-define(BRIDGE_TYPE, bridge_type()).
-define(BRIDGE_NAME, bridge_name()).

bridge_type() ->
    <<"webhook">>.

bridge_name() ->
    atom_to_binary(?MODULE).

make_bridge(Config) ->
    Type = ?BRIDGE_TYPE,
    Name = ?BRIDGE_NAME,
    BridgeConfig = bridge_async_config(Config#{
        name => Name,
        type => Type
    }),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        BridgeConfig
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

bridge_async_config(#{port := Port} = Config) ->
    Type = maps:get(type, Config, ?BRIDGE_TYPE),
    Name = maps:get(name, Config, ?BRIDGE_NAME),
    Host = maps:get(host, Config, "localhost"),
    Path = maps:get(path, Config, ""),
    PoolSize = maps:get(pool_size, Config, 1),
    QueryMode = maps:get(query_mode, Config, "async"),
    ConnectTimeout = maps:get(connect_timeout, Config, "1s"),
    RequestTimeout = maps:get(request_timeout, Config, "10s"),
    ResumeInterval = maps:get(resume_interval, Config, "1s"),
    HealthCheckInterval = maps:get(health_check_interval, Config, "200ms"),
    ResourceRequestTTL = maps:get(resource_request_ttl, Config, "infinity"),
    LocalTopic =
        case maps:find(local_topic, Config) of
            {ok, LT} ->
                lists:flatten(["local_topic = \"", LT, "\""]);
            error ->
                ""
        end,
    ConfigString = io_lib:format(
        "bridges.~s.~s {\n"
        "  url = \"http://~s:~p~s\"\n"
        "  connect_timeout = \"~p\"\n"
        "  enable = true\n"
        %% local_topic
        "  ~s\n"
        "  enable_pipelining = 100\n"
        "  max_retries = 2\n"
        "  method = \"post\"\n"
        "  pool_size = ~p\n"
        "  pool_type = \"random\"\n"
        "  request_timeout = \"~s\"\n"
        "  body = \"${id}\"\n"
        "  resource_opts {\n"
        "    inflight_window = 100\n"
        "    health_check_interval = \"~s\"\n"
        "    max_buffer_bytes = \"1GB\"\n"
        "    query_mode = \"~s\"\n"
        "    request_ttl = \"~p\"\n"
        "    resume_interval = \"~s\"\n"
        "    start_after_created = \"true\"\n"
        "    start_timeout = \"5s\"\n"
        "    worker_pool_size = \"1\"\n"
        "  }\n"
        "  ssl {\n"
        "    enable = false\n"
        "  }\n"
        "}\n",
        [
            Type,
            Name,
            Host,
            Port,
            Path,
            ConnectTimeout,
            LocalTopic,
            PoolSize,
            RequestTimeout,
            HealthCheckInterval,
            QueryMode,
            ResourceRequestTTL,
            ResumeInterval
        ]
    ),
    ct:pal(ConfigString),
    parse_and_check(ConfigString, Type, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

success_http_handler() ->
    success_http_handler(#{response_delay => 0}).

success_http_handler(Opts) ->
    ResponseDelay = maps:get(response_delay, Opts, 0),
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        Headers = cowboy_req:headers(Req),
        ct:pal("http request received: ~p", [
            #{body => Body, headers => Headers, response_delay => ResponseDelay}
        ]),
        ResponseDelay > 0 andalso timer:sleep(ResponseDelay),
        TestPid ! {http, Headers, Body},
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"text/plain">>},
            <<"hello">>,
            Req
        ),
        {ok, Rep, State}
    end.

init_http_success_server(Config) ->
    HTTPPath = <<"/path">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ResponseDelayMS = 500,
    ok = emqx_bridge_http_connector_test_server:set_handler(
        success_http_handler(#{response_delay => ResponseDelayMS})
    ),
    [
        {http_server, #{port => HTTPPort, path => HTTPPath}},
        {response_delay_ms, ResponseDelayMS},
        {bridge_name, ?BRIDGE_NAME}
        | Config
    ].
