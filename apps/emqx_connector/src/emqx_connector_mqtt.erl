%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_mqtt).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-behaviour(hocon_schema).

-export([ roots/0
        , fields/1]).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, "config")}}].

fields("config") ->
    [ {server, emqx_schema:t(emqx_schema:ip_port(), undefined, "127.0.0.1:1883")}
    , {reconnect_interval, emqx_schema:t(emqx_schema:duration_ms(), undefined, "30s")}
    , {proto_ver, fun proto_ver/1}
    , {bridge_mode, emqx_schema:t(boolean(), undefined, true)}
    , {clientid, emqx_schema:t(string())}
    , {username, emqx_schema:t(string())}
    , {password, emqx_schema:t(string())}
    , {clean_start, emqx_schema:t(boolean(), undefined, true)}
    , {keepalive, emqx_schema:t(integer(), undefined, 300)}
    , {retry_interval, emqx_schema:t(emqx_schema:duration_ms(), undefined, "30s")}
    , {max_inflight, emqx_schema:t(integer(), undefined, 32)}
    , {replayq, emqx_schema:t(hoconsc:ref(?MODULE, "replayq"))}
    , {in, hoconsc:array(hoconsc:ref(?MODULE, "in"))}
    , {out, hoconsc:array(hoconsc:ref(?MODULE, "out"))}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("in") ->
    [ {from_remote_topic, #{type => binary(), nullable => false}}
    , {to_local_topic, #{type => binary(), nullable => false}}
    , {qos, emqx_schema:t(integer(), undefined, 1)}
    , {payload_template, emqx_schema:t(binary(), undefined, <<"${message}">>)}
    ];

fields("out") ->
    [ {to_remote_topic, #{type => binary(), nullable => false}}
    , {from_local_topic, #{type => binary(), nullable => false}}
    , {payload_template, emqx_schema:t(binary(), undefined, <<"${payload}">>)}
    ];

fields("replayq") ->
    [ {dir, hoconsc:union([boolean(), string()])}
    , {seg_bytes, emqx_schema:t(emqx_schema:bytesize(), undefined, "100MB")}
    , {offload, emqx_schema:t(boolean(), undefined, false)}
    , {max_total_bytes, emqx_schema:t(emqx_schema:bytesize(), undefined, "1024MB")}
    ].

proto_ver(type) -> hoconsc:enum([v3, v4, v5]);
proto_ver(default) -> v4;
proto_ver(_) -> undefined.

%% ===================================================================
on_start(InstId, #{server := Server,
                   reconnect_interval := ReconnIntv,
                   proto_ver := ProtoVer,
                   bridge_mode := BridgeMod,
                   clientid := ClientID,
                   username := User,
                   password := Password,
                   clean_start := CleanStart,
                   keepalive := KeepAlive,
                   retry_interval := RetryIntv,
                   max_inflight := MaxInflight,
                   replayq := ReplayQ,
                   in := In,
                   out := Out,
                   ssl := #{enable := EnableSsl} = Ssl} = Conf) ->
    logger:info("starting mqtt connector: ~p, ~p", [InstId, Conf]),
    BridgeName = binary_to_atom(InstId, latin1),
    BridgeConf = Conf#{
        name => BridgeName,
        config => #{
            conn_type => mqtt,
            subscriptions => In,
            forwards => Out,
            replayq => ReplayQ,
            %% connection opts
            server => Server,
            reconnect_interval => ReconnIntv,
            proto_ver => ProtoVer,
            bridge_mode => BridgeMod,
            clientid => ClientID,
            username => User,
            password => Password,
            clean_start => CleanStart,
            keepalive => KeepAlive,
            retry_interval => RetryIntv,
            max_inflight => MaxInflight,
            ssl => EnableSsl,
            ssl_opts => maps:to_list(maps:remove(enable, Ssl)),
            if_record_metrics => true
        }
    },
    case emqx_bridge_mqtt_sup:create_bridge(BridgeConf) of
        {ok, _Pid} ->
            start_bridge(BridgeName);
        {error, {already_started, _Pid}} ->
            start_bridge(BridgeName);
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(InstId, #{}) ->
    logger:info("stopping mqtt connector: ~p", [InstId]),
    emqx_bridge_mqtt_sup:drop_bridge(InstId).

%% TODO: let the emqx_resource trigger on_query/4 automatically according to the
%%  `in` and `out` config
on_query(InstId, {publish_to_local, Msg}, _AfterQuery, _State) ->
    logger:debug("publish to local node, connector: ~p, msg: ~p", [InstId, Msg]);
on_query(InstId, {publish_to_remote, Msg}, _AfterQuery, _State) ->
    logger:debug("publish to remote node, connector: ~p, msg: ~p", [InstId, Msg]).

on_health_check(_InstId, #{bridge_worker := Worker}) ->
    {ok, emqx_bridge_worker:ping(Worker)}.

start_bridge(Name) ->
    case emqx_bridge_worker:ensure_started(Name) of
        ok -> {ok, #{bridge_name => Name}};
        {error, Reason} -> {error, Reason}
    end.
