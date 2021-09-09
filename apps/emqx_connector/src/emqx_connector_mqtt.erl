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
    [ {server, hoconsc:mk(emqx_schema:ip_port(), #{default => "127.0.0.1:1883"})}
    , {reconnect_interval, hoconsc:mk(emqx_schema:duration_ms(), #{default => "30s"})}
    , {proto_ver, fun proto_ver/1}
    , {bridge_mode, hoconsc:mk(boolean(), #{default => true})}
    , {clientid_prefix, hoconsc:mk(string(), #{default => ""})}
    , {username, hoconsc:mk(string())}
    , {password, hoconsc:mk(string())}
    , {clean_start, hoconsc:mk(boolean(), #{default => true})}
    , {keepalive, hoconsc:mk(integer(), #{default => 300})}
    , {retry_interval, hoconsc:mk(emqx_schema:duration_ms(), #{default => "30s"})}
    , {max_inflight, hoconsc:mk(integer(), #{default => 32})}
    , {replayq, hoconsc:mk(hoconsc:ref(?MODULE, "replayq"))}
    , {in, hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, "in")), #{default => []})}
    , {out, hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, "out")), #{default => []})}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("in") ->
    [ {subscribe_remote_topic, #{type => binary(), nullable => false}}
    , {local_topic, hoconsc:mk(binary(), #{default => <<"${topic}">>})}
    , {subscribe_qos, hoconsc:mk(qos(), #{default => 1})}
    ] ++ common_inout_confs();

fields("out") ->
    [ {subscribe_local_topic, #{type => binary(), nullable => false}}
    , {remote_topic, hoconsc:mk(binary(), #{default => <<"${topic}">>})}
    ] ++ common_inout_confs();

fields("replayq") ->
    [ {dir, hoconsc:union([boolean(), string()])}
    , {seg_bytes, hoconsc:mk(emqx_schema:bytesize(), #{default => "100MB"})}
    , {offload, hoconsc:mk(boolean(), #{default => false})}
    , {max_total_bytes, hoconsc:mk(emqx_schema:bytesize(), #{default => "1024MB"})}
    ].

common_inout_confs() ->
    [{id, #{type => binary(), nullable => false}}] ++ publish_confs().

publish_confs() ->
    [ {qos, hoconsc:mk(qos(), #{default => <<"${qos}">>})}
    , {retain, hoconsc:mk(hoconsc:union([boolean(), binary()]), #{default => <<"${retain}">>})}
    , {payload, hoconsc:mk(binary(), #{default => <<"${payload}">>})}
    ].

qos() ->
    hoconsc:union([typerefl:integer(0), typerefl:integer(1), typerefl:integer(2), binary()]).

proto_ver(type) -> hoconsc:enum([v3, v4, v5]);
proto_ver(default) -> v4;
proto_ver(_) -> undefined.

%% ===================================================================
on_start(InstId, Conf) ->
    logger:info("starting mqtt connector: ~p, ~p", [InstId, Conf]),
    NamePrefix = binary_to_list(InstId),
    BasicConf = basic_config(Conf),
    InitRes = {ok, #{name_prefix => NamePrefix, baisc_conf => BasicConf, sub_bridges => []}},
    InOutConfigs = check_channel_id_dup(maps:get(in, Conf, []) ++ maps:get(out, Conf, [])),
    lists:foldl(fun
            (_InOutConf, {error, Reason}) ->
                {error, Reason};
            (InOutConf, {ok, #{sub_bridges := SubBridges} = Res}) ->
                case create_channel(InOutConf, NamePrefix, BasicConf) of
                    {error, Reason} -> {error, Reason};
                    {ok, Name} -> {ok, Res#{sub_bridges => [Name | SubBridges]}}
                end
        end, InitRes, InOutConfigs).

on_stop(InstId, #{}) ->
    logger:info("stopping mqtt connector: ~p", [InstId]),
    case emqx_bridge_mqtt_sup:drop_bridge(InstId) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} ->
            logger:error("stop bridge failed, error: ~p", [Reason])
    end.

%% TODO: let the emqx_resource trigger on_query/4 automatically according to the
%%  `in` and `out` config
on_query(InstId, {create_channel, Conf}, _AfterQuery, #{name_prefix := Prefix,
        baisc_conf := BasicConf}) ->
    logger:debug("create channel to connector: ~p, conf: ~p", [InstId, Conf]),
    create_channel(Conf, Prefix, BasicConf);
on_query(InstId, {publish_to_local, Msg}, _AfterQuery, _State) ->
    logger:debug("publish to local node, connector: ~p, msg: ~p", [InstId, Msg]);
on_query(InstId, {publish_to_remote, Msg}, _AfterQuery, _State) ->
    logger:debug("publish to remote node, connector: ~p, msg: ~p", [InstId, Msg]).

on_health_check(_InstId, #{sub_bridges := NameList} = State) ->
    Results = [{Name, emqx_bridge_worker:ping(Name)} || Name <- NameList],
    case lists:all(fun({_, pong}) -> true; ({_, _}) -> false end, Results) of
        true -> {ok, State};
        false -> {error, {some_sub_bridge_down, Results}, State}
    end.

check_channel_id_dup(Confs) ->
    lists:foreach(fun(#{id := Id}) ->
            case length([Id || #{id := Id0} <- Confs, Id0 == Id]) of
                1 -> ok;
                L when L > 1 -> error({mqtt_bridge_conf, {duplicate_id_found, Id}})
            end
        end, Confs),
    Confs.

%% this is an `in` bridge
create_channel(#{subscribe_remote_topic := _, id := BridgeId} = InConf, NamePrefix,
        #{clientid_prefix := ClientPrefix} = BasicConf) ->
    logger:info("creating 'in' channel for: ~p", [BridgeId]),
    create_sub_bridge(BasicConf#{name => bridge_name(NamePrefix, BridgeId),
        clientid => clientid(ClientPrefix, BridgeId),
        subscriptions => InConf, forwards => undefined});
%% this is an `out` bridge
create_channel(#{subscribe_local_topic := _, id := BridgeId} = OutConf, NamePrefix,
        #{clientid_prefix := ClientPrefix} = BasicConf) ->
    logger:info("creating 'out' channel for: ~p", [BridgeId]),
    create_sub_bridge(BasicConf#{name => bridge_name(NamePrefix, BridgeId),
        clientid => clientid(ClientPrefix, BridgeId),
        subscriptions => undefined, forwards => OutConf}).

create_sub_bridge(#{name := Name} = Conf) ->
    case emqx_bridge_mqtt_sup:create_bridge(Conf) of
        {ok, _Pid} ->
            start_sub_bridge(Name);
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

start_sub_bridge(Name) ->
    case emqx_bridge_worker:ensure_started(Name) of
        ok -> {ok, Name};
        {error, Reason} -> {error, Reason}
    end.

basic_config(#{
        server := Server,
        reconnect_interval := ReconnIntv,
        proto_ver := ProtoVer,
        bridge_mode := BridgeMod,
        clientid_prefix := ClientIdPrefix,
        username := User,
        password := Password,
        clean_start := CleanStart,
        keepalive := KeepAlive,
        retry_interval := RetryIntv,
        max_inflight := MaxInflight,
        replayq := ReplayQ,
        ssl := #{enable := EnableSsl} = Ssl}) ->
    #{
        conn_type => mqtt,
        replayq => ReplayQ,
        %% connection opts
        server => Server,
        reconnect_interval => ReconnIntv,
        proto_ver => ProtoVer,
        bridge_mode => BridgeMod,
        clientid_prefix => ClientIdPrefix,
        username => User,
        password => Password,
        clean_start => CleanStart,
        keepalive => KeepAlive,
        retry_interval => RetryIntv,
        max_inflight => MaxInflight,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl)),
        if_record_metrics => true
    }.

bridge_name(Prefix, Id) ->
    list_to_atom(str(Prefix) ++ ":" ++ str(Id)).

clientid(Prefix, Id) ->
    list_to_binary(str(Prefix) ++ str(Id)).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
