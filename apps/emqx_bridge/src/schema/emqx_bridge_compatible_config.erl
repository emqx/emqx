%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module was created to convert old version (from v5.0.0 to v5.0.11)
%% mqtt/webhook connector configs to newer version (developed for enterprise edition).
-module(emqx_bridge_compatible_config).

-export([
    upgrade_pre_ee/2,
    maybe_upgrade/1,
    http_maybe_upgrade/1
]).

upgrade_pre_ee(undefined, _UpgradeFunc) ->
    undefined;
upgrade_pre_ee(Conf0, UpgradeFunc) when is_map(Conf0) ->
    maps:from_list(upgrade_pre_ee(maps:to_list(Conf0), UpgradeFunc));
upgrade_pre_ee([], _UpgradeFunc) ->
    [];
upgrade_pre_ee([{Name, Config} | Bridges], UpgradeFunc) ->
    [{Name, UpgradeFunc(Config)} | upgrade_pre_ee(Bridges, UpgradeFunc)].

maybe_upgrade(#{<<"connector">> := _} = Config0) ->
    Config1 = up(Config0),
    Config = lists:map(fun binary_key/1, Config1),
    maps:from_list(Config);
maybe_upgrade(NewVersion) ->
    NewVersion.

http_maybe_upgrade(#{<<"direction">> := _} = Config0) ->
    Config1 = maps:remove(<<"direction">>, Config0),
    Config1#{<<"resource_opts">> => default_resource_opts()};
http_maybe_upgrade(NewVersion) ->
    NewVersion.

binary_key({K, V}) ->
    {atom_to_binary(K, utf8), V}.

up(#{<<"connector">> := Connector} = Config) ->
    Cn = fun(Key0, Default) ->
        Key = atom_to_binary(Key0, utf8),
        {Key0, maps:get(Key, Connector, Default)}
    end,
    Direction =
        case maps:get(<<"direction">>, Config) of
            <<"egress">> ->
                {egress, egress(Config)};
            <<"ingress">> ->
                {ingress, ingress(Config)}
        end,
    Enable = maps:get(<<"enable">>, Config, true),
    [
        Cn(bridge_mode, false),
        Cn(username, <<>>),
        Cn(password, <<>>),
        Cn(clean_start, true),
        Cn(keepalive, <<"60s">>),
        Cn(proto_ver, <<"v4">>),
        Cn(server, undefined),
        Cn(retry_interval, <<"15s">>),
        Cn(ssl, default_ssl()),
        {enable, Enable},
        {resource_opts, default_resource_opts()},
        Direction
    ].

default_ssl() ->
    #{
        <<"enable">> => false,
        <<"verify">> => <<"verify_peer">>
    }.

default_resource_opts() ->
    #{
        <<"inflight_window">> => 100,
        <<"health_check_interval">> => <<"15s">>,
        <<"max_buffer_bytes">> => <<"1GB">>,
        <<"query_mode">> => <<"sync">>,
        %% there is only one underlying MQTT connection
        %% doesn't make a lot of sense to have a large pool
        <<"worker_pool_size">> => 4
    }.

egress(Config) ->
    % <<"local">> % the old version has no 'local' config for egress
    #{
        <<"remote">> =>
            #{
                <<"topic">> => maps:get(<<"remote_topic">>, Config),
                <<"qos">> => maps:get(<<"remote_qos">>, Config),
                <<"retain">> => maps:get(<<"retain">>, Config),
                <<"payload">> => maps:get(<<"payload">>, Config)
            }
    }.

ingress(Config) ->
    #{
        <<"remote">> =>
            #{
                <<"qos">> => maps:get(<<"remote_qos">>, Config),
                <<"topic">> => maps:get(<<"remote_topic">>, Config)
            },
        <<"local">> =>
            #{
                <<"payload">> => maps:get(<<"payload">>, Config),
                <<"qos">> => maps:get(<<"local_qos">>, Config),
                <<"retain">> => maps:get(<<"retain">>, Config, false)
                %% <<"topic">> % th old version has no local topic for ingress
            }
    }.
