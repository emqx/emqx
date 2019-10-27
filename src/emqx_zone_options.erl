%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_zone_options).

-compile(inline).

-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([ idle_timeout/1
        , publish_limit/1
        , mqtt_frame_options/1
        , mqtt_strict_mode/1
        , max_packet_size/1
        , mountpoint/1
        , use_username_as_clientid/1
        , enable_stats/1
        , enable_acl/1
        , enable_ban/1
        , enable_flapping_detect/1
        , ignore_loop_deliver/1
        , server_keepalive/1
        , keepalive_backoff/1
        , max_inflight/1
        , session_expiry_interval/1
        , force_gc_policy/1
        , force_shutdown_policy/1
        ]).

-import(emqx_zone, [get_env/2, get_env/3]).

-define(DEFAULT_IDLE_TIMEOUT, 30000).

-spec(idle_timeout(emqx_zone:zone()) -> pos_integer()).
idle_timeout(Zone) ->
    get_env(Zone, idle_timeout, ?DEFAULT_IDLE_TIMEOUT).

-spec(publish_limit(emqx_zone:zone()) -> maybe(esockd_rate_limit:bucket())).
publish_limit(Zone) ->
    get_env(Zone, publish_limit).

-spec(mqtt_frame_options(emqx_zone:zone()) -> emqx_frame:options()).
mqtt_frame_options(Zone) ->
    #{strict_mode => mqtt_strict_mode(Zone),
      max_size    => max_packet_size(Zone)
     }.

-spec(mqtt_strict_mode(emqx_zone:zone()) -> boolean()).
mqtt_strict_mode(Zone) ->
    get_env(Zone, mqtt_strict_mode, false).

-spec(max_packet_size(emqx_zone:zone()) -> integer()).
max_packet_size(Zone) ->
    get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE).

-spec(mountpoint(emqx_zone:zone()) -> maybe(emqx_mountpoint:mountpoint())).
mountpoint(Zone) -> get_env(Zone, mountpoint).

-spec(use_username_as_clientid(emqx_zone:zone()) -> boolean()).
use_username_as_clientid(Zone) ->
    get_env(Zone, use_username_as_clientid, false).

-spec(enable_stats(emqx_zone:zone()) -> boolean()).
enable_stats(Zone) ->
    get_env(Zone, enable_stats, true).

-spec(enable_acl(emqx_zone:zone()) -> boolean()).
enable_acl(Zone) ->
    get_env(Zone, enable_acl, true).

-spec(enable_ban(emqx_zone:zone()) -> boolean()).
enable_ban(Zone) ->
    get_env(Zone, enable_ban, false).

-spec(enable_flapping_detect(emqx_zone:zone()) -> boolean()).
enable_flapping_detect(Zone) ->
    get_env(Zone, enable_flapping_detect, false).

-spec(ignore_loop_deliver(emqx_zone:zone()) -> boolean()).
ignore_loop_deliver(Zone) ->
    get_env(Zone, ignore_loop_deliver, false).

-spec(server_keepalive(emqx_zone:zone()) -> pos_integer()).
server_keepalive(Zone) ->
    get_env(Zone, server_keepalive).

-spec(keepalive_backoff(emqx_zone:zone()) -> float()).
keepalive_backoff(Zone) ->
    get_env(Zone, keepalive_backoff, 0.75).

-spec(max_inflight(emqx_zone:zone()) -> 0..65535).
max_inflight(Zone) ->
    get_env(Zone, max_inflight, 65535).

-spec(session_expiry_interval(emqx_zone:zone()) -> non_neg_integer()).
session_expiry_interval(Zone) ->
    get_env(Zone, session_expiry_interval, 0).

-spec(force_gc_policy(emqx_zone:zone()) -> maybe(emqx_gc:opts())).
force_gc_policy(Zone) ->
    get_env(Zone, force_gc_policy).

-spec(force_shutdown_policy(emqx_zone:zone()) -> maybe(emqx_oom:opts())).
force_shutdown_policy(Zone) ->
    get_env(Zone, force_shutdown_policy).

