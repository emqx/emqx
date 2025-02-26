%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Facade module for the rate-limiting functionality.

-module(emqx_limiter).

-include("logger.hrl").

-export([
    init/0
]).

%% Zone limiter management
-export([
    create_zone_limiters/0,
    update_zone_limiters/0,
    delete_zone_limiters/0,
    create_zone_limiters/1,
    update_zone_limiters/1,
    delete_zone_limiters/1
]).

%% Listener limiter management
-export([
    create_listener_limiters/2,
    update_listener_limiters/2,
    delete_listener_limiters/1
]).

%% API for limiter clients (channel, esockd)
-export([
    create_esockd_limiter_client/2,
    create_channel_client_container/2
]).

%% Generic limiter client API
-export([
    connect/1,
    create_group/3,
    update_group/2,
    delete_group/1
]).

%% Config Listener
-export([
    add_handler/0,
    remove_handler/0,
    post_config_update/5,
    propagated_post_config_update/5
]).

%%  Config helpers
-export([
    config/2,
    config_unlimited/0,
    config_from_rps/1,
    config_from_rate/1
]).

-export_type([zone/0, group/0, name/0, id/0, options/0]).

-type zone() :: group().
-type group() :: term().
-type name() :: atom().
-type id() :: {group(), name()}.
-type listener_id() :: term().

%% Limiter types
-type options() :: unlimited() | limited() | limited_with_burst().
-type unlimited() :: #{
    capacity := infinity
}.
-type limited() :: #{
    capacity := pos_integer(),
    interval := pos_integer(),
    burst_capacity := 0
}.
-type limited_with_burst() :: #{
    capacity := pos_integer(),
    burst_capacity := pos_integer(),
    interval := pos_integer(),
    burst_interval := pos_integer()
}.

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

-callback create_group(group(), [{name(), options()}]) -> ok.
-callback update_group(group(), [{name(), options()}]) -> ok.
-callback delete_group(group()) -> ok.
-callback connect(id()) -> emqx_limiter_client:t().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% NOTE
%% Deinit happens as a part supervision tree shutdown
-spec init() -> ok.
init() ->
    create_zone_limiters().

-spec create_zone_limiters() -> ok.
create_zone_limiters() ->
    lists:foreach(
        fun(Zone) ->
            create_zone_limiters(Zone)
        end,
        maps:keys(emqx_config:get([zones]))
    ).

-spec update_zone_limiters() -> ok.
update_zone_limiters() ->
    ?SLOG(debug, #{
        msg => "update_zone_limiters",
        zones => maps:keys(emqx_config:get([zones]))
    }),
    lists:foreach(
        fun(Zone) ->
            update_zone_limiters(Zone)
        end,
        maps:keys(emqx_config:get([zones]))
    ).

-spec delete_zone_limiters() -> ok.
delete_zone_limiters() ->
    lists:foreach(
        fun(Zone) ->
            delete_zone_limiters(Zone)
        end,
        maps:keys(emqx_config:get([zones]))
    ).

-spec create_listener_limiters(listener_id(), term()) -> ok.
create_listener_limiters(ListenerId, ListenerConfig) ->
    ListenerLimiters = config_limiters(ListenerConfig),
    create_group(exclusive, listener_group(ListenerId), ListenerLimiters).

-spec update_listener_limiters(listener_id(), term()) -> ok.
update_listener_limiters(ListenerId, ListenerConfig) ->
    ListenerLimiters = config_limiters(ListenerConfig),
    update_group(listener_group(ListenerId), ListenerLimiters).

-spec delete_listener_limiters(listener_id()) -> ok.
delete_listener_limiters(ListenerId) ->
    try
        delete_group(listener_group(ListenerId))
    catch
        error:{limiter_group_not_found, _} ->
            ok
    end.

-spec create_channel_client_container(zone(), listener_id()) -> emqx_limiter_client_container:t().
create_channel_client_container(ZoneName, ListenerId) ->
    create_client_container(ZoneName, ListenerId, [messages, bytes]).

-spec create_esockd_limiter_client(zone(), listener_id()) -> emqx_esockd_limiter:create_options().
create_esockd_limiter_client(ZoneName, ListenerId) ->
    LimiterClient = create_listener_limiter(ZoneName, ListenerId, max_conn),
    emqx_esockd_limiter:create_options(LimiterClient).

%%--------------------------------------------------------------------
%% Generic limiter client API
%%--------------------------------------------------------------------

-spec connect(id()) -> emqx_limiter_client:t().
connect({Group, _} = ListenerId) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            error({limiter_group_not_found, Group});
        {Module, _} ->
            Module:connect(ListenerId)
    end.

-spec create_group(shared | exclusive | module(), group(), [{name(), options()}]) -> ok.
%% Shortcuts for built-in types
create_group(shared, Group, Options) ->
    create_group(emqx_limiter_shared, Group, Options);
create_group(exclusive, Group, Options) ->
    create_group(emqx_limiter_exclusive, Group, Options);
%% Any other module
create_group(Module, Group, Options) ->
    ok = emqx_limiter_registry:register_group(Group, Module, Options),
    Module:create_group(Group, Options).

-spec update_group(group(), [{name(), options()}]) -> ok.
update_group(Group, Options) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            error({limiter_group_not_found, Group});
        {Module, _OldLimiterConfigs} ->
            ok = emqx_limiter_registry:register_group(Group, Module, Options),
            ok = Module:update_group(Group, Options)
    end.

-spec delete_group(group()) -> ok.
delete_group(Group) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            error({limiter_group_not_found, Group});
        {Module, _} ->
            ok = Module:delete_group(Group),
            ok = emqx_limiter_registry:unregister_group(Group)
    end.

%%--------------------------------------------------------------------
%% Zone config update
%%--------------------------------------------------------------------

add_handler() ->
    ok = emqx_config_handler:add_handler([mqtt, limiter], ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler([mqtt, limiter]),
    ok.

post_config_update([mqtt, limiter], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    update_zone_limiters().

propagated_post_config_update([mqtt, limiter], _UpdateReq, _NewConf, _OldConf, _AppEnvs) ->
    update_zone_limiters().

%%--------------------------------------------------------------------
%% Config helpers
%%--------------------------------------------------------------------

%% @doc get the config of a limiter from a config map of different parameters.
%%
%% The convention is as follows:
%% Limiter with name `x` is configured with the following keys in a config map:
%%  `x_rate`, `x_burst`, `x_rate_window`, and `x_burst_window`.
%%
%% Having a config like
%% ```
%% Config = #{
%%   foo => bar,
%%   x_rate => {10, 1000},
%%   x_burst => {100, 300000},
%% }
%% ```
%% means that the limiter `x` has a rate of 10 tokens per 1000ms and a burst of 100 each 5 minutes.
%%
%% The `config(x, Config)` function will return limiter config
%%  `#{capacity => 10, burst_capacity => 110, interval => 1000, burst_interval => 30000}`.
%%
%% If the limiter `x` is not configured, the function will return unlimited limiter config
%%  `#{capacity => infinity}`.
-spec config(name(), emqx_config:config()) -> options().
config(Name, Config) ->
    RateKey = to_rate_key(Name),
    case Config of
        #{RateKey := {Capacity, Interval}} ->
            BurstKey = to_burst_key(Name),
            case Config of
                #{BurstKey := {BurstCapacity, BurstInterval}} ->
                    %% limited_with_burst()
                    #{
                        capacity => Capacity,
                        burst_capacity => BurstCapacity + Capacity,
                        interval => Interval,
                        burst_interval => BurstInterval
                    };
                _ ->
                    %% limited()
                    #{
                        capacity => Capacity,
                        interval => Interval,
                        burst_capacity => 0
                    }
            end;
        _ ->
            %% unlimited()
            #{
                capacity => infinity
            }
    end.

config_unlimited() ->
    #{
        capacity => infinity
    }.

config_from_rps(RPS) ->
    #{
        capacity => RPS,
        interval => 1000,
        burst_capacity => 0
    }.

config_from_rate(infinity) ->
    config_unlimited();
config_from_rate({Capacity, Interval}) ->
    #{
        capacity => Capacity,
        interval => Interval,
        burst_capacity => 0
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Zone-related

zone_group(Zone) when is_atom(Zone) ->
    {zone, Zone}.

zone_limiters(Zone) when is_atom(Zone) ->
    Config = emqx_config:get_zone_conf(Zone, [mqtt, limiter], #{}),
    config_limiters(Config).

create_zone_limiters(Zone) ->
    ZoneLimiters = zone_limiters(Zone),
    create_group(shared, zone_group(Zone), ZoneLimiters).

update_zone_limiters(Zone) ->
    ZoneLimiters = zone_limiters(Zone),
    update_group(zone_group(Zone), ZoneLimiters).

delete_zone_limiters(Zone) ->
    delete_group(zone_group(Zone)).

%% Listener-related

listener_group(ListenerConfig) ->
    {listener, ListenerConfig}.

%% General helper functions

create_listener_limiter(ZoneName, ListenerId, Name) ->
    ZoneLimiterId = {zone_group(ZoneName), Name},
    ZoneLimiterClient = connect(ZoneLimiterId),
    ListenerLimiterId = {listener_group(ListenerId), Name},
    ListenerLimiterClient = connect(ListenerLimiterId),
    emqx_limiter_composite:new([
        ZoneLimiterClient, ListenerLimiterClient
    ]).

create_client_container(ZoneName, ListenerId, Names) ->
    Clients = lists:map(
        fun(Name) ->
            LimiterClient = create_listener_limiter(ZoneName, ListenerId, Name),
            {Name, LimiterClient}
        end,
        Names
    ),
    emqx_limiter_client_container:new(Clients).

%% NOTE
%% all limiter names are predefined, so we ignore atom leakage threat
to_rate_key(Name) ->
    NameStr = emqx_utils_conv:str(Name),
    list_to_atom(NameStr ++ "_rate").

to_burst_key(Name) ->
    NameStr = emqx_utils_conv:str(Name),
    list_to_atom(NameStr ++ "_burst").

config_limiters(Config) ->
    lists:map(
        fun(Name) ->
            {Name, config(Name, Config)}
        end,
        emqx_limiter_schema:mqtt_limiter_names()
    ).
