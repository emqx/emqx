%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_utils_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_gateway_test_utils:load_all_gateway_apps(),
    Config.

end_per_suite(Config) ->
    Config.

t_global_chain(_Config) ->
    Names = emqx_gateway_schema:gateway_names(),
    lists:foreach(
        fun(Name) ->
            %% no exception is expected
            _ = emqx_gateway_utils:global_chain(Name)
        end,
        Names
    ),
    ?assertError({invalid_protocol_name, 'Others'}, emqx_gateway_utils:global_chain('Others')).

%% `authentication' carries credentials and is only used by the gateway
%% supervisor. It must not be printable from the connection callback arguments,
%% otherwise the esockd connection supervisor leaks it in offender reports.
t_connection_config_protects_authentication(_Config) ->
    Ctx = #{gwname => stomp, cm => self()},
    Authn = #{
        mechanism => jwt,
        backend => jwt,
        algorithm => <<"hmac-based">>,
        secret => <<"authn-secret">>,
        secret_base64_encoded => false
    },
    GwConfig = #{
        mountpoint => <<>>,
        authentication => Authn,
        clientinfo_override => #{username => <<"user1">>, password => <<"override-pw">>},
        listeners => #{tcp => #{default => #{bind => 1883}}}
    },
    ModConfig = #{frame_mod => emqx_stomp_frame, chann_mod => emqx_stomp_channel},
    RtConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(stomp, GwConfig, ModConfig, Ctx),
    [#{listener_opts := {esockd, #{mfa := {_Mod, _Fun, [ConnConfig]}}}}] = RtConfigs,
    Protected = maps:get(authentication, ConnConfig),
    ?assert(is_function(Protected, 0)),
    Rendered = iolist_to_binary(io_lib:format("~0p", [ConnConfig])),
    ?assertEqual(nomatch, binary:match(Rendered, <<"authn-secret">>)),
    ?assertEqual(Authn, emqx_secret:unwrap(Protected)),
    %% Rebuilding an unchanged configuration must yield an equal listener config,
    %% otherwise diffing would trigger spurious listener updates.  Equal secrets
    %% wrap to equal closures.
    ?assertEqual(
        RtConfigs,
        emqx_gateway_utils_conf:to_rt_listener_configs(stomp, GwConfig, ModConfig, Ctx)
    ),
    ?assertEqual(
        #{stop => [], update => [], start => []},
        emqx_gateway_utils_conf:diff_rt_listener_configs(
            RtConfigs,
            emqx_gateway_utils_conf:to_rt_listener_configs(stomp, GwConfig, ModConfig, Ctx)
        )
    ),
    %% Changing the authn chain must still be detected, so listener updates keep
    %% happening when the authentication configuration changes.
    ChangedGwConfig = GwConfig#{authentication => Authn#{secret => <<"other-secret">>}},
    ChangedRtConfigs = emqx_gateway_utils_conf:to_rt_listener_configs(
        stomp, ChangedGwConfig, ModConfig, Ctx
    ),
    #{update := [_]} = emqx_gateway_utils_conf:diff_rt_listener_configs(
        RtConfigs, ChangedRtConfigs
    ).
