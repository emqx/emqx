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

-module(emqx_mod_telemetry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(proplists, [get_value/2]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = ekka_mnesia:start(),
    ok = emqx_mod_telemetry:mnesia(boot),
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx_modules]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules]).

t_uuid(_) ->
    UUID = emqx_mod_telemetry:generate_uuid(),
    Parts = binary:split(UUID, <<"-">>, [global, trim]),
    ?assertEqual(5, length(Parts)),
    {ok, UUID2} = emqx_mod_telemetry:get_uuid(),
    emqx_mod_telemetry:unload(#{}),
    emqx_mod_telemetry:load(#{}),
    {ok, UUID3} = emqx_mod_telemetry:get_uuid(),
    ?assertEqual(UUID2, UUID3).

t_official_version(_) ->
    true = emqx_mod_telemetry:official_version("0.0.0"),
    true = emqx_mod_telemetry:official_version("1.1.1"),
    true = emqx_mod_telemetry:official_version("10.10.10"),
    false = emqx_mod_telemetry:official_version("0.0.0.0"),
    false = emqx_mod_telemetry:official_version("1.1.a"),
    true = emqx_mod_telemetry:official_version("0.0-alpha.1"),
    true = emqx_mod_telemetry:official_version("1.1-alpha.1"),
    true = emqx_mod_telemetry:official_version("10.10-alpha.10"),
    false = emqx_mod_telemetry:official_version("1.1-alpha.0"),
    true = emqx_mod_telemetry:official_version("1.1-beta.1"),
    true = emqx_mod_telemetry:official_version("1.1-rc.1"),
    false = emqx_mod_telemetry:official_version("1.1-alpha.a").

t_get_telemetry(_) ->
    {ok, TelemetryData} = emqx_mod_telemetry:get_telemetry(),
    OTPVersion = bin(erlang:system_info(otp_release)),
    ?assertEqual(OTPVersion, get_value(otp_version, TelemetryData)),
    {ok, UUID} = emqx_mod_telemetry:get_uuid(),
    ?assertEqual(UUID, get_value(uuid, TelemetryData)),
    ?assertEqual(0, get_value(num_clients, TelemetryData)).

t_enable(_) ->
    ok = meck:new(emqx_mod_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_mod_telemetry, official_version, fun(_) -> true end),
    ok = emqx_mod_telemetry:load(#{}),
    ?assertEqual(true, emqx_mod_telemetry:get_status()),
    ok = emqx_mod_telemetry:unload(#{}),
    ?assertEqual(false, emqx_mod_telemetry:get_status()),
    meck:unload([emqx_mod_telemetry]).

t_send_after_enable(_) ->
    ok = meck:new(emqx_mod_telemetry, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_mod_telemetry, official_version, fun(_) -> true end),
    ok = emqx_mod_telemetry:unload(#{}),
    ok = snabbkaffe:start_trace(),
    try
        ok = emqx_mod_telemetry:load(#{}),
        ?assertMatch({ok, _}, ?block_until(#{?snk_kind := telemetry_data_reported}, 2000, 100))
    after
        ok = snabbkaffe:stop(),
        meck:unload([emqx_mod_telemetry])
    end.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.
