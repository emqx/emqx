%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_ctx_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_gateway_test_utils:load_all_gateway_apps(),
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun
            (#{clientid := bad_client}) ->
                {error, bad_username_or_password};
            (#{clientid := admin}) ->
                {ok, #{is_superuser => true}};
            (_) ->
                {ok, #{}}
        end
    ),
    Conf.

end_per_suite(_Conf) ->
    meck:unload(emqx_access_control),
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_authenticate(_) ->
    Ctx = #{gwname => mqttsn, cm => self()},
    Info1 = #{
        mountpoint => undefined,
        clientid => <<"user1">>
    },
    NInfo1 = default_result(Info1),
    ?assertMatch({ok, NInfo1}, emqx_gateway_ctx:authenticate(Ctx, Info1)),

    Info2 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => <<"user1">>
    },
    NInfo2 = default_result(Info2#{mountpoint => <<"mqttsn/user1/">>}),
    ?assertMatch({ok, NInfo2}, emqx_gateway_ctx:authenticate(Ctx, Info2)),

    Info3 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => bad_client
    },
    {error, bad_username_or_password} =
        emqx_gateway_ctx:authenticate(Ctx, Info3),

    Info4 = #{
        mountpoint => undefined,
        clientid => admin
    },
    ?assertMatch({ok, #{is_superuser := true}}, emqx_gateway_ctx:authenticate(Ctx, Info4)),
    ok.

default_result(Info) -> Info#{zone => default, is_superuser => false, auth_expire_at => undefined}.
