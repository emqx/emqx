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

-module(emqx_gateway_ctx_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_access_control,
        authenticate,
        fun
            (#{clientid := bad_client}) ->
                {error, bad_username_or_password};
            (ClientInfo) ->
                {ok, ClientInfo}
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
    NInfo1 = zone(Info1),
    ?assertEqual({ok, NInfo1}, emqx_gateway_ctx:authenticate(Ctx, Info1)),

    Info2 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => <<"user1">>
    },
    NInfo2 = zone(Info2#{mountpoint => <<"mqttsn/user1/">>}),
    ?assertEqual({ok, NInfo2}, emqx_gateway_ctx:authenticate(Ctx, Info2)),

    Info3 = #{
        mountpoint => <<"mqttsn/${clientid}/">>,
        clientid => bad_client
    },
    {error, bad_username_or_password} =
        emqx_gateway_ctx:authenticate(Ctx, Info3),
    ok.

zone(Info) -> Info#{zone => default}.
