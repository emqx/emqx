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

-module(emqx_portal_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_rpc/1,
         t_mqtt/1
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_mqtt.hrl").
-include("emqx.hrl").

-define(wait(For, Timeout), emqx_ct_helpers:wait_for(?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

all() -> [t_rpc,
          t_mqtt
         ].

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            net_kernel:start(['emqx@127.0.0.1', longnames]);
        _ ->
            ok
    end,
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_rpc(Config) when is_list(Config) ->
    Cfg = #{address => node(),
            forwards => [<<"t_rpc/#">>],
            connect_module => emqx_portal_rpc,
            mountpoint => <<"forwarded">>
           },
    {ok, Pid} = emqx_portal:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
        {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
        %% message from a different client, to avoid getting terminated by no-local
        Msg1 = emqx_message:make(<<"ClientId-2">>, ?QOS_2, <<"t_rpc/one">>, <<"hello">>),
        ok = emqx_session:subscribe(SPid, [{<<"forwarded/t_rpc/one">>, #{qos => ?QOS_1}}]),
        PacketId = 1,
        emqx_session:publish(SPid, PacketId, Msg1),
        ?wait(case emqx_mock_client:get_last_message(ConnPid) of
                  {publish, PacketId, #message{topic = <<"forwarded/t_rpc/one">>}} -> true;
                  Other -> Other
              end, 4000),
        emqx_mock_client:close_session(ConnPid)
    after
        ok = emqx_portal:stop(Pid)
    end.

t_mqtt(Config) when is_list(Config) -> ok.


