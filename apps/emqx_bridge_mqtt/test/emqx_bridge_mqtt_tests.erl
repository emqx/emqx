%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

send_and_ack_test() ->
    %% delegate from gen_rpc to rpc for unit test
    meck:new(emqtt, [passthrough, no_history]),
    meck:expect(emqtt, start_link, 1,
                fun(_) ->
                        {ok, spawn_link(fun() -> ok end)}
                end),
    meck:expect(emqtt, connect, 1, {ok, dummy}),
    meck:expect(emqtt, stop, 1,
                fun(Pid) -> Pid ! stop end),
    meck:expect(emqtt, publish, 2,
                fun(Client, Msg) ->
                    Client ! {publish, Msg},
                    {ok, Msg} %% as packet id
                end),
    try
        Max = 1,
        Batch = lists:seq(1, Max),
        {ok, Conn} = emqx_bridge_mqtt:start(#{address => "127.0.0.1:1883"}),
    %     %% return last packet id as batch reference
        {ok, _AckRef} = emqx_bridge_mqtt:send(Conn, Batch),

        ok = emqx_bridge_mqtt:stop(Conn)
    after
        meck:unload(emqtt)
    end.

replvar_test() ->
    Node = atom_to_list(node()),
    Config = #{clientid => <<"Hey ${node}">>, topic => <<"topic ${node}">>, other => <<"other">>},

    ReplacedConfig = emqx_bridge_mqtt:replvar(Config),

    ExpectedConfig = #{clientid => iolist_to_binary("Hey " ++ Node),
                       topic    => iolist_to_binary("topic " ++ Node),
                       other    => <<"other">>},
    ?assertEqual(ExpectedConfig, ReplacedConfig).
