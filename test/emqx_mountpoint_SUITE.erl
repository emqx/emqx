%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mountpoint_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() -> [t_mount_unmount, t_replvar].

t_mount_unmount(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    Msg2 = emqx_mountpoint:mount(<<"mount">>, Msg),
    ?assertEqual(<<"mounttopic">>, Msg2#message.topic),
    TopicFilter = [{<<"mounttopic">>, #{qos => ?QOS_2}}],
    TopicFilter = emqx_mountpoint:mount(<<"mount">>, [{<<"topic">>, #{qos => ?QOS_2}}]),
    Msg = emqx_mountpoint:unmount(<<"mount">>, Msg2).

t_replvar(_) ->
    <<"mount/test/clientid">> = emqx_mountpoint:replvar(<<"mount/%u/%c">>, #{client_id => <<"clientid">>, username => <<"test">>}).
