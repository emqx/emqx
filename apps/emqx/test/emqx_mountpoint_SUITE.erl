%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mountpoint_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_mountpoint,
        [ mount/2
        , unmount/2
        , replvar/2
        ]).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_mount(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    TopicFilters = [{<<"topic">>, #{qos => 2}}],
    ?assertEqual(<<"topic">>, mount(undefined, <<"topic">>)),
    ?assertEqual(Msg, mount(undefined, Msg)),
    ?assertEqual(TopicFilters, mount(undefined, TopicFilters)),
    ?assertEqual(<<"device/1/topic">>,
                 mount(<<"device/1/">>, <<"topic">>)),
    ?assertEqual(Msg#message{topic = <<"device/1/topic">>},
                 mount(<<"device/1/">>, Msg)),
    ?assertEqual([{<<"device/1/topic">>, #{qos => 2}}],
                 mount(<<"device/1/">>, TopicFilters)).

t_unmount(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"device/1/topic">>, <<"payload">>),
    ?assertEqual(<<"topic">>, unmount(undefined, <<"topic">>)),
    ?assertEqual(Msg, unmount(undefined, Msg)),
    ?assertEqual(<<"topic">>, unmount(<<"device/1/">>, <<"device/1/topic">>)),
    ?assertEqual(Msg#message{topic = <<"topic">>}, unmount(<<"device/1/">>, Msg)),
    ?assertEqual(<<"device/1/topic">>, unmount(<<"device/2/">>, <<"device/1/topic">>)),
    ?assertEqual(Msg#message{topic = <<"device/1/topic">>}, unmount(<<"device/2/">>, Msg)).

t_replvar(_) ->
    ?assertEqual(undefined, replvar(undefined, #{})),
    ?assertEqual(<<"mount/user/clientid/">>,
                 replvar(<<"mount/%u/%c/">>,
                         #{clientid => <<"clientid">>,
                           username => <<"user">>
                          })),
    ?assertEqual(<<"mount/%u/clientid/">>,
                 replvar(<<"mount/%u/%c/">>,
                         #{clientid => <<"clientid">>,
                           username => undefined
                          })).

