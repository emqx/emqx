%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(
    emqx_mountpoint,
    [
        mount/2,
        unmount/2,
        replvar/2
    ]
).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_mount(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    TopicFilters = [{<<"topic">>, #{qos => 2}}],
    ?assertEqual(<<"topic">>, mount(undefined, <<"topic">>)),
    ?assertEqual(Msg, mount(undefined, Msg)),
    ?assertEqual(TopicFilters, mount(undefined, TopicFilters)),
    ?assertEqual(
        <<"device/1/topic">>,
        mount(<<"device/1/">>, <<"topic">>)
    ),
    ?assertEqual(
        Msg#message{topic = <<"device/1/topic">>},
        mount(<<"device/1/">>, Msg)
    ),
    ?assertEqual(
        [{<<"device/1/topic">>, #{qos => 2}}],
        mount(<<"device/1/">>, TopicFilters)
    ).

t_mount_share(_) ->
    T = {TopicFilter, Opts} = emqx_topic:parse(<<"$share/group/topic">>),
    TopicFilters = [T],
    ?assertEqual(TopicFilter, #share{group = <<"group">>, topic = <<"topic">>}),

    ?assertEqual(
        TopicFilter,
        mount(undefined, TopicFilter)
    ),
    ?assertEqual(
        #share{group = <<"group">>, topic = <<"device/1/topic">>},
        mount(<<"device/1/">>, TopicFilter)
    ),
    ?assertEqual(
        [{#share{group = <<"group">>, topic = <<"device/1/topic">>}, Opts}],
        mount(<<"device/1/">>, TopicFilters)
    ).

t_unmount(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"device/1/topic">>, <<"payload">>),
    ?assertEqual(<<"topic">>, unmount(undefined, <<"topic">>)),
    ?assertEqual(Msg, unmount(undefined, Msg)),
    ?assertEqual(<<"topic">>, unmount(<<"device/1/">>, <<"device/1/topic">>)),
    ?assertEqual(Msg#message{topic = <<"topic">>}, unmount(<<"device/1/">>, Msg)),
    ?assertEqual(<<"device/1/topic">>, unmount(<<"device/2/">>, <<"device/1/topic">>)),
    ?assertEqual(Msg#message{topic = <<"device/1/topic">>}, unmount(<<"device/2/">>, Msg)).

t_unmount_share(_) ->
    {TopicFilter, _Opts} = emqx_topic:parse(<<"$share/group/topic">>),
    MountedTopicFilter = #share{group = <<"group">>, topic = <<"device/1/topic">>},

    ?assertEqual(TopicFilter, #share{group = <<"group">>, topic = <<"topic">>}),

    ?assertEqual(
        TopicFilter,
        unmount(undefined, TopicFilter)
    ),
    ?assertEqual(
        #share{group = <<"group">>, topic = <<"topic">>},
        unmount(<<"device/1/">>, MountedTopicFilter)
    ).

t_replvar(_) ->
    ?assertEqual(undefined, replvar(undefined, #{})),
    ?assertEqual(
        <<"mount/user/clientid/">>,
        replvar(
            <<"mount/${username}/${clientid}/">>,
            #{
                clientid => <<"clientid">>,
                username => <<"user">>
            }
        )
    ),
    ?assertEqual(
        <<"mount/${username}/clientid/">>,
        replvar(
            <<"mount/${username}/${clientid}/">>,
            #{
                clientid => <<"clientid">>,
                username => undefined
            }
        )
    ),
    ?assertEqual(
        <<"mount/g1/clientid/">>,
        replvar(
            <<"mount/${client_attrs.group}/${clientid}/">>,
            #{
                clientid => <<"clientid">>,
                client_attrs => #{<<"group">> => <<"g1">>}
            }
        )
    ),
    ?assertEqual(
        <<"mount/${client_attrs.group}/clientid/">>,
        replvar(
            <<"mount/${client_attrs.group}/${clientid}/">>,
            #{
                clientid => <<"clientid">>
            }
        )
    ),
    ?assertEqual(
        <<"mount/${not.allowed}/clientid/">>,
        replvar(
            <<"mount/${not.allowed}/${clientid}/">>,
            #{
                clientid => <<"clientid">>
            }
        )
    ),
    ?assertEqual(
        <<"mount/default">>,
        replvar(
            <<"mount/${zone}">>,
            #{
                zone => default
            }
        )
    ).
