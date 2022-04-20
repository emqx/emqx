%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite([emqx_conf]).

t_get_put(_) ->
    {ok, Default} = get_sys_topics_config(),
    ?assertEqual(
        #{
            <<"sys_event_messages">> =>
                #{
                    <<"client_connected">> => true,
                    <<"client_disconnected">> => true,
                    <<"client_subscribed">> => false,
                    <<"client_unsubscribed">> => false
                },
            <<"sys_heartbeat_interval">> => <<"30s">>,
            <<"sys_msg_interval">> => <<"1m">>
        },
        Default
    ),

    NConfig = Default#{
        <<"sys_msg_interval">> => <<"4m">>,
        <<"sys_event_messages">> => #{<<"client_subscribed">> => false}
    },
    {ok, ConfigResp} = put_sys_topics_config(NConfig),
    ?assertEqual(NConfig, ConfigResp),
    {ok, Default} = put_sys_topics_config(Default).

get_sys_topics_config() ->
    Path = emqx_mgmt_api_test_util:api_path(["mqtt", "sys_topics"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Conf0} -> {ok, emqx_json:decode(Conf0, [return_maps])};
        Error -> Error
    end.

put_sys_topics_config(Config) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["mqtt", "sys_topics"]),
    case emqx_mgmt_api_test_util:request_api(put, Path, "", AuthHeader, Config) of
        {ok, Conf0} -> {ok, emqx_json:decode(Conf0, [return_maps])};
        Error -> Error
    end.
