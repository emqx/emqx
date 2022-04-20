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

-module(emqx_mgmt_api_sys).

-behaviour(minirest_api).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

%% API
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([sys/2]).

-define(TAGS, [<<"sys">>]).

namespace() -> "sys".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/mqtt/sys_topics"].

sys(get, _Params) ->
    {200, emqx_conf:get_raw([sys_topics], #{})};
sys(put, #{body := Body}) ->
    {ok, _} = emqx_conf:update([sys_topics], Body, #{override_to => cluster}),
    {200, emqx_conf:get_raw([sys_topics], #{})}.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/mqtt/sys_topics") ->
    #{
        'operationId' => sys,
        get =>
            #{
                tags => ?TAGS,
                description => <<"Get System Topics config">>,
                responses =>
                    #{
                        200 => schema_sys_topics()
                    }
            },
        put =>
            #{
                tags => ?TAGS,
                description => <<"Update System Topics config">>,
                'requestBody' => schema_sys_topics(),
                responses =>
                    #{
                        200 => schema_sys_topics()
                    }
            }
    }.

schema_sys_topics() ->
    emqx_dashboard_swagger:schema_with_example(
        hoconsc:ref(emqx_schema, "sys_topics"), example_sys_topics()
    ).

example_sys_topics() ->
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
    }.
