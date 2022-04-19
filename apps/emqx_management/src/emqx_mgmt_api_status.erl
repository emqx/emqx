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
-module(emqx_mgmt_api_status).
%% API
-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([running_status/2]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/status"].

schema("/status") ->
    #{
        'operationId' => running_status,
        get =>
            #{
                description => <<"Node running status">>,
                security => [],
                responses =>
                    #{
                        200 =>
                            #{
                                description => <<"Node is running">>,
                                content =>
                                    #{
                                        'text/plain' =>
                                            #{
                                                schema => #{type => string},
                                                example =>
                                                    <<"Node emqx@127.0.0.1 is started\nemqx is running">>
                                            }
                                    }
                            }
                    }
            }
    }.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

running_status(get, _Params) ->
    BrokerStatus =
        case emqx:is_running() of
            true ->
                started;
            false ->
                stopped
        end,
    AppStatus =
        case lists:keysearch(emqx, 1, application:which_applications()) of
            false -> not_running;
            {value, _Val} -> running
        end,
    Status = io_lib:format("Node ~ts is ~ts~nemqx is ~ts", [node(), BrokerStatus, AppStatus]),
    Body = list_to_binary(Status),
    {200, #{<<"content-type">> => <<"text/plain">>}, Body}.
