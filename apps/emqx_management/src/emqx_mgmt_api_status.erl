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

-export([
    init/2,
    path/0
]).

path() ->
    "/status".

init(Req0, State) ->
    {Code, Headers, Body} = running_status(),
    Req = cowboy_req:reply(Code, Headers, Body, Req0),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

running_status() ->
    case emqx_dashboard_listener:is_ready(timer:seconds(20)) of
        true ->
            BrokerStatus = broker_status(),
            AppStatus = application_status(),
            Body = io_lib:format("Node ~ts is ~ts~nemqx is ~ts", [node(), BrokerStatus, AppStatus]),
            {200, #{<<"content-type">> => <<"text/plain">>}, list_to_binary(Body)};
        false ->
            {503, #{<<"retry-after">> => <<"15">>}, <<>>}
    end.

broker_status() ->
    case emqx:is_running() of
        true ->
            started;
        false ->
            stopped
    end.

application_status() ->
    case lists:keysearch(emqx, 1, application:which_applications()) of
        false -> not_running;
        {value, _Val} -> running
    end.
