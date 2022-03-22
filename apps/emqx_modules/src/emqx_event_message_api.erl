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
-module(emqx_event_message_api).

-include("emqx_modules.hrl").

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([event_message/2]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/mqtt/event_message"].

schema("/mqtt/event_message") ->
    #{
        'operationId' => event_message,
        get =>
            #{
                description => <<"Event Message">>,
                tags => ?API_TAG_MQTT,
                responses =>
                    #{200 => status_schema(<<"Get Event Message config successfully">>)}
            },
        put =>
            #{
                description => <<"Update Event Message">>,
                tags => ?API_TAG_MQTT,
                'requestBody' => status_schema(<<"Update Event Message config">>),
                responses =>
                    #{200 => status_schema(<<"Update Event Message config successfully">>)}
            }
    }.

status_schema(Desc) ->
    mk(ref(?API_SCHEMA_MODULE, "event_message"), #{in => body, desc => Desc}).

event_message(get, _Params) ->
    {200, emqx_event_message:list()};
event_message(put, #{body := Body}) ->
    case emqx_event_message:update(Body) of
        {ok, NewConfig} ->
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {500, 'INTERNAL_ERROR', Message}
    end.
