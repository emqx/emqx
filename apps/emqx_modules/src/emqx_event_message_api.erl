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

-behaviour(minirest_api).

-export([api_spec/0]).

-export([event_message/2]).

-import(emqx_mgmt_util, [ schema/1
                        ]).

api_spec() ->
    {[event_message_api()], []}.

conf_schema() ->
    emqx_mgmt_api_configs:gen_schema(emqx:get_config([event_message])).

event_message_api() ->
    Path = "/mqtt/event_message",
    Metadata = #{
        get => #{
            description => <<"Event Message">>,
            responses => #{
                <<"200">> => schema(conf_schema())
            }
        },
        put => #{
            description => <<"Update Event Message">>,
            'requestBody' => schema(conf_schema()),
            responses => #{
                <<"200">> => schema(conf_schema())
            }
        }
    },
    {Path, Metadata, event_message}.

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
