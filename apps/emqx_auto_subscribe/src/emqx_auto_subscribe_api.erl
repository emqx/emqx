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

-module(emqx_auto_subscribe_api).

-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([auto_subscribe/2]).

-define(INTERNAL_ERROR, 'INTERNAL_ERROR').
-define(EXCEED_LIMIT, 'EXCEED_LIMIT').
-define(BAD_REQUEST, 'BAD_REQUEST').

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    ["/mqtt/auto_subscribe"].

schema("/mqtt/auto_subscribe") ->
    #{
        'operationId' => auto_subscribe,
        get => #{
            description => ?DESC(list_auto_subscribe_api),
            responses => #{
                200 => hoconsc:ref(emqx_auto_subscribe_schema, "auto_subscribe")
            }
        },
        put => #{
            description => ?DESC(update_auto_subscribe_api),
            'requestBody' => hoconsc:ref(emqx_auto_subscribe_schema, "auto_subscribe"),
            responses => #{
                200 => hoconsc:ref(emqx_auto_subscribe_schema, "auto_subscribe"),
                409 => emqx_dashboard_swagger:error_codes(
                    [?EXCEED_LIMIT],
                    ?DESC(update_auto_subscribe_api_response409)
                )
            }
        }
    }.

%%%==============================================================================================
%% api apply
auto_subscribe(get, _) ->
    {200, emqx_auto_subscribe:list()};
auto_subscribe(put, #{body := #{}}) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Request body required">>}};
auto_subscribe(put, #{body := Params}) ->
    case emqx_auto_subscribe:update(Params) of
        {error, quota_exceeded} ->
            Message = list_to_binary(
                io_lib:format(
                    "Max auto subscribe topic count is  ~p",
                    [emqx_auto_subscribe:max_limit()]
                )
            ),
            {409, #{code => ?EXCEED_LIMIT, message => Message}};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {500, #{code => ?INTERNAL_ERROR, message => Message}};
        {ok, NewTopics} ->
            {200, NewTopics}
    end.
