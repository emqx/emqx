%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([api_spec/0]).

-export([auto_subscribe/2]).

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').
-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    {[auto_subscribe_api()], []}.

schema() ->
    emqx_mgmt_util:schema(
        emqx_mgmt_api_configs:gen_schema(
            emqx:get_raw_config([auto_subscribe, topics])), <<"">>).

auto_subscribe_api() ->
    Metadata = #{
        get => #{
            description => <<"Auto subscribe list">>,
            responses => #{
                <<"200">> => schema()}},
        put => #{
            description => <<"Update auto subscribe topic list">>,
            'requestBody' => schema(),
            responses => #{
                <<"200">> => schema(),
                <<"400">> => emqx_mgmt_util:error_schema(
                                <<"Request body required">>, [?BAD_REQUEST]),
                <<"409">> => emqx_mgmt_util:error_schema(
                                <<"Auto Subscribe topics max limit">>, [?EXCEED_LIMIT])}}
    },
    {"/mqtt/auto_subscribe", Metadata, auto_subscribe}.

%%%==============================================================================================
%% api apply
auto_subscribe(get, _) ->
    {200, emqx_auto_subscribe:list()};

auto_subscribe(put, #{body := #{}}) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Request body required">>}};
auto_subscribe(put, #{body := Params}) ->
    case emqx_auto_subscribe:update(Params) of
        {error, quota_exceeded} ->
            Message = list_to_binary(io_lib:format("Max auto subscribe topic count is  ~p",
                                        [emqx_auto_subscribe:max_limit()])),
            {409, #{code => ?EXCEED_LIMIT, message => Message}};
        ok ->
            {200, emqx_auto_subscribe:list()}
    end.
