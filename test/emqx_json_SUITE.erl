%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_json_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

all() -> [t_decode_encode, t_safe_decode_encode].

t_decode_encode(_) ->
    JsonText = <<"{\"library\": \"jsx\", \"awesome\": true}">>,
    JsonTerm = emqx_json:decode(JsonText),
    JsonMaps = #{library => <<"jsx">>, awesome => true},
    JsonMaps = emqx_json:decode(JsonText, [{labels, atom}, return_maps]),
    JsonText = emqx_json:encode(JsonTerm, [{space, 1}]).

t_safe_decode_encode(_) ->
    JsonText = <<"{\"library\": \"jsx\", \"awesome\": true}">>,
    {ok, JsonTerm} = emqx_json:safe_decode(JsonText),
    JsonMaps = #{library => <<"jsx">>, awesome => true},
    {ok, JsonMaps} = emqx_json:safe_decode(JsonText, [{labels, atom}, return_maps]),
    {ok, JsonText} = emqx_json:safe_encode(JsonTerm, [{space, 1}]),
    BadJsonText = <<"{\"library\", \"awesome\": true}">>,
    {error, _} = emqx_json:safe_decode(BadJsonText),
    {error, _} = emqx_json:safe_encode({a, {b ,1}}).
