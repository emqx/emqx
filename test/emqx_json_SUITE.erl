%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_json_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(DEC_OPTS, [{labels, atom}, return_maps]).

all() -> emqx_ct:all(?MODULE).

t_decode_encode(_) ->
    JsonText = <<"{\"library\": \"jsx\", \"awesome\": true}">>,
    JsonTerm = emqx_json:decode(JsonText),
    JsonMaps = #{library => <<"jsx">>, awesome => true},
    ?assertEqual(JsonText, emqx_json:encode(JsonTerm, [{space, 1}])),
    ?assertEqual(JsonMaps, emqx_json:decode(JsonText, ?DEC_OPTS)).

t_safe_decode_encode(_) ->
    JsonText = <<"{\"library\": \"jsx\", \"awesome\": true}">>,
    {ok, JsonTerm} = emqx_json:safe_decode(JsonText),
    JsonMaps = #{library => <<"jsx">>, awesome => true},
    ?assertEqual({ok, JsonText}, emqx_json:safe_encode(JsonTerm, [{space, 1}])),
    ?assertEqual({ok, JsonMaps}, emqx_json:safe_decode(JsonText, ?DEC_OPTS)),
    BadJsonText = <<"{\"library\", \"awesome\": true}">>,
    ?assertEqual({error, badarg}, emqx_json:safe_decode(BadJsonText)),
    {error, badarg} = emqx_json:safe_encode({a, {b ,1}}).

