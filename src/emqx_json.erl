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

-module(emqx_json).

-export([ encode/1
        , encode/2
        , safe_encode/1
        , safe_encode/2
        ]).

-export([ decode/1
        , decode/2
        , safe_decode/1
        , safe_decode/2
        ]).

-spec(encode(jsx:json_term()) -> jsx:json_text()).
encode(Term) ->
    jsx:encode(Term).

-spec(encode(jsx:json_term(), jsx_to_json:config()) -> jsx:json_text()).
encode(Term, Opts) ->
    jsx:encode(Term, Opts).

-spec(safe_encode(jsx:json_term())
      -> {ok, jsx:json_text()} | {error, term()}).
safe_encode(Term) ->
    safe_encode(Term, []).

-spec(safe_encode(jsx:json_term(), jsx_to_json:config())
      -> {ok, jsx:json_text()} | {error, term()}).
safe_encode(Term, Opts) ->
    try encode(Term, Opts) of
        Json -> {ok, Json}
    catch
        error:Reason ->
            {error, Reason}
    end.

-spec(decode(jsx:json_text()) -> jsx:json_term()).
decode(Json) ->
    jsx:decode(Json).

-spec(decode(jsx:json_text(), jsx_to_json:config()) -> jsx:json_term()).
decode(Json, Opts) ->
    jsx:decode(Json, Opts).

-spec(safe_decode(jsx:json_text())
      -> {ok, jsx:json_term()} | {error, term()}).
safe_decode(Json) ->
    safe_decode(Json, []).

-spec(safe_decode(jsx:json_text(), jsx_to_json:config())
      -> {ok, jsx:json_term()} | {error, term()}).
safe_decode(Json, Opts) ->
    try decode(Json, Opts) of
        Term -> {ok, Term}
    catch
        error:Reason ->
            {error, Reason}
    end.

