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

%% @doc The utility functions for erlang process dictionary.
-module(emqx_pd).

-include("types.hrl").

-export([ update_counter/2
        , get_counter/1
        , reset_counter/1
        ]).

-type(key() :: term()).

-spec(update_counter(key(), number()) -> maybe(number())).
update_counter(Key, Inc) ->
    put(Key, get_counter(Key) + Inc).

-spec(get_counter(key()) -> number()).
get_counter(Key) ->
    case get(Key) of undefined -> 0; Cnt -> Cnt end.

-spec(reset_counter(key()) -> number()).
reset_counter(Key) ->
    case put(Key, 0) of undefined -> 0; Cnt -> Cnt end.

