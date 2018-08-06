%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc.
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

-module(emqx_base62_SUITE).

-include_lib("eunit/include/eunit.hrl").

-define(BASE62, emqx_base62).

-compile(export_all).
-compile(nowarn_export_all).

all() -> [t_base62_encode].

t_base62_encode(_) ->
    <<"10">> = ?BASE62:decode(?BASE62:encode(<<"10">>)),
    <<"100">> = ?BASE62:decode(?BASE62:encode(<<"100">>)),
    <<"9999">> = ?BASE62:decode(?BASE62:encode(<<"9999">>)),
    <<"65535">> = ?BASE62:decode(?BASE62:encode(<<"65535">>)),
    <<X:128/unsigned-big-integer>> = emqx_guid:gen(),
    <<Y:128/unsigned-big-integer>> = emqx_guid:gen(),
    X = erlang:binary_to_integer(?BASE62:decode(?BASE62:encode(X))),
    Y = erlang:binary_to_integer(?BASE62:decode(?BASE62:encode(Y))).
