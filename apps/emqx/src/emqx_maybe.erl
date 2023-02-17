%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_maybe).

-include_lib("emqx/include/types.hrl").

-export([to_list/1]).
-export([from_list/1]).
-export([apply/2]).

-spec to_list(maybe(A)) -> [A].
to_list(undefined) ->
    [];
to_list(Term) ->
    [Term].

-spec from_list([A]) -> maybe(A).
from_list([]) ->
    undefined;
from_list([Term]) ->
    Term.

-spec apply(fun((maybe(A)) -> maybe(A)), maybe(A)) ->
    maybe(A).
apply(_Fun, undefined) ->
    undefined;
apply(Fun, Term) when is_function(Fun) ->
    erlang:apply(Fun, [Term]).
