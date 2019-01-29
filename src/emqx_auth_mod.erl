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

-module(emqx_auth_mod).

-include("emqx.hrl").

%%--------------------------------------------------------------------
%% Authentication behavihour
%%--------------------------------------------------------------------

-ifdef(use_specs).

-callback(init(AuthOpts :: list()) -> {ok, State :: term()}).

-callback(check(credentials(), password(), State :: term())
          -> ok | {ok, boolean()} | {ok, map()} |
             {continue, map()} | ignore | {error, term()}).
-callback(description() -> string()).

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init, 1}, {check, 3}, {description, 0}];
behaviour_info(_Other) ->
    undefined.

-endif.
