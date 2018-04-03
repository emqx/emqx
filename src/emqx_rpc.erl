%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_rpc).

-export([call/4, cast/4]).

-export([multicall/4]).

call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args).

multicall(Nodes, Mod, Fun, Args) ->
    rpc:multicall(Nodes, Mod, Fun, Args).

cast(Node, Mod, Fun, Args) ->
    rpc:cast(Node, Mod, Fun, Args).

