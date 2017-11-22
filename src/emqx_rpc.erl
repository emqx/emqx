%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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
%%
%% @doc EMQ X Distributed RPC.
%%
%%--------------------------------------------------------------------

-module(emqx_rpc).

-author("Feng Lee <feng@emqtt.io>").

-export([cast/4]).

%% @doc Wraps gen_rpc first.
cast(Node, Mod, Fun, Args) ->
    emqx_metrics:inc('messages/forward'), rpc:cast(Node, Mod, Fun, Args).

