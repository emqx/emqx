%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_tar/3
]).

-include_lib("emqx/include/bpapi.hrl").

-type name_vsn() :: binary() | string().

introduced_in() ->
    "5.0.21".

-spec get_tar(node(), name_vsn(), timeout()) -> {ok, binary()} | {error, any}.
get_tar(Node, NameVsn, Timeout) ->
    rpc:call(Node, emqx_plugins, get_tar, [NameVsn], Timeout).
