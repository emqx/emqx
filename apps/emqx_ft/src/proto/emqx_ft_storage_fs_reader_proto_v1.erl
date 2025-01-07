%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_fs_reader_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([read/3]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.17".

-spec read(node(), pid(), pos_integer()) ->
    {ok, binary()} | eof | {error, term()} | no_return().
read(Node, Pid, Bytes) when
    is_atom(Node) andalso is_pid(Pid) andalso is_integer(Bytes) andalso Bytes > 0
->
    emqx_rpc:call(Node, emqx_ft_storage_fs_reader, read, [Pid, Bytes]).
