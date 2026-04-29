%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_data_backup_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    peek_sensitive_table_sets/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.8.11".

-spec peek_sensitive_table_sets(node(), binary(), timeout()) ->
    {ok, [binary()]} | {error, _} | {badrpc, _}.
peek_sensitive_table_sets(Node, FileName, Timeout) ->
    rpc:call(Node, emqx_mgmt_data_backup, peek_sensitive_table_sets, [FileName], Timeout).
