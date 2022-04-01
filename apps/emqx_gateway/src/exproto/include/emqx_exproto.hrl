%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_exproto).

-define(TCP_SOCKOPTS, [
    binary,
    {packet, raw},
    {reuseaddr, true},
    {backlog, 512},
    {nodelay, true}
]).

-define(UDP_SOCKOPTS, []).

%%--------------------------------------------------------------------
%% gRPC result code

-define(RESP_UNKNOWN, 'UNKNOWN').
-define(RESP_SUCCESS, 'SUCCESS').
-define(RESP_CONN_PROCESS_NOT_ALIVE, 'CONN_PROCESS_NOT_ALIVE').
-define(RESP_PARAMS_TYPE_ERROR, 'PARAMS_TYPE_ERROR').
-define(RESP_REQUIRED_PARAMS_MISSED, 'REQUIRED_PARAMS_MISSED').
-define(RESP_PERMISSION_DENY, 'PERMISSION_DENY').
-define(IS_GRPC_RESULT_CODE(C),
    (C =:= ?RESP_SUCCESS orelse
        C =:= ?RESP_CONN_PROCESS_NOT_ALIVE orelse
        C =:= ?RESP_REQUIRED_PARAMS_MISSED orelse
        C =:= ?RESP_PERMISSION_DENY)
).
