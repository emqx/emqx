%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Stomp Frame Header.

-define(STOMP_VER, <<"1.2">>).

-define(STOMP_SERVER, <<"emqx-stomp/1.2">>).

%%--------------------------------------------------------------------
%% STOMP Frame
%%--------------------------------------------------------------------

-record(stomp_frame, {command, headers = [], body = <<>> :: iodata()}).

-type(stomp_frame() :: #stomp_frame{}).

%%--------------------------------------------------------------------
%% Frame Size Limits
%%
%% To prevent malicious clients from exploiting memory allocation in a server,
%% servers MAY place maximum limits on:
%%
%% the number of frame headers allowed in a single frame
%% the maximum length of header lines
%% the maximum size of a frame body
%%
%% If these limits are exceeded the server SHOULD send the client an ERROR frame
%% and then close the connection.
%%--------------------------------------------------------------------

-define(MAX_HEADER_NUM,    10).
-define(MAX_HEADER_LENGTH, 1024).
-define(MAX_BODY_LENGTH,   65536).

