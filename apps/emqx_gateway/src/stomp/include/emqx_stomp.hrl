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

-ifndef(EMQX_STOMP_HRL).
-define(EMQX_STOMP_HRL, true).

-define(STOMP_VER, <<"1.2">>).

-define(STOMP_SERVER, <<"emqx-stomp/1.2">>).

%%--------------------------------------------------------------------
%% STOMP Frame
%%--------------------------------------------------------------------

%% client command
-define(CMD_STOMP, <<"STOMP">>).
-define(CMD_CONNECT, <<"CONNECT">>).
-define(CMD_SEND, <<"SEND">>).
-define(CMD_SUBSCRIBE, <<"SUBSCRIBE">>).
-define(CMD_UNSUBSCRIBE, <<"UNSUBSCRIBE">>).
-define(CMD_BEGIN, <<"BEGIN">>).
-define(CMD_COMMIT, <<"COMMIT">>).
-define(CMD_ABORT, <<"ABORT">>).
-define(CMD_ACK, <<"ACK">>).
-define(CMD_NACK, <<"NACK">>).
-define(CMD_DISCONNECT, <<"DISCONNECT">>).

%% server command
-define(CMD_CONNECTED, <<"CONNECTED">>).
-define(CMD_MESSAGE, <<"MESSAGE">>).
-define(CMD_RECEIPT, <<"RECEIPT">>).
-define(CMD_ERROR, <<"ERROR">>).
-define(CMD_HEARTBEAT, <<"HEARTBEAT">>).

%-type client_command() :: ?CMD_SEND | ?CMD_SUBSCRIBE | ?CMD_UNSUBSCRIBE
%                        | ?CMD_BEGIN | ?CMD_COMMIT | ?CMD_ABORT | ?CMD_ACK
%                        | ?CMD_NACK | ?CMD_DISCONNECT | ?CMD_CONNECT
%                        | ?CMD_STOMP.
%
-type client_command() :: binary().

%-type server_command() :: ?CMD_CONNECTED | ?CMD_MESSAGE | ?CMD_RECEIPT
%                        | ?CMD_ERROR.
-type server_command() :: binary().

-record(stomp_frame, {
    command :: client_command() | server_command(),
    headers = [],
    body = <<>> :: iodata()
}).

-type stomp_frame() :: #stomp_frame{}.

-define(PACKET(CMD), #stomp_frame{command = CMD}).

-define(PACKET(CMD, Headers), #stomp_frame{command = CMD, headers = Headers}).

-define(PACKET(CMD, Headers, Body), #stomp_frame{
    command = CMD,
    headers = Headers,
    body = Body
}).

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

-define(MAX_HEADER_NUM, 10).
-define(MAX_HEADER_LENGTH, 1024).
-define(MAX_BODY_LENGTH, 65536).

-endif.
