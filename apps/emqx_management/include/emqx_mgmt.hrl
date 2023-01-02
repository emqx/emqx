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

%% Return Codes
-define(SUCCESS, 0).  %% Success
-define(ERROR1, 101). %% badrpc
-define(ERROR2, 102). %% Unknown error
-define(ERROR3, 103). %% Username or password error
-define(ERROR4, 104). %% Empty username or password
-define(ERROR5, 105). %% User does not exist
-define(ERROR6, 106). %% Admin can not be deleted
-define(ERROR7, 107). %% Missing request parameter
-define(ERROR8, 108). %% Request parameter type error
-define(ERROR9, 109). %% Request parameter is not a json
-define(ERROR10, 110). %% Plugin has been loaded
-define(ERROR11, 111). %% Plugin has been unloaded
-define(ERROR12, 112). %% Client not online
-define(ERROR13, 113). %% User already exist
-define(ERROR14, 114). %% OldPassword error
-define(ERROR15, 115). %% bad topic
-define(ERROR16, 116). %% bad QoS

-define(VERSIONS, ["4.0", "4.1", "4.2", "4.3", "4.4"]).
