%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% this module is only intended to be mocked
-module(emqx_bridge_v2_dummy_connector).
-behavior(emqx_resource).

-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_get_channel_status/3
]).
resource_type() -> dummy.
callback_mode() -> error(unexpected).
on_start(_, _) -> error(unexpected).
on_stop(_, _) -> error(unexpected).
on_add_channel(_, _, _, _) -> error(unexpected).
on_get_channel_status(_, _, _) -> error(unexpected).
