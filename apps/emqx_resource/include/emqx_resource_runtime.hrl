%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_RESOURCE_RUNTIME_HRL).
-define(EMQX_RESOURCE_RUNTIME_HRL, true).

-include("emqx_resource.hrl").

-define(NO_CHANNEL, no_channel).

%% status and error, cached in ets for each connector
-type st_err() :: #{
    status := resource_status(),
    error := term()
}.

%% the relatively stable part to be cached in persistent_term for each connector
-type cb() :: #{
    mod := module(),
    callback_mode := callback_mode(),
    query_mode := query_mode(),
    state := term()
}.

%% the rutime context to be used for each channel
-record(rt, {
    st_err :: st_err(),
    cb :: cb(),
    query_mode :: emqx_resource:resource_query_mode(),
    channel_status :: ?NO_CHANNEL | channel_status()
}).

-type runtime() :: #rt{}.

-endif.
