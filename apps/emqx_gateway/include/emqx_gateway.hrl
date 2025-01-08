%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_GATEWAY_HRL).
-define(EMQX_GATEWAY_HRL, 1).

-type gateway_name() :: atom().

%% @doc The Gateway definition
-type gateway() ::
    #{
        name := gateway_name(),
        %% Description
        descr => binary() | undefined,
        %% Appears only in getting gateway info
        status => stopped | running | unloaded,
        %% Timestamp in millisecond
        created_at => integer(),
        %% Timestamp in millisecond
        started_at => integer(),
        %% Timestamp in millisecond
        stopped_at => integer(),
        %% Appears only in getting gateway info
        config => emqx_config:config()
    }.

-type gateway_def() ::
    #{
        name := gateway_name(),
        callback_module := module(),
        config_schema_module := module()
    }.

-define(GATEWAY_SUP_NAME, emqx_gateway_sup).

-endif.
