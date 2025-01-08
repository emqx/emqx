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
-ifndef(EMQX_PERSISTENT_MESSAGE_HRL).
-define(EMQX_PERSISTENT_MESSAGE_HRL, true).

-define(PERSISTENT_MESSAGE_DB, messages).
-define(PERSISTENCE_ENABLED, emqx_message_persistence_enabled).

-define(WITH_DURABILITY_ENABLED(DO),
    case is_persistence_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

-endif.
