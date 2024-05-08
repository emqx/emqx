%%--------------------------------------------------------------------
%% Copyright (c) 2022, 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(EMQX_DS_REPLICATION_LAYER_HRL).
-define(EMQX_DS_REPLICATION_LAYER_HRL, true).

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(STREAM, 1).
-define(IT, 2).
-define(BATCH, 3).
-define(DELETE_IT, 4).

%% keys:
-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

%% ?BATCH
-define(batch_messages, 2).
-define(timestamp, 3).

%% add_generation / update_config
-define(config, 2).
-define(since, 3).

%% drop_generation
-define(generation, 2).

%% custom events
-define(payload, 2).
-define(now, 3).

-endif.
