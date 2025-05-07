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
-ifndef(EMQX_DS_BUILTIN_TX_HRL).
-define(EMQX_DS_BUILTIN_TX_HRL, true).

-record(kv_tx_ctx, {
    shard :: emqx_ds:shard(),
    leader :: pid(),
    serial :: term(),
    %% Current generation. Used to guard against committing
    %% transactions that span multiple generations
    generation :: emqx_ds_storage_layer:generation(),
    opts :: emqx_ds:transaction_opts()
}).

-record(ds_tx, {
    ctx :: emqx_ds:kv_tx_context() | emqx_ds:tx_context(),
    ops :: emqx_ds:kv_tx_ops() | emqx_ds:tx_ops(),
    from :: pid() | reference(),
    ref :: reference(),
    meta :: term()
}).

-endif.
