%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_CONF_HRL).
-define(EMQX_CONF_HRL, true).

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CLUSTER_MFA, cluster_rpc_mfa).
-define(CLUSTER_COMMIT, cluster_rpc_commit).
-define(DEFAULT_INIT_TXN_ID, -1).

-record(cluster_rpc_mfa, {
    tnx_id :: pos_integer(),
    mfa :: {module(), atom(), [any()]},
    created_at :: calendar:datetime(),
    initiator :: node()
}).

-record(cluster_rpc_commit, {
    node :: node(),
    tnx_id :: pos_integer() | '$1'
}).

-define(SUGGESTION(Node),
    lists:flatten(
        io_lib:format(
            "run `./bin/emqx_ctl conf cluster_sync fix`"
            " on ~p(config leader) to force sync the configs, "
            "if this node has been lagging for more than 3 minutes.",
            [Node]
        )
    )
).

-endif.
