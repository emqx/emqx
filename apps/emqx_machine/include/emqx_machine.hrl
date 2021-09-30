%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQ_X_CLUSTER_RPC_HRL).
-define(EMQ_X_CLUSTER_RPC_HRL, true).

-define(CLUSTER_MFA, cluster_rpc_mfa).
-define(CLUSTER_COMMIT, cluster_rpc_commit).

-define(EMQX_MACHINE_SHARD, emqx_machine_shard).

-record(cluster_rpc_mfa, {
    tnx_id :: pos_integer(),
    mfa :: mfa(),
    created_at :: calendar:datetime(),
    initiator :: node()
}).

-record(cluster_rpc_commit, {
    node :: node(),
    tnx_id :: pos_integer() | '$1'
}).

-endif.
