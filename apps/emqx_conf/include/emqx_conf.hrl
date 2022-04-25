-ifndef(EMQX_CONF_HRL).
-define(EMQX_CONF_HRL, true).

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CLUSTER_MFA, cluster_rpc_mfa).
-define(CLUSTER_COMMIT, cluster_rpc_commit).

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

-endif.
