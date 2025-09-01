%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_BUILTIN_RAFT_HRL).
-define(EMQX_DS_BUILTIN_RAFT_HRL, true).

%% Gvars:
%%   These gvars are kept on all replicas of the shard (not just the leader).
-define(gv_sc_replica, replica).
%%   Replica gvars:
-define(gv_timestamp, timestamp).
-define(gv_otx_leader_pid, otx_leader_pid).
%%   These gvars are set on the leader replica of the shard:
-define(gv_sc_leader, leader).

-endif.
