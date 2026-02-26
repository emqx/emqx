-ifndef(EMQX_USERNAME_QUOTA_HRL).
-define(EMQX_USERNAME_QUOTA_HRL, true).

-define(DEFAULT_MAX_SESSIONS_PER_USERNAME, 100).
-define(DEFAULT_PAGE, 1).
-define(DEFAULT_LIMIT, 100).
-define(MAX_LIMIT, 100).
-define(DEFAULT_SNAPSHOT_REFRESH_INTERVAL_MS, 5000).
-define(DEFAULT_SNAPSHOT_REQUEST_TIMEOUT_MS, 5000).
-define(MIN_CLIENTID, <<>>).
-define(MIN_PID, 0).

%% ?RECORD_TAB stores one row per active session, keyed by {Username, ClientId, Pid}.
%% It is the canonical session membership data used for client listing and reconnect checks.
-define(RECORD_TAB, emqx_username_quota_record).
%% ?COUNTER_TAB stores per-node counters, keyed by {Username, Node}.
%% Global session count for a username is the sum across all nodes.
-define(COUNTER_TAB, emqx_username_quota_counter).
-define(MONITOR_TAB, emqx_username_quota_monitor).
-define(CCACHE_TAB, emqx_username_quota_ccache).
-define(SNAPSHOT_TAB, emqx_username_quota_snapshot).
-define(DB_SHARD, emqx_username_quota_shard).

-define(RECORD_KEY(Username, ClientId, Pid), {Username, ClientId, Pid}).
-define(COUNTER_KEY(Username, Node), {Username, Node}).
-define(MONITOR(Pid, Username, ClientId), {Pid, Username, ClientId}).
-define(CCACHE(Username, Ts, Cnt), {Username, Ts, Cnt}).
-define(CCACHE_VALID_MS, 5000).

-define(OVERRIDE_TAB, emqx_username_quota_override).

-define(LOCK(Node), {emqx_username_quota_clear_node_lock, Node}).

-record(?OVERRIDE_TAB, {
    username,   %% binary
    quota       %% non_neg_integer() | nolimit
}).

-record(?RECORD_TAB, {
    key,
    node,
    extra = #{}
}).

-record(?COUNTER_TAB, {
    key,
    count = 0
}).

-endif.
