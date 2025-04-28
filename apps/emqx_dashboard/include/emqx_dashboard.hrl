%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-include("emqx_dashboard_rbac.hrl").

-define(ADMIN, emqx_admin).

-define(BACKEND_LOCAL, local).
-define(SSO_USERNAME(Backend, Name), {Backend, Name}).

-type dashboard_sso_backend() :: atom().
-type dashboard_sso_username() :: {dashboard_sso_backend(), binary()}.
-type dashboard_username() :: binary() | dashboard_sso_username().
-type dashboard_user_role() :: binary().

-record(?ADMIN, {
    username :: dashboard_username(),
    pwdhash :: binary(),
    description :: binary(),
    role = ?ROLE_DEFAULT :: dashboard_user_role(),
    extra = #{} :: map()
}).

-type dashboard_user() :: #?ADMIN{}.

-define(ADMIN_JWT, emqx_admin_jwt).

-record(?ADMIN_JWT, {
    token :: binary(),
    username :: binary(),
    exptime :: integer(),
    extra = #{} :: map()
}).

-define(TAB_COLLECT, emqx_collect).

-define(EMPTY_KEY(Key), ((Key == undefined) orelse (Key == <<>>))).

-define(DASHBOARD_SHARD, emqx_dashboard_shard).

-ifdef(TEST).
%% for test
-define(DEFAULT_SAMPLE_INTERVAL, 1).
-define(RPC_TIMEOUT, 50).
-else.
%% dashboard monitor do sample interval, default 10s
-define(DEFAULT_SAMPLE_INTERVAL, 10).
-define(RPC_TIMEOUT, 5000).
-endif.

-define(DELTA_SAMPLER_LIST, [
    received,
    sent,
    validation_succeeded,
    validation_failed,
    transformation_succeeded,
    transformation_failed,
    dropped,
    persisted
]).

-define(GAUGE_SAMPLER_LIST, [
    disconnected_durable_sessions,
    subscriptions_durable,
    subscriptions,
    topics,
    connections,
    live_connections
]).

-define(SAMPLER_LIST, (?GAUGE_SAMPLER_LIST ++ ?DELTA_SAMPLER_LIST)).

-define(DELTA_SAMPLER_RATE_MAP, #{
    received => received_msg_rate,
    sent => sent_msg_rate,
    validation_succeeded => validation_succeeded_rate,
    validation_failed => validation_failed_rate,
    transformation_succeeded => transformation_succeeded_rate,
    transformation_failed => transformation_failed_rate,
    dropped => dropped_msg_rate,
    persisted => persisted_rate
}).

-define(CURRENT_SAMPLE_NON_RATE,
    [
        node_uptime,
        retained_msg_count,
        shared_subscriptions
    ] ++ ?LICENSE_QUOTA
).

-define(CLUSTERONLY_SAMPLER_LIST, [
    subscriptions_durable,
    disconnected_durable_sessions
]).

-if(?EMQX_RELEASE_EDITION == ee).
-define(LICENSE_QUOTA, [license_quota]).
-else.
-define(LICENSE_QUOTA, []).
-endif.

%% record the max value over the history
-define(WATERMARK_SAMPLER_LIST, [
    %% sessions history high water mark is only recorded when
    %% the config broker.s
    sessions_hist_hwmark
]).

%% Pick the newer value from the two maps when merging
%% Keys are from ?WATERMARK_SAMPLER_LIST and ?GAUGE_SAMPLER_LIST
%% test case is added to ensure no missing key in this macro
-define(IS_PICK_NEWER(Key),
    (Key =:= sessions_hist_hwmark orelse
        Key =:= disconnected_durable_sessions orelse
        Key =:= subscriptions_durable orelse
        Key =:= subscriptions orelse
        Key =:= topics orelse
        Key =:= connections orelse
        Key =:= live_connections)
).

%% use this atom to indicate no value provided from http request
-define(NO_MFA_TOKEN, no_mfa_token).
%% use this atom for internal calls where token validation is not required
%% for example, when handling change_pwd request (which is authenticated
%% with bearer token).
-define(TRUSTED_MFA_TOKEN, trusted_mfa_token).
%% empty bin-string is when the token field exists, but empty
-define(IS_NO_MFA_TOKEN(X), (X =:= ?NO_MFA_TOKEN orelse X =:= <<>>)).
