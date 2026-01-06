%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This header file lists all durable storages avialable in EMQX
-ifndef(EMQX_DS_DBS_HRL).
-define(EMQX_DS_DBS_HRL, true).

%% `timers' DB stores information about postponed events
-define(DURABLE_TIMERS_DB, timers).

%% `messages' DB stores MQTT messages sent to durable sessions:
-define(PERSISTENT_MESSAGE_DB, messages).

%% `sessions' DB stores state of the durable sessions
-define(DURABLE_SESSION_STATE_DB, sessions).

%% `shared_subs' DB stores state of the shared sub leader processes
-define(SHARED_SUBS_DB, shared_subs).

-define(MQ_STATE_DB, mq_state).
-define(MQ_STATE_CONF_ROOT, mq_states).

-define(MQ_MESSAGE_LASTVALUE_DB, mq_message_lastvalue).
-define(MQ_MESSAGE_REGULAR_DB, mq_message_regular).
-define(MQ_MESSAGE_CONF_ROOT, mq_messages).

-define(STREAMS_MESSAGE_LASTVALUE_DB, streams_message_lastvalue).
-define(STREAMS_MESSAGE_REGULAR_DB, streams_message_regular).
-define(STREAMS_MESSAGE_CONF_ROOT, streams_messages).

-endif.
