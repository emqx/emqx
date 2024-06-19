%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(SHARED_SUBS_AGENT_HRL).
-define(SHARED_SUBS_AGENT_HRL, true).

-ifdef(EMQX_RELEASE_EDITION).

-if(?EMQX_RELEASE_EDITION == ee).

%% agent from BSL app

-ifdef(TEST).
-define(shared_subs_agent, emqx_ds_shared_sub_agent).
-else.
%% Till full implementation we need to dispach to the null agent.
%% It will report "not implemented" error for attempts to use shared subscriptions.
-define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).
%% -ifdef(TEST).
-endif.

%% -if(?EMQX_RELEASE_EDITION == ee).
-else.

-define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).

%% -if(?EMQX_RELEASE_EDITION == ee).
-endif.

%% -ifdef(EMQX_RELEASE_EDITION).
-else.

-define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).

%% -ifdef(EMQX_RELEASE_EDITION).
-endif.

-endif.
