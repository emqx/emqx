%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%% @noformat

-ifndef(SHARED_SUBS_AGENT_HRL).
-define(SHARED_SUBS_AGENT_HRL, true).

-ifdef(EMQX_RELEASE_EDITION).
  -if(?EMQX_RELEASE_EDITION == ee).
    -define(shared_subs_agent, emqx_ds_shared_sub_agent).
  -else.
    -define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).
  -endif.
-else.
  -define(shared_subs_agent, emqx_persistent_session_ds_shared_subs_null_agent).
-endif.

-endif.
