%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% OTP messages exchanged between emqx_agent_pipeline_mgr (sender)
%% and emqx_agent_pipeline instances (receiver).  All are delivered
%% as gen_statem casts.

-define(AGENT_EVT_PREFIX, <<"$evt/">>).
-define(AGENT_SESS_IN_PREFIX, <<"$sess/in/">>).
-define(AGENT_SESS_OUT_PREFIX, <<"$sess/out/">>).
-define(AGENT_CAP_PREFIX, <<"$cap/">>).
-define(AGENT_PIPE_PREFIX, <<"$pipe/">>).

%% A frame published by an LLM session on $sess/out/<sid>/.
-record(sess_frame, {
    sid :: binary(),
    frame :: map()
}).

%% A skill reply published on $cap/reply/<req_id>.
-record(cap_reply, {
    req_id :: binary(),
    frame :: map()
}).
