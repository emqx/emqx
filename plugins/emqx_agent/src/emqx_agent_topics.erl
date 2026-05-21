%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_topics).

-include("emqx_agent_pipeline.hrl").

-export([
    evt_topic/1,
    sess_in_topic/1,
    sess_out_topic/1,
    cap_request_topic/3,
    cap_response_topic/3,
    cap_tmp_response_prefix/0,
    pipe_events_topic/2,
    req_id_from_cap_response_topic/1
]).

evt_topic(Suffix) ->
    <<?AGENT_EVT_PREFIX/binary, Suffix/binary>>.

sess_in_topic(Sid) ->
    <<?AGENT_SESS_IN_PREFIX/binary, Sid/binary, "/">>.

sess_out_topic(Sid) ->
    <<?AGENT_SESS_OUT_PREFIX/binary, Sid/binary, "/">>.

cap_request_topic(Type, SkillId, ReqId) ->
    <<?AGENT_CAP_PREFIX/binary, Type/binary, "/", SkillId/binary, "/request/", ReqId/binary>>.

cap_response_topic(Type, SkillId, ReqId) ->
    <<?AGENT_CAP_PREFIX/binary, Type/binary, "/", SkillId/binary, "/response/", ReqId/binary>>.

cap_tmp_response_prefix() ->
    <<?AGENT_CAP_PREFIX/binary, "tmp/response/">>.

pipe_events_topic(PipelineId, Iid) ->
    <<?AGENT_PIPE_PREFIX/binary, PipelineId/binary, "/inst/", Iid/binary, "/events">>.

req_id_from_cap_response_topic(Topic) ->
    case binary:split(Topic, <<"/response/">>) of
        [_TypeSkill, ReqId] -> ReqId;
        _ -> undefined
    end.
