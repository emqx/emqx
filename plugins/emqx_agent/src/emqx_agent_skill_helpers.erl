%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_helpers).

-include_lib("emqx/include/emqx_mqtt.hrl").

-export([cap_response/1, format_error/1, publish_reply/4, error_response/1]).

%% Copies correlation fields from an invoke Request into a reply skeleton.
%%
%% Fields forwarded: req_id (required), trace_id, iid, sid (optional, default undefined).
%% The caller merges the returned map with skill-specific fields.
%% Converts a service-layer error term to a human-readable binary for the LLM.
-spec format_error(term()) -> binary().
format_error(#{field_name := F, reason := R}) ->
    iolist_to_binary(io_lib:format("invalid field '~s': ~p", [F, R]));
format_error(#{field_name := F, expected := E}) ->
    iolist_to_binary(io_lib:format("unknown value for '~s', expected one of: ~p", [F, E]));
format_error({missing_field, F}) when is_binary(F) ->
    <<"missing required field: ", F/binary>>;
format_error({missing_field, F}) when is_atom(F) ->
    <<"missing required field: ", (atom_to_binary(F, utf8))/binary>>;
format_error(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

error_response(Reason) ->
    #{<<"status">> => <<"error">>, <<"reason">> => format_error(Reason)}.

%% Build and publish a unary skill reply to `cap/<Type>/<SkillId>/response/<ReqId>`.
-spec publish_reply(binary(), binary(), map(), map()) -> ok.
publish_reply(Type, SkillId, Request, Data) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = correlation(
        Request,
        #{
            <<"skill">> => #{<<"type">> => Type, <<"id">> => SkillId},
            <<"response">> => Data
        }
    ),
    ReplyTopic = <<"cap/", Type/binary, "/", SkillId/binary, "/response/", ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.

-spec cap_response(map()) -> map().
cap_response(Frame) ->
    maps:get(<<"response">>, Frame, #{}).

-spec correlation(_Request :: map(), _Reply :: map()) -> map().
correlation(Request, Reply) ->
    maps:merge(
        #{
            <<"req_id">> => maps:get(<<"req_id">>, Request),
            <<"trace_id">> => maps:get(<<"trace_id">>, Request, undefined),
            <<"iid">> => maps:get(<<"iid">>, Request, undefined),
            <<"sid">> => maps:get(<<"sid">>, Request, undefined)
        },
        Reply
    ).
