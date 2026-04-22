%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_helpers).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([correlation/2, format_error/1, if_skill_request/3, publish_reply/4]).

%% Copies correlation fields from an invoke Request into a reply skeleton.
%%
%% Fields forwarded: req_id (required), trace_id, iid, sid (optional, default null).
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

%% Intercept a skill invocation on `cap/<Type>/<SkillId>/request`.
%% Calls Fun(SkillId, Msg) for side effects when the topic matches; always returns {ok, Msg}.
-spec if_skill_request(binary(), fun((binary(), #message{}) -> any()), #message{}) ->
    {ok, #message{}}.
if_skill_request(Type, Fun, #message{topic = Topic} = Msg) ->
    Prefix = <<"cap/", Type/binary, "/">>,
    PLen = byte_size(Prefix),
    case Topic of
        <<P:PLen/binary, Rest/binary>> when P =:= Prefix ->
            case binary:split(Rest, <<"/">>) of
                [SkillId, <<"request">>] -> Fun(SkillId, Msg);
                _ -> ok
            end;
        _ ->
            ok
    end,
    {ok, Msg}.

%% Build and publish a unary skill reply to `cap/<Type>/<SkillId>/response/<ReqId>`.
-spec publish_reply(binary(), binary(), map(), map()) -> ok.
publish_reply(Type, SkillId, Request, Data) ->
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = correlation(Request, #{
        <<"skill">> => #{<<"type">> => Type, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Data
    }),
    ReplyTopic = <<"cap/", Type/binary, "/", SkillId/binary, "/response/", ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.

-spec correlation(Request :: map(), Extra :: map()) -> map().
correlation(Request, Extra) ->
    maps:merge(
        #{
            <<"req_id">> => maps:get(<<"req_id">>, Request),
            <<"trace_id">> => maps:get(<<"trace_id">>, Request, null),
            <<"iid">> => maps:get(<<"iid">>, Request, null),
            <<"sid">> => maps:get(<<"sid">>, Request, null)
        },
        Extra
    ).
