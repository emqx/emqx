%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").

-export([init_hook/0, deinit_hook/0, on_message_publish/1]).

-spec init_hook() -> ok.
init_hook() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit_hook() -> ok.
deinit_hook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

on_message_publish(#message{topic = <<"cap/", Rest/binary>>, payload = Payload} = Msg) ->
    case binary:split(Rest, <<"/request/">>) of
        [TypeSkill, ReqId] ->
            dispatch_type_skill(TypeSkill, ReqId, Payload);
        _ ->
            ok
    end,
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

dispatch_type_skill(TypeSkill, ReqId, Payload) ->
    case binary:split(TypeSkill, <<"/">>, [global]) of
        [Type, SkillId] -> dispatch(Type, SkillId, ReqId, Payload);
        _ -> ok
    end.

dispatch(Type, SkillId, ReqId, Payload) ->
    case emqx_agent_skill_registry:lookup(Type, SkillId) of
        {ok, #{module := Module} = Skill} ->
            Context = maps:get(context, Skill, #{}),
            Request = emqx_utils_json:decode(Payload),
            case is_map(Request) of
                true ->
                    Request2 = maps:put(<<"req_id">>, ReqId, Request),
                    Timeout = maps:get(<<"timeout_ms">>, Request2, 30_000),
                    _ = emqx_agent_skill_invocation_sup:start_invocation(
                        Type, SkillId, Module, Context, Request2, Timeout
                    ),
                    ok;
                false ->
                    ok
            end;
        {error, not_found} ->
            ok
    end.
