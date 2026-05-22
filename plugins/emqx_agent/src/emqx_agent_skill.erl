%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").

%% Behaviour definition
-callback init() -> ok.
-callback deinit() -> ok.
-callback create(Context :: map()) -> {ok, map()} | {error, term()}.
-callback destroy(Skill :: map()) -> ok.
-callback to_map(Skill :: map()) -> map().
-callback handle_invoke(Context :: map(), Request :: map()) -> {ok, map()} | {error, term()}.

-export([init/0, deinit/0, on_message_publish/1]).
-export([discover_skill_modules/0]).

-define(DEFAULT_INVOKE_TIMEOUT_MS, 30_000).

-spec init() -> ok.
init() ->
    lists:foreach(fun(Mod) -> Mod:init() end, discover_skill_modules()),
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    lists:foreach(fun(Mod) -> Mod:deinit() end, discover_skill_modules()),
    ok.

-spec discover_skill_modules() -> [module()].
discover_skill_modules() ->
    AllModules = [M || {M, _} <- code:all_loaded()],
    [M || M <- AllModules, is_skill_module(M)].

is_skill_module(Module) ->
    case erlang:function_exported(Module, module_info, 1) of
        true ->
            Attrs = Module:module_info(attributes),
            Behaviours = proplists:get_value(behaviour, Attrs, []),
            lists:member(?MODULE, Behaviours);
        false ->
            false
    end.

on_message_publish(#message{topic = <<"$cap/", Rest/binary>>, payload = Payload} = Msg) ->
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
    DispatchResult =
        case emqx_agent_skill_registry:lookup(Type, SkillId) of
            {ok, #{module := Module} = Skill} ->
                Context = maps:get(context, Skill, #{}),
                case parse_payload(Payload, ReqId) of
                    {ok, Request} ->
                        Timeout = maps:get(<<"timeout_ms">>, Request, ?DEFAULT_INVOKE_TIMEOUT_MS),
                        case
                            emqx_agent_skill_invocation_sup:start_invocation(
                                Type, SkillId, Module, Context, Request, Timeout
                            )
                        of
                            {ok, _Pid} -> ok;
                            {error, _Reason} = Error -> Error
                        end;
                    {error, _Reason} = Error ->
                        Error
                end;
            {error, not_found} ->
                {error, skill_not_found}
        end,
    case DispatchResult of
        ok ->
            ok;
        {error, Reason} ->
            emqx_agent_skill_helpers:publish_reply(
                Type,
                SkillId,
                #{<<"req_id">> => ReqId},
                emqx_agent_skill_helpers:error_response(Reason)
            )
    end.

parse_payload(Payload, ReqId) ->
    try emqx_utils_json:decode(Payload) of
        Decoded when is_map(Decoded) ->
            {ok, Decoded#{<<"req_id">> => ReqId}};
        _ ->
            {error, payload_not_a_map}
    catch
        _:Reason -> {error, {invalid_payload, Reason}}
    end.
