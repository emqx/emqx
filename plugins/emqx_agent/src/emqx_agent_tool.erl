%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").

%% Behaviour definition
-callback init() -> ok.
-callback deinit() -> ok.
-callback create(Context :: map()) -> {ok, map()} | {error, term()}.
-callback destroy(Tool :: map()) -> ok.
-callback to_map(Tool :: map()) -> map().
-callback handle_invoke(Context :: map(), Request :: map()) -> {ok, map()} | {error, term()}.

-export([init/0, deinit/0, on_message_publish/1]).
-export([discover_tool_modules/0]).

-define(DEFAULT_INVOKE_TIMEOUT_MS, 30_000).

-spec init() -> ok.
init() ->
    lists:foreach(fun(Mod) -> Mod:init() end, discover_tool_modules()),
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    lists:foreach(fun(Mod) -> Mod:deinit() end, discover_tool_modules()),
    ok.

-spec discover_tool_modules() -> [module()].
discover_tool_modules() ->
    AllModules = [M || {M, _} <- code:all_loaded()],
    [M || M <- AllModules, is_tool_module(M)].

is_tool_module(Module) ->
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
        [TypeTool, ReqId] ->
            dispatch_type_tool(TypeTool, ReqId, Payload);
        _ ->
            ok
    end,
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

dispatch_type_tool(TypeTool, ReqId, Payload) ->
    case binary:split(TypeTool, <<"/">>, [global]) of
        [Type, ToolId] -> dispatch(Type, ToolId, ReqId, Payload);
        _ -> ok
    end.

dispatch(Type, ToolId, ReqId, Payload) ->
    DispatchResult =
        case emqx_agent_tool_registry:lookup(Type, ToolId) of
            {ok, #{module := Module} = Tool} ->
                Context = maps:get(context, Tool, #{}),
                case parse_payload(Payload, ReqId) of
                    {ok, Request} ->
                        Timeout = maps:get(<<"timeout_ms">>, Request, ?DEFAULT_INVOKE_TIMEOUT_MS),
                        case
                            emqx_agent_tool_invocation_sup:start_invocation(
                                Type, ToolId, Module, Context, Request, Timeout
                            )
                        of
                            {ok, _Pid} -> ok;
                            {error, _Reason} = Error -> Error
                        end;
                    {error, _Reason} = Error ->
                        Error
                end;
            {error, not_found} ->
                {error, tool_not_found}
        end,
    case DispatchResult of
        ok ->
            ok;
        {error, Reason} ->
            emqx_agent_tool_helpers:publish_reply(
                Type,
                ToolId,
                #{<<"req_id">> => ReqId},
                emqx_agent_tool_helpers:error_response(Reason)
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
