%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    call_completion/3,
    list_models/1
]).

%%------------------------------------------------------------------------------
%% Callbacks & types
%%------------------------------------------------------------------------------

-type completion_profile() :: emqx_ai_completion_config:completion_profile().
-type provider() :: emqx_ai_completion_config:provider().
-type model() :: emqx_ai_completion_config:model().
-type prompt() :: binary().
-type options() :: #{
    prompt => prompt()
}.
-type data() :: binary().

-callback call_completion(completion_profile(), data(), options()) -> binary().
-callback list_models(provider()) -> {ok, list(model())} | {error, term()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec call_completion(binary(), data(), options()) -> {ok, binary()} | {error, term()}.
call_completion(Name, Data, Options) ->
    maybe
        {ok, Module, CompletionProfile} ?= completion_profile(Name),
        {ok, Module:call_completion(CompletionProfile, Data, Options)}
    end.

-spec list_models(binary() | provider()) ->
    {ok, list(model())} | {error, provider_not_found | term()}.
list_models(Name) when is_binary(Name) ->
    maybe
        {ok, Module, Provider} ?= provider(Name),
        Module:list_models(Provider)
    end;
list_models(#{type := Type} = Provider) ->
    Module = completion_module(Type),
    Module:list_models(Provider).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

completion_profile(Name) ->
    case emqx_ai_completion_config:get_completion_profile(Name) of
        {ok, #{type := Type} = CompletionProfile} ->
            {ok, completion_module(Type), CompletionProfile};
        not_found ->
            {error, {completion_profile_not_found, Name}}
    end.

provider(Name) ->
    case emqx_ai_completion_config:get_provider(Name) of
        {ok, #{type := Type} = Provider} ->
            {ok, completion_module(Type), Provider};
        not_found ->
            {error, provider_not_found}
    end.

completion_module(openai) ->
    emqx_ai_completion_openai;
completion_module(anthropic) ->
    emqx_ai_completion_anthropic.
