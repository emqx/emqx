%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    rsf_ai_completion/1
]).

%%------------------------------------------------------------------------------
%% Callbacks & types
%%------------------------------------------------------------------------------

-type completion_profile() :: emqx_ai_completion_config:completion_profile().
-type prompt() :: binary().
-type options() :: #{
    prompt => prompt()
}.
-type data() :: binary().

-callback call(completion_profile(), data(), options()) -> binary().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

rsf_ai_completion([Name, Prompt, Data]) ->
    call_completion(Name, Data, #{prompt => Prompt});
rsf_ai_completion([Name, Data]) ->
    call_completion(Name, Data, #{});
rsf_ai_completion(Args) ->
    error({args_count_error, {ai_completion, Args}}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

call_completion(Name, Data, Options) ->
    {Module, CompletionProfile} = completion_profile(Name),
    Module:call(CompletionProfile, Data, Options).

completion_profile(Name) ->
    case emqx_ai_completion_config:get_completion_profile(Name) of
        {ok, #{type := Type} = CompletionProfile} ->
            {completion_module(Type), CompletionProfile};
        not_found ->
            error({completion_profile_not_found, Name})
    end.

completion_module(openai) ->
    emqx_ai_completion_openai;
completion_module(anthropic) ->
    emqx_ai_completion_anthropic.
