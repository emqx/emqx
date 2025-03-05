%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion).

-feature(maybe_expr, enable).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-export_type([
    state/0,
    options/0,
    completion_name/0,
    prompt/0,
    data/0
]).

-export([
    rsf_ai_completion/1
]).

%%------------------------------------------------------------------------------
%% Callbacks & types
%%------------------------------------------------------------------------------

-type state() :: term().
-type options() :: map().

-type completion_name() :: binary().
-type prompt() :: binary().
-type data() :: binary().

-callback call_completion(state(), prompt(), data()) -> binary().
-callback create(options()) -> state().
-callback update_options(state(), options()) -> state().
-callback destroy(state()) -> ok.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

rsf_ai_completion([Name, Prompt, Data]) ->
    ?tp(warning, ai_completion_call, #{name => Name, prompt => Prompt, data => Data}),
    emqx_ai_completion_registry:call(Name, Prompt, Data);
rsf_ai_completion([Name, Data]) ->
    ?tp(warning, ai_completion_call, #{name => Name, data => Data}),
    emqx_ai_completion_registry:call(Name, Data);
rsf_ai_completion(_Args) ->
    ?tp(warning, ai_completion_call_error, #{args => _Args}),
    error({args_count_error, {ai_completion, _Args}}).
