%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_rule_sql).

%% NOTE
%% rsf_ stands for Rule SQL Function
-export([
    rsf_ai_completion/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec rsf_ai_completion([binary()]) -> binary().
rsf_ai_completion([Name, Prompt, Data]) ->
    case emqx_ai_completion:call_completion(Name, Data, #{prompt => Prompt}) of
        {ok, Result} ->
            Result;
        {error, Reason} ->
            error({ai_completion_error, Reason})
    end;
rsf_ai_completion([Name, Data]) ->
    case emqx_ai_completion:call_completion(Name, Data, #{}) of
        {ok, Result} ->
            Result;
        {error, Reason} ->
            error({ai_completion_error, Reason})
    end;
rsf_ai_completion(Args) ->
    error({args_count_error, {ai_completion, Args}}).
