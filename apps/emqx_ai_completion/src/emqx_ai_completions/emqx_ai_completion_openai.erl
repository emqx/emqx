%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_openai).

-behaviour(emqx_ai_completion).
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    call/3
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call(#{model := Model, system_prompt := SystemPrompt, provider := Provider}, Data, Options) ->
    Prompt = maps:get(prompt, Options, SystemPrompt),
    Client = create_client(Provider),
    Request = #{
        model => Model,
        messages => [
            #{role => <<"system">>, content => Prompt},
            #{role => <<"user">>, content => Data}
        ]
    },
    ?tp(warning, emqx_ai_completion_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post(Client, {chat, completions}, Request) of
        {ok, #{<<"choices">> := [#{<<"message">> := #{<<"content">> := Content}}]}} ->
            ?tp(warning, emqx_ai_completion_result, #{
                result => Content
            }),
            Content;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_error, #{
                reason => Reason
            }),
            <<"">>
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(#{api_key := ApiKey}) ->
    emqx_ai_completion_client:new(#{
        host => <<"api.openai.com">>,
        base_path => <<"/v1/">>,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"Authorization">>,
                emqx_secret:wrap(<<"Bearer ", (emqx_secret:unwrap(ApiKey))/binary>>)}
        ]
    }).
