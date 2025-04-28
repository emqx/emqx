%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_anthropic).

-behaviour(emqx_ai_completion).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    call/3
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call(
    #{
        model := Model,
        system_prompt := SystemPrompt,
        max_tokens := MaxTokens,
        provider := Provider,
        anthropic_version := AnthropicVersion
    },
    Data,
    Options
) ->
    Prompt = maps:get(prompt, Options, SystemPrompt),
    Client = create_client(Provider, AnthropicVersion),
    Request = #{
        model => Model,
        messages => [
            #{role => <<"user">>, content => Data}
        ],
        max_tokens => MaxTokens,
        system => Prompt
    },
    ?tp(debug, emqx_ai_completion_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post(Client, messages, Request) of
        {ok, #{<<"content">> := [#{<<"type">> := <<"text">>, <<"text">> := Result} | _]}} ->
            ?tp(debug, emqx_ai_completion_result, #{
                result => Result
            }),
            Result;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_error, #{
                reason => Reason
            }),
            <<"">>
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(#{api_key := ApiKey, transport_options := TransportOptions}, AnthropicVersion) ->
    emqx_ai_completion_client:new(#{
        host => <<"api.anthropic.com">>,
        base_path => <<"/v1/">>,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"x-api-key">>, ApiKey},
            {<<"anthropic-version">>, atom_to_binary(AnthropicVersion, utf8)}
        ],
        transport_options => TransportOptions
    }).
