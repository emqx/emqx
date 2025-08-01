%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_anthropic).

-behaviour(emqx_ai_completion).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    call/3,
    list_models/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call(
    #{
        name := Name,
        model := Model,
        system_prompt := SystemPrompt,
        max_tokens := MaxTokens,
        provider := #{name := ProviderName} = Provider,
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
                result => Result,
                provider => ProviderName,
                completion_profile => Name
            }),
            Result;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_error, #{
                reason => Reason,
                provider => ProviderName,
                completion_profile => Name
            }),
            <<"">>
    end.

list_models(#{}) -> [].

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(
    #{base_url := BaseUrl, api_key := ApiKey, transport_options := TransportOptions} = Provider,
    AnthropicVersion
) ->
    emqx_ai_completion_client:new(#{
        base_url => BaseUrl,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"x-api-key">>, ApiKey},
            {<<"anthropic-version">>, atom_to_binary(AnthropicVersion, utf8)}
        ],
        transport_options => TransportOptions,
        hackney_pool => emqx_ai_completion_provider:hackney_pool(Provider)
    }).
