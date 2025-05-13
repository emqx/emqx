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

call(
    #{
        name := Name,
        model := Model,
        system_prompt := SystemPrompt,
        provider := #{name := ProviderName} = Provider
    },
    Data,
    Options
) ->
    Prompt = maps:get(prompt, Options, SystemPrompt),
    Client = create_client(Provider),
    Request = #{
        model => Model,
        messages => [
            #{role => <<"system">>, content => Prompt},
            #{role => <<"user">>, content => Data}
        ]
    },
    ?tp(debug, emqx_ai_completion_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post(Client, {chat, completions}, Request) of
        {ok, #{<<"choices">> := [#{<<"message">> := #{<<"content">> := Content}}]}} ->
            ?tp(debug, emqx_ai_completion_result, #{
                result => Content,
                provider => ProviderName,
                completion_profile => Name
            }),
            Content;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_error, #{
                reason => Reason,
                provider => ProviderName,
                completion_profile => Name
            }),
            <<"">>
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(#{api_key := ApiKey, transport_options := TransportOptions}) ->
    emqx_ai_completion_client:new(#{
        host => <<"api.openai.com">>,
        base_path => <<"/v1/">>,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"Authorization">>,
                emqx_secret:wrap(<<"Bearer ", (emqx_secret:unwrap(ApiKey))/binary>>)}
        ],
        transport_options => TransportOptions
    }).
