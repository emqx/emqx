%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_openai).

-behaviour(emqx_ai_completion).
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    call_completion/3,
    list_models/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call_completion(
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
        <<"model">> => Model,
        <<"input">> => Data,
        <<"instructions">> => Prompt
    },
    ?tp(debug, emqx_ai_completion_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post(Client, responses, Request) of
        {ok, #{
            <<"status">> := <<"completed">>,
            <<"output">> := [
                #{
                    <<"type">> := <<"message">>,
                    <<"status">> := <<"completed">>,
                    <<"content">> := [#{<<"type">> := <<"output_text">>, <<"text">> := Output} | _]
                }
                | _
            ]
        }} ->
            ?tp(debug, emqx_ai_completion_result, #{
                result => Output,
                provider => ProviderName,
                completion_profile => Name
            }),
            Output;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_error, #{
                reason => Reason,
                provider => ProviderName,
                completion_profile => Name
            }),
            <<"">>
    end.

list_models(Provider) ->
    Client = create_client(Provider),
    case emqx_ai_completion_client:api_get(Client, models) of
        {ok, #{<<"data">> := Models}} when is_list(Models) ->
            ModelIds = [Model || #{<<"id">> := Model} <- Models],
            {ok, ModelIds};
        {ok, Other} ->
            {error, {cannot_list_models, {unexpected_response, Other}}};
        {error, Reason} ->
            {error, {cannot_list_models, Reason}}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(
    #{base_url := BaseUrl, api_key := ApiKey, transport_options := TransportOptions} = Provider
) ->
    emqx_ai_completion_client:new(#{
        base_url => BaseUrl,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"Authorization">>,
                emqx_secret:wrap(<<"Bearer ", (emqx_secret:unwrap(ApiKey))/binary>>)}
        ],
        transport_options => TransportOptions,
        hackney_pool => emqx_ai_completion_provider:hackney_pool(Provider)
    }).
