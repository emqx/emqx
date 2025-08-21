%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_anthropic).

-behaviour(emqx_ai_completion).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    call_completion/3,
    list_models/1
]).

-define(LIST_MODELS_LIMIT, 100).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call_completion(
    #{
        name := Name,
        model := Model,
        system_prompt := SystemPrompt,
        max_tokens := MaxTokens,
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

list_models(Provider) ->
    Client = create_client(Provider),
    list_models(Client, [], undefined).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

list_models(Client, ModelsAcc, AfterId) ->
    QS = list_models_qs(AfterId),
    case emqx_ai_completion_client:api_get(Client, <<"models">>, QS) of
        {ok, #{<<"data">> := Models, <<"has_more">> := HasMore, <<"last_id">> := LastId}} when
            is_list(Models) andalso is_boolean(HasMore) andalso is_binary(LastId)
        ->
            ModelIds = [Model || #{<<"id">> := Model} <- Models],
            case HasMore of
                true ->
                    list_models(Client, [ModelIds | ModelsAcc], LastId);
                false ->
                    {ok, lists:append(lists:reverse([ModelIds | ModelsAcc]))}
            end;
        {ok, Other} ->
            {error, {cannot_list_models, {unexpected_response, Other}}};
        {error, Reason} ->
            {error, {cannot_list_models, Reason}}
    end.

list_models_qs(undefined) ->
    #{<<"limit">> => ?LIST_MODELS_LIMIT};
list_models_qs(AfterId) ->
    #{<<"limit">> => ?LIST_MODELS_LIMIT, <<"after_id">> => AfterId}.

create_client(
    #{
        base_url := BaseUrl,
        api_key := ApiKey,
        transport_options := TransportOptions,
        anthropic_version := AnthropicVersion
    } = Provider
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
