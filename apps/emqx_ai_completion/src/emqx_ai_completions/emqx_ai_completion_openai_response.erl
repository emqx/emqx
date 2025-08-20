%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_openai_response).

-behaviour(emqx_ai_completion).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([call_completion/3, list_models/1]).

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
    Request =
        #{
            <<"model">> => Model,
            <<"input">> => Data,
            <<"instructions">> => Prompt
        },
    ?tp(debug, emqx_ai_completion_request, #{request => Request}),
    case emqx_ai_completion_client:api_post(Client, <<"responses">>, Request) of
        {ok, #{<<"status">> := <<"completed">>, <<"output">> := Output}} ->
            Content = take_content(Output),
            ?tp(
                debug,
                emqx_ai_completion_result,
                #{
                    result => Content,
                    provider => ProviderName,
                    completion_profile => Name
                }
            ),
            Content;
        {error, Reason} ->
            ?tp(
                error,
                emqx_ai_completion_error,
                #{
                    reason => Reason,
                    provider => ProviderName,
                    completion_profile => Name
                }
            ),
            <<"">>
    end.

list_models(Provider) ->
    emqx_ai_completion_openai:list_models(Provider).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(Provider) ->
    emqx_ai_completion_openai:create_client(Provider).

%% Content is like:
%%
%%     [#{<<"id">> => <<"rs_689c31cac8348195b1cff9d8f1d3a7b50d623e416147c0e2">>,
%%     <<"summary">> => [],
%%     <<"type">> => <<"reasoning">>},
%%   #{<<"content">> =>
%%         [#{<<"annotations">> => [],
%%            <<"logprobs">> => [],
%%            <<"text">> => <<"3">>,
%%            <<"type">> => <<"output_text">>}],
%%     <<"id">> => <<"msg_689c31cca6648195a2bef2be2bfbbe5e0d623e416147c0e2">>,
%%     <<"role">> => <<"assistant">>,
%%     <<"status">> => <<"completed">>,
%%     <<"type">> => <<"message">>}],
%%
%% See https://platform.openai.com/docs/api-reference/responses/object#responses/object-output

take_content(Output) ->
    Texts = [
        Text
     || #{
            <<"content">> := Content,
            <<"role">> := <<"assistant">>,
            <<"type">> := <<"message">>
        } <-
            Output,
        #{<<"text">> := Text, <<"type">> := <<"output_text">>} <- Content
    ],
    iolist_to_binary(Texts).
