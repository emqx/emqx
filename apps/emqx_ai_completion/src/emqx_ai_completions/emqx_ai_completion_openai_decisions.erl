%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_openai_decisions).

-behaviour(emqx_ai_completion).

-include_lib("snabbkaffe/include/trace.hrl").

-export([call_completion/3, list_models/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

call_completion(_Profile, _Data, #{prompt := _}) ->
    error({ai_completion_error, {unsupported_option, prompt}});
call_completion(
    #{
        name := Name,
        model := Model,
        questions := Questions,
        provider := #{name := ProviderName} = Provider
    },
    Data,
    _Options
) ->
    Client = emqx_ai_completion_openai:create_client(Provider),
    Request = #{
        <<"model">> => Model,
        <<"input">> => Data,
        <<"questions">> => Questions
    },
    ?tp(debug, emqx_ai_completion_request, #{request => Request}),
    Result = emqx_ai_completion_client:api_post(Client, <<"decisions">>, Request),
    case take_answers(Result) of
        {ok, Answers} ->
            Content = emqx_utils_json:encode(#{<<"answers">> => Answers}),
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
            <<>>
    end.

list_models(Provider) ->
    emqx_ai_completion_openai:list_models(Provider).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

take_answers({ok, #{<<"answers">> := Answers}}) when is_list(Answers) ->
    {ok, Answers};
take_answers({ok, Other}) ->
    {error, {unexpected_response, Other}};
take_answers(ok) ->
    {error, {unexpected_response, empty_body}};
take_answers({error, _} = Error) ->
    Error.
