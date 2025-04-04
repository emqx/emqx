%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_anthropic).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    create/1,
    update_options/2,
    destroy/1,
    call_completion/3
]).

-type state() :: #{
    model := binary(),
    client := emqx_ai_completion_client:t()
}.

-spec create(map()) -> state().
create(#{model := Model, api_key := ApiKey, anthropic_version := AnthropicVersion}) ->
    #{model => Model, client => create_client(ApiKey, AnthropicVersion)}.

update_options(State, #{model := Model}) ->
    State#{model => Model}.

destroy(State) ->
    State.

call_completion(#{model := Model, client := Client}, Prompt, Data) ->
    Request = #{
        model => Model,
        messages => [
            #{role => <<"user">>, content => Data}
        ],
        max_tokens => 1000,
        system => Prompt
    },
    ?tp(warning, emqx_ai_completion_on_message_publish_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post(Client, messages, Request) of
        {ok, #{<<"content">> := [#{<<"type">> := <<"text">>, <<"text">> := Result} | _]}} ->
            ?tp(warning, emqx_ai_completion_on_message_publish_result, #{
                result => Result
            }),
            Result;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_on_message_publish_error, #{
                reason => Reason
            }),
            <<"">>
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_client(ApiKey, AnthropicVersion) ->
    emqx_ai_completion_client:new(#{
        host => <<"api.anthropic.com">>,
        base_path => <<"/v1/">>,
        headers => [
            {<<"Content-Type">>, <<"application/json">>},
            {<<"x-api-key">>, fun() -> ApiKey end},
            {<<"anthropic-version">>, AnthropicVersion}
        ]
    }).
