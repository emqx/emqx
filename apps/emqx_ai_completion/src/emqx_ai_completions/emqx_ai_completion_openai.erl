%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_openai).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").


-export([
    create/1,
    update_options/2,
    destroy/1,
    call_completion/3
]).

-type state() :: #{
    model := binary()
}.

-spec create(map()) -> state().
create(#{model := Model}) ->
    #{model => Model}.

update_options(State, #{model := Model}) ->
    State#{model => Model}.

destroy(State) ->
    State.

call_completion(#{model := Model}, Prompt, Data) ->
    Request = #{
        model => Model,
        messages => [
            #{role => <<"system">>, content => Prompt},
            #{role => <<"user">>, content => Data}
        ]
    },
    ?tp(warning, emqx_ai_completion_on_message_publish_request, #{
        request => Request
    }),
    case emqx_ai_completion_client:api_post({chat, completions}, Request) of
        {ok, #{<<"choices">> := [#{<<"message">> := #{<<"content">> := Content}}]}} ->
            ?tp(warning, emqx_ai_completion_on_message_publish_result, #{
                result => Content
            }),
            Content;
        {error, Reason} ->
            ?tp(error, emqx_ai_completion_on_message_publish_error, #{
                reason => Reason
            }),
            <<"">>
    end.
