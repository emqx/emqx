%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_test_helpers).

-export([
    clean_completion_profiles/0,
    clean_providers/0,
    mock_ai_client/1,
    unmock_ai_client/0
]).

clean_completion_profiles() ->
    ok = lists:foreach(
        fun(#{<<"name">> := Name}) ->
            ok = emqx_ai_completion_config:update_completion_profiles_raw({delete, Name})
        end,
        emqx_ai_completion_config:get_completion_profiles_raw()
    ).

clean_providers() ->
    ok = lists:foreach(
        fun(#{<<"name">> := Name}) ->
            ok = emqx_ai_completion_config:update_providers_raw({delete, Name})
        end,
        emqx_ai_completion_config:get_providers_raw()
    ).

mock_ai_client(AICompletion) ->
    meck:new(emqx_ai_completion_client, [passthrough]),

    meck:expect(
        emqx_ai_completion_client,
        api_post,
        fun
            %% OpenAI
            (_St, {chat, completions}, _Request) ->
                {ok, #{<<"choices">> => [#{<<"message">> => #{<<"content">> => AICompletion}}]}};
            %% Anthropic
            (_St, messages, _Request) ->
                {ok, #{<<"content">> => [#{<<"type">> => <<"text">>, <<"text">> => AICompletion}]}}
        end
    ).

unmock_ai_client() ->
    meck:unload(emqx_ai_completion_client).
