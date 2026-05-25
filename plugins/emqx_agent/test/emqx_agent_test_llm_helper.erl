%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_test_llm_helper).

-compile(export_all).
-compile(nowarn_export_all).

-define(OPENAI_BASE_URL, <<"https://api.openai.com/v1">>).
-define(OPENAI_MODEL, <<"gpt-5.4-mini">>).
-define(OPENROUTER_BASE_URL, <<"https://openrouter.ai/api/v1">>).
-define(OPENROUTER_KIMI_MODEL, <<"moonshotai/kimi-k2.5">>).

provider(Name) ->
    #{
        <<"name">> => Name,
        <<"type">> => <<"openai">>,
        <<"api_key">> => api_key(),
        <<"base_url">> => base_url(),
        <<"transport_options">> => #{<<"recv_timeout">> => <<"120s">>}
    }.

default_model() ->
    case provider_type() of
        openai -> ?OPENAI_MODEL;
        openrouter_kimi -> ?OPENROUTER_KIMI_MODEL
    end.

available() ->
    os:getenv(api_key_env()) =/= false.

skip_reason(SuiteName) ->
    lists:flatten(
        io_lib:format("~s not set - skipping ~s LLM integration tests", [api_key_env(), SuiteName])
    ).

base_url() ->
    case provider_type() of
        openai -> ?OPENAI_BASE_URL;
        openrouter_kimi -> ?OPENROUTER_BASE_URL
    end.

api_key_env() ->
    case provider_type() of
        openai -> "OPENAI_API_KEY";
        openrouter_kimi -> "OPENROUTER_API_KEY"
    end.

provider_type() ->
    case os:getenv("EMQX_AGENT_TEST_LLM_PROVIDER", "openai") of
        "openrouter-kimi" -> openrouter_kimi;
        _ -> openai
    end.

api_key() ->
    case os:getenv(api_key_env()) of
        false -> <<>>;
        Key -> list_to_binary(Key)
    end.
