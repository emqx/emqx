%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request/2,
        request/3,
        uri/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            {emqx_ai_completion, #{config => "ai.credentials = [], ai.completion_profiles = []"}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_ai_completion_test_helpers:clean_completion_profiles(),
    emqx_ai_completion_test_helpers:clean_credentials(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_ai_completion_test_helpers:clean_completion_profiles(),
    emqx_ai_completion_test_helpers:clean_credentials().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    %% Fail to create invalid credentials
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, credentials], #{<<"foo">> => <<"bar">>})
    ),

    %% Create valid credentials
    ?assertMatch(
        {ok, 204},
        api_post([ai, credentials], #{
            name => <<"test-credential">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>
        })
    ),

    %% Fail to create credential with duplicate name
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, credentials], #{
            name => <<"test-credential">>,
            type => <<"openai">>,
            api_key => <<"test-api-key">>
        })
    ),

    %% Succeed to fetch credentials
    ?assertMatch(
        {ok, 200, [
            #{<<"name">> := <<"test-credential">>, <<"type">> := <<"openai">>}
        ]},
        api_get([ai, credentials])
    ),

    %% Fail to create invalid completion profile
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{<<"foo">> => <<"bar">>})
    ),

    %% Fail to create with invalid credential
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            credential_name => <<"non-existent-credential">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Fail to create with mismatching credential type
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"anthropic">>,
            credential_name => <<"test-credential">>
        })
    ),

    %% Create valid completion profile
    ?assertMatch(
        {ok, 204},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            credential_name => <<"test-credential">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Fail to create completion profile with duplicate name
    ?assertMatch(
        {ok, 400, _},
        api_post([ai, completion_profiles], #{
            name => <<"test-completion-profile">>,
            type => <<"openai">>,
            credential_name => <<"test-credential">>,
            model => <<"gpt-4o">>
        })
    ),

    %% Succeed to fetch completion profiles
    ?assertMatch(
        {ok, 200, [
            #{<<"name">> := <<"test-completion-profile">>, <<"type">> := <<"openai">>}
        ]},
        api_get([ai, completion_profiles])
    ),

    %% Fail to update credential type of the used credential
    ?assertMatch(
        {ok, 400, _},
        api_put([ai, credentials, <<"test-credential">>], #{
            type => <<"anthropic">>,
            api_key => <<"test-api-key">>
        })
    ),

    %% Fail to delete the used credential
    ?assertMatch(
        {ok, 400, _},
        api_delete([ai, credentials, <<"test-credential">>])
    ),

    %% Succeed to update the used credential
    ?assertMatch(
        {ok, 204},
        api_put([ai, credentials, <<"test-credential">>], #{
            type => <<"openai">>,
            api_key => <<"new-test-api-key">>
        })
    ),

    %% Fail to change completion profile type
    ?assertMatch(
        {ok, 400, _},
        api_put([ai, completion_profiles, <<"test-completion-profile">>], #{
            type => <<"anthropic">>,
            credential_name => <<"test-credential">>
        })
    ),

    %% Fail to delete unknown completion profile
    ?assertMatch(
        {ok, 404, _},
        api_delete([ai, completion_profiles, <<"unknown-completion-profile">>])
    ),

    %% Succeed to update completion profile
    ?assertMatch(
        {ok, 204},
        api_put([ai, completion_profiles, <<"test-completion-profile">>], #{
            type => <<"openai">>,
            credential_name => <<"test-credential">>,
            model => <<"gpt-4o-mini">>
        })
    ),

    %% Succeed to delete the completion profile
    ?assertMatch(
        {ok, 204},
        api_delete([ai, completion_profiles, <<"test-completion-profile">>])
    ),

    %% Fail to delete unknown credential
    ?assertMatch(
        {ok, 404, _},
        api_delete([ai, credentials, <<"unknown-credential">>])
    ),

    %% Succeed to delete the credential
    ?assertMatch(
        {ok, 204},
        api_delete([ai, credentials, <<"test-credential">>])
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    decode_body(request(get, uri(Path))).

api_post(Path, Data) ->
    decode_body(request(post, uri(Path), Data)).

api_put(Path, Data) ->
    decode_body(request(put, uri(Path), Data)).

api_delete(Path) ->
    decode_body(request(delete, uri(Path))).

decode_body(Response) ->
    ct:pal("Response: ~p", [Response]),
    do_decode_body(Response).

do_decode_body({ok, Code, <<>>}) ->
    {ok, Code};
do_decode_body({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} ->
            {ok, Code, Decoded};
        {error, _} = Error ->
            ct:pal("Invalid body: ~p", [Body]),
            Error
    end;
do_decode_body(Error) ->
    Error.
