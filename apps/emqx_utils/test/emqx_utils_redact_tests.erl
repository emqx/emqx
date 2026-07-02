%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_utils_redact_tests).

-include_lib("eunit/include/eunit.hrl").

is_redacted_test_() ->
    [
        ?_assertNot(is_redacted(password, <<"secretpass">>)),
        ?_assertNot(is_redacted(password, <<>>)),
        ?_assertNot(is_redacted(password, undefined)),
        ?_assert(is_redacted(password, <<"******">>)),
        ?_assertNot(is_redacted(password, fun() -> <<"secretpass">> end)),
        ?_assertNot(is_redacted(password, emqx_secret:wrap(<<"secretpass">>))),
        ?_assert(is_redacted(password, fun() -> <<"******">> end)),
        ?_assert(is_redacted(password, emqx_secret:wrap(<<"******">>)))
    ].

no_redact_template_var_test() ->
    ?assertEqual(
        #{
            password => <<"${var}">>,
            account_key => <<"${path.to.var}">>,
            <<"secret">> => <<"******">>,
            private_key => <<"******">>
        },
        redact(#{
            password => <<"${var}">>,
            <<"secret">> => <<"abc">>,
            account_key => <<"${path.to.var}">>,
            private_key => <<"${var}more">>
        })
    ).

no_redact_file_paths_test() ->
    ?assertEqual(
        #{
            password => <<"file:///abs/path/a">>,
            <<"secret">> => <<"file://relative/path/b">>,
            account_key => "file://string/path/x"
        },
        redact(#{
            password => <<"file:///abs/path/a">>,
            <<"secret">> => <<"file://relative/path/b">>,
            account_key => "file://string/path/x"
        })
    ).

no_redact_wrapped_file_paths_test() ->
    ?assertEqual(
        #{password => <<"file:///abs/path/a">>},
        redact(#{
            password => emqx_secret:wrap_load({file, <<"file:///abs/path/a">>})
        })
    ).

redact_wrapped_secret_test() ->
    ?assertEqual(
        #{password => <<"******">>},
        redact(#{
            password => emqx_secret:wrap(<<"aaa">>)
        })
    ).

redact_sentinel_password_test() ->
    ?assertEqual(
        #{sentinel_password => <<"******">>},
        redact(#{sentinel_password => <<"sentinel-password">>})
    ).

redact_common_token_aliases_test() ->
    ?assertEqual(
        #{
            access_token => <<"******">>,
            client_jwks => "******",
            <<"refresh_token">> => <<"******">>,
            "id_token" => "******"
        },
        redact(#{
            access_token => <<"access-token">>,
            client_jwks => #{type => file, file => <<"private-jwk">>},
            <<"refresh_token">> => <<"refresh-token">>,
            "id_token" => "id-token"
        })
    ).

redact_secret_headers_test() ->
    ?assertEqual(
        #{
            headers => #{
                "X-API-Key" => "******",
                <<"API-Key">> => <<"******">>,
                cookie => "******"
            }
        },
        redact(#{
            headers => #{
                "X-API-Key" => "api-key",
                <<"API-Key">> => <<"api-key">>,
                cookie => "emqx_auth=token"
            }
        })
    ).

redact_iolist_header_keys_test_() ->
    %% Header keys stored as iolists (a shape produced by template parsers) must
    %% still be recognised as sensitive.
    Secret = <<"abcd">>,
    Redacted = <<"******">>,
    Wrap = fun(KeyT, Value) ->
        redact(#{headers => [{KeyT, Value}]})
    end,
    [
        %% binary key (regression, already worked)
        ?_assertEqual(
            #{headers => [{<<"x-api-key">>, Redacted}]},
            Wrap(<<"x-api-key">>, Secret)
        ),
        %% iolist single-binary key (the previously failing case)
        ?_assertEqual(
            #{headers => [{[<<"x-api-key">>], Redacted}]},
            Wrap([<<"x-api-key">>], Secret)
        ),
        %% iolist multi-fragment key
        ?_assertEqual(
            #{headers => [{[<<"x-">>, <<"api-key">>], Redacted}]},
            Wrap([<<"x-">>, <<"api-key">>], Secret)
        ),
        %% mixed-case iolist key
        ?_assertEqual(
            #{headers => [{[<<"X-API-Key">>], Redacted}]},
            Wrap([<<"X-API-Key">>], Secret)
        ),
        %% non-sensitive header untouched
        ?_assertEqual(
            #{headers => [{[<<"content-type">>], <<"application/json">>}]},
            Wrap([<<"content-type">>], <<"application/json">>)
        )
    ].

redact_dashboard_secret_fields_test() ->
    ?assertEqual(
        #{
            <<"old_pwd">> => <<"******">>,
            new_pwd => <<"******">>,
            "mfa_token" => "******"
        },
        redact(#{
            <<"old_pwd">> => <<"old-password">>,
            new_pwd => <<"new-password">>,
            "mfa_token" => "mfa-token"
        })
    ).
deobfuscate_file_path_secrets_test_() ->
    Original1 = #{foo => #{bar => #{headers => #{"authorization" => "file://a"}}}},
    Original2 = #{foo => #{bar => #{headers => #{"authorization" => "a"}}}},
    Redacted2 = #{foo => #{bar => #{headers => #{"authorization" => "******"}}}},
    [
        ?_assertEqual(Original1, redact(Original1)),
        ?_assertEqual(Original1, emqx_utils_redact:deobfuscate(Original1, Original1)),
        ?_assertEqual(Redacted2, redact(Original2)),
        ?_assertEqual(Original2, emqx_utils_redact:deobfuscate(Redacted2, Original2))
    ].

redact(X) -> emqx_utils:redact(X).

is_redacted(Key, Value) ->
    emqx_utils:is_redacted(Key, Value).
