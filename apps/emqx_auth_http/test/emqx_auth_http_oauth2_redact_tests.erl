%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_auth_http_oauth2_redact_tests).

-include_lib("eunit/include/eunit.hrl").

%% @doc Reproduce the serialization chain used by the config GET handlers
%% (`emqx_authn_api:get_raw_config_with_defaults/1' /
%% `emqx_authz_api_sources:get_raw_sources/0') up to the redaction step, and
%% assert that `oauth2.client_secret' never leaves the API in plaintext while
%% the serialized (pre-redaction) config still holds the real value.
authn_http_oauth2_client_secret_is_masked_test() ->
    ok = inject_schemas(),
    Conf = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"http">>,
        <<"method">> => <<"post">>,
        <<"url">> => <<"http://127.0.0.1:1/auth">>,
        <<"oauth2">> => #{
            <<"enable">> => true,
            <<"grant_type">> => <<"client_credentials">>,
            <<"token_endpoint">> => <<"http://127.0.0.1:1/token">>,
            <<"client_id">> => <<"cid">>,
            <<"client_secret">> => <<"s3cr3t">>
        }
    },
    Serialized = emqx_authn:fill_defaults(Conf),
    %% Root cause: `make_serializable' restores the secret to plaintext, so the
    %% handler must redact it afterwards.
    ?assertEqual(<<"s3cr3t">>, client_secret(Serialized)),
    Redacted = emqx_utils:redact(Serialized),
    ?assertEqual(<<"******">>, client_secret(Redacted)),
    ?assertEqual(nomatch, binary:match(term_to_binary(Redacted), <<"s3cr3t">>)),
    cleanup_schemas().

authz_http_oauth2_client_secret_is_masked_test() ->
    ok = inject_schemas(),
    Source = #{
        <<"type">> => <<"http">>,
        <<"enable">> => true,
        <<"method">> => <<"post">>,
        <<"url">> => <<"http://127.0.0.1:1/authz">>,
        <<"oauth2">> => #{
            <<"enable">> => true,
            <<"grant_type">> => <<"client_credentials">>,
            <<"token_endpoint">> => <<"http://127.0.0.1:1/token">>,
            <<"client_id">> => <<"cid">>,
            <<"client_secret">> => <<"authz-s3cr3t">>
        }
    },
    Schema = emqx_hocon:make_schema(emqx_authz_schema:authz_fields()),
    %% `emqx_authz_api_sources:get_raw_sources/0' then calls
    %% `emqx_authz:format_for_api/1', which needs the running source registry and
    %% only rewrites `headers'; it does not touch `client_secret'.  The
    %% end-to-end handler (including `format_for_api') is covered by the
    %% `emqx_authz_api_sources_SUITE' case instead.
    #{<<"sources">> := [Serialized]} =
        hocon_tconf:make_serializable(Schema, #{<<"sources">> => [Source]}, #{}),
    ?assertEqual(<<"authz-s3cr3t">>, client_secret(Serialized)),
    Redacted = emqx_utils:redact(Serialized),
    ?assertEqual(<<"******">>, client_secret(Redacted)),
    ?assertEqual(nomatch, binary:match(term_to_binary(Redacted), <<"authz-s3cr3t">>)),
    cleanup_schemas().

client_secret(Conf) ->
    maps:get(<<"client_secret">>, maps:get(<<"oauth2">>, Conf)).

%% `emqx_authn_schema' and `emqx_authz_schema' receive their provider schema
%% modules from `emqx_conf_schema:roots/0' when the schema is built; these
%% pure tests build the schema without booting the node, so they inject the
%% providers themselves. Only the two providers under test are injected: the
%% assertions are about the HTTP backends, and naming them here keeps the test
%% independent of which other providers the product ships.
inject_schemas() ->
    ok = emqx_schema_hooks:inject_from_modules([
        {emqx_authn_schema, [emqx_authn_http_schema]},
        {emqx_authz_schema, [emqx_authz_http_schema]}
    ]).

cleanup_schemas() ->
    ok = emqx_schema_hooks:erase_injections().
