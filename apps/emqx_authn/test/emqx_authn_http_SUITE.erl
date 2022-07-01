%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(PATH, [?CONF_NS_ATOM]).

-define(HTTP_PORT, 32333).
-define(HTTP_PATH, "/auth").
-define(CREDENTIALS, #{
    clientid => <<"clienta">>,
    username => <<"plain">>,
    password => <<"plain">>,
    peerhost => {127, 0, 0, 1},
    listener => 'tcp:default',
    protocol => mqtt
}).

-define(SERVER_RESPONSE_JSON(Result), ?SERVER_RESPONSE_JSON(Result, false)).
-define(SERVER_RESPONSE_JSON(Result, IsSuperuser),
    jiffy:encode(#{
        result => Result,
        is_superuser => IsSuperuser
    })
).

-define(SERVER_RESPONSE_URLENCODE(Result), ?SERVER_RESPONSE_URLENCODE(Result, false)).
-define(SERVER_RESPONSE_URLENCODE(Result, IsSuperuser),
    list_to_binary(
        "result=" ++
            uri_encode(Result) ++ "&" ++
            "is_superuser=" ++
            uri_encode(IsSuperuser)
    )
).

-define(EXCEPTION_ALLOW, ?EXCEPTION_ALLOW(false)).
-define(EXCEPTION_ALLOW(IsSuperuser), {ok, #{is_superuser := IsSuperuser}}).
-define(EXCEPTION_DENY, {error, not_authorized}).
-define(EXCEPTION_IGNORE, ignore).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_authn]),
    application:ensure_all_started(cowboy),
    Config.

end_per_suite(_) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    application:stop(cowboy),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    {ok, _} = emqx_authn_http_test_server:start_link(?HTTP_PORT, ?HTTP_PATH),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_authn_http_test_server:stop().

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_http_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_http}]} = emqx_authentication:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    AuthConfig = raw_http_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"headers">> => []},
            AuthConfig#{<<"method">> => <<"delete">>}
        ],

    lists:foreach(
        fun(Config) ->
            ct:pal("creating authenticator with invalid config: ~p", [Config]),
            {error, _} =
                try
                    emqx:update_config(
                        ?PATH,
                        {create_authenticator, ?GLOBAL, Config}
                    )
                catch
                    throw:Error ->
                        {error, Error}
                end,
            ?assertEqual(
                {error, {not_found, {chain, ?GLOBAL}}},
                emqx_authentication:list_authenticators(?GLOBAL)
            )
        end,
        InvalidConfigs
    ).

t_authenticate(_Config) ->
    ok = lists:foreach(
        fun(Sample) ->
            ct:pal("test_user_auth sample: ~p", [Sample]),
            test_user_auth(Sample)
        end,
        samples()
    ).

test_user_auth(#{
    handler := Handler,
    config_params := SpecificConfgParams,
    result := Result
}) ->
    AuthConfig = maps:merge(raw_http_auth_config(), SpecificConfgParams),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    ok = emqx_authn_http_test_server:set_handler(Handler),

    ?assertEqual(Result, emqx_access_control:authenticate(?CREDENTIALS)),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_destroy(_Config) ->
    AuthConfig = raw_http_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    Headers = #{<<"content-type">> => <<"application/json">>},
    Response = ?SERVER_RESPONSE_JSON(allow),

    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Headers, Response, Req0),
            {ok, Req, State}
        end
    ),

    {ok, [#{provider := emqx_authn_http, state := State}]} =
        emqx_authentication:list_authenticators(?GLOBAL),

    Credentials = maps:with([username, password], ?CREDENTIALS),

    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_authn_http:authenticate(
            Credentials,
            State
        )
    ),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    % Authenticator should not be usable anymore
    ?assertMatch(
        ?EXCEPTION_IGNORE,
        emqx_authn_http:authenticate(
            Credentials,
            State
        )
    ).

t_update(_Config) ->
    CorrectConfig = raw_http_auth_config(),
    IncorrectConfig =
        CorrectConfig#{<<"url">> => <<"http://127.0.0.1:32333/invalid">>},

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, IncorrectConfig}
    ),

    Headers = #{<<"content-type">> => <<"application/json">>},
    Response = ?SERVER_RESPONSE_JSON(allow),

    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Headers, Response, Req0),
            {ok, Req, State}
        end
    ),

    ?assertMatch(
        ?EXCEPTION_DENY,
        emqx_access_control:authenticate(?CREDENTIALS)
    ),

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, CorrectConfig}
    ),

    ?assertMatch(
        ?EXCEPTION_ALLOW,
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_is_superuser(_Config) ->
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        %% {ContentType, ExpectedIsSuperuser, ResponseIsSuperuser}
        %% Is Superuser
        {json, true, <<"1">>},
        {json, true, 1},
        {json, true, 123},
        {json, true, <<"true">>},
        {json, true, true},

        %% Not Superuser
        {json, false, <<"">>},
        {json, false, <<"0">>},
        {json, false, 0},
        {json, false, null},
        {json, false, undefined},
        {json, false, <<"false">>},
        {json, false, false},

        {json, false, <<"val">>},

        %% Is Superuser
        {form, true, <<"1">>},
        {form, true, 1},
        {form, true, 123},
        {form, true, <<"true">>},
        {form, true, true},

        %% Not Superuser
        {form, false, <<"">>},
        {form, false, <<"0">>},
        {form, false, 0},

        {form, false, null},
        {form, false, undefined},
        {form, false, <<"false">>},
        {form, false, false},

        {form, false, <<"val">>}
    ],

    lists:foreach(fun test_is_superuser/1, Checks).

test_is_superuser({Kind, ExpectedValue, ServerResponse}) ->
    {ContentType, Res} =
        case Kind of
            json ->
                {<<"application/json; charset=utf-8">>,
                    ?SERVER_RESPONSE_JSON(allow, ServerResponse)};
            form ->
                {<<"application/x-www-form-urlencoded; charset=utf-8">>,
                    ?SERVER_RESPONSE_URLENCODE(allow, ServerResponse)}
        end,

    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => ContentType},
                Res,
                Req0
            ),
            {ok, Req, State}
        end
    ),

    ?assertMatch(
        ?EXCEPTION_ALLOW(ExpectedValue),
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

t_ignore_allow_deny(_Config) ->
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        %% only one chain, ignore by authn http and deny by default
        {deny, ?SERVER_RESPONSE_JSON(ignore)},

        {{allow, true}, ?SERVER_RESPONSE_JSON(allow, true)},
        {{allow, false}, ?SERVER_RESPONSE_JSON(allow)},
        {{allow, false}, ?SERVER_RESPONSE_JSON(allow, false)},

        {deny, ?SERVER_RESPONSE_JSON(deny)},
        {deny, ?SERVER_RESPONSE_JSON(deny, true)},
        {deny, ?SERVER_RESPONSE_JSON(deny, false)}
    ],

    lists:foreach(fun test_ignore_allow_deny/1, Checks).

test_ignore_allow_deny({ExpectedValue, ServerResponse}) ->
    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                ServerResponse,
                Req0
            ),
            {ok, Req, State}
        end
    ),

    case ExpectedValue of
        {allow, IsSuperuser} ->
            ?assertMatch(
                ?EXCEPTION_ALLOW(IsSuperuser),
                emqx_access_control:authenticate(?CREDENTIALS)
            );
        deny ->
            ?assertMatch(
                ?EXCEPTION_DENY,
                emqx_access_control:authenticate(?CREDENTIALS)
            )
    end.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_http_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:32333/auth">>,
        <<"body">> => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

samples() ->
    [
        %% simple get request
        #{
            handler => fun(Req0, State) ->
                #{
                    username := <<"plain">>,
                    password := <<"plain">>
                } = cowboy_req:match_qs([username, password], Req0),

                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{result => allow, is_superuser => false}),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false, user_property => #{}}}
        },

        %% get request with json body response
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{result => allow, is_superuser => true}),
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => true, user_property => #{}}}
        },

        %% get request with url-form-encoded body response
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{
                        <<"content-type">> =>
                            <<"application/x-www-form-urlencoded">>
                    },
                    <<"is_superuser=true&result=allow">>,
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => true, user_property => #{}}}
        },

        %% get request with response of unknown encoding
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{
                        <<"content-type">> =>
                            <<"test/plain">>
                    },
                    <<"is_superuser=true">>,
                    Req0
                ),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% simple post request, application/json
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = jiffy:decode(RawBody, [return_maps]),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{<<"content-type">> => <<"application/json">>}
            },
            result => {ok, #{is_superuser => false, user_property => #{}}}
        },

        %% simple post request, application/x-www-form-urlencoded
        #{
            handler => fun(Req0, State) ->
                {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = maps:from_list(PostVars),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{
                    <<"content-type">> =>
                        <<"application/x-www-form-urlencoded">>
                }
            },
            result => {ok, #{is_superuser => false, user_property => #{}}}
        },

        %% simple post request for placeholders, application/json
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>,
                    <<"clientid">> := <<"clienta">>,
                    <<"peerhost">> := <<"127.0.0.1">>
                } = jiffy:decode(RawBody, [return_maps]),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{result => allow, is_superuser => false}),
                    Req1
                ),
                {ok, Req, State}
            end,
            config_params => #{
                <<"method">> => <<"post">>,
                <<"headers">> => #{<<"content-type">> => <<"application/json">>},
                <<"body">> => #{
                    <<"clientid">> => ?PH_CLIENTID,
                    <<"username">> => ?PH_USERNAME,
                    <<"password">> => ?PH_PASSWORD,
                    <<"peerhost">> => ?PH_PEERHOST
                }
            },
            result => {ok, #{is_superuser => false, user_property => #{}}}
        },

        %% custom headers
        #{
            handler => fun(Req0, State) ->
                <<"Test Value">> = cowboy_req:header(<<"x-test-header">>, Req0),
                Req = cowboy_req:reply(200, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% 204 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(204, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false}}
        },

        %% 400 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(400, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% 500 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(500, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        },

        %% Handling error
        #{
            handler => fun(Req0, State) ->
                error(woops),
                {ok, Req0, State}
            end,
            config_params => #{},
            %% only one chain, ignore by authn http and deny by default
            result => {error, not_authorized}
        }
    ].

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(N) when is_integer(N) ->
    integer_to_list(N);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
