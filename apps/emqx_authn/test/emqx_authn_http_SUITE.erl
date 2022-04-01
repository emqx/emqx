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

-define(HTTP_PORT, 33333).
-define(HTTP_PATH, "/auth").
-define(CREDENTIALS, #{
    username => <<"plain">>,
    password => <<"plain">>,
    listener => 'tcp:default',
    protocol => mqtt
}).

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
            AuthConfig#{headers => []},
            AuthConfig#{method => delete}
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
            {ok, []} = emqx_authentication:list_authenticators(?GLOBAL)
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

    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Req0),
            {ok, Req, State}
        end
    ),

    {ok, [#{provider := emqx_authn_http, state := State}]} =
        emqx_authentication:list_authenticators(?GLOBAL),

    Credentials = maps:with([username, password], ?CREDENTIALS),

    {ok, _} = emqx_authn_http:authenticate(
        Credentials,
        State
    ),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    % Authenticator should not be usable anymore
    ?assertMatch(
        ignore,
        emqx_authn_http:authenticate(
            Credentials,
            State
        )
    ).

t_update(_Config) ->
    CorrectConfig = raw_http_auth_config(),
    IncorrectConfig =
        CorrectConfig#{url => <<"http://127.0.0.1:33333/invalid">>},

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, IncorrectConfig}
    ),

    ok = emqx_authn_http_test_server:set_handler(
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Req0),
            {ok, Req, State}
        end
    ),

    {error, not_authorized} = emqx_access_control:authenticate(?CREDENTIALS),

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:http">>, CorrectConfig}
    ),

    {ok, _} = emqx_access_control:authenticate(?CREDENTIALS).

t_is_superuser(_Config) ->
    Config = raw_http_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        {json, <<"0">>, false},
        {json, <<"">>, false},
        {json, null, false},
        {json, 0, false},

        {json, <<"1">>, true},
        {json, <<"val">>, true},
        {json, 1, true},
        {json, 123, true},

        {form, <<"0">>, false},
        {form, <<"">>, false},

        {form, <<"1">>, true},
        {form, <<"val">>, true}
    ],

    lists:foreach(fun test_is_superuser/1, Checks).

test_is_superuser({Kind, Value, ExpectedValue}) ->
    {ContentType, Res} =
        case Kind of
            json ->
                {<<"application/json">>, jiffy:encode(#{is_superuser => Value})};
            form ->
                {<<"application/x-www-form-urlencoded">>,
                    iolist_to_binary([<<"is_superuser=">>, Value])}
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
        {ok, #{is_superuser := ExpectedValue}},
        emqx_access_control:authenticate(?CREDENTIALS)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_http_auth_config() ->
    #{
        mechanism => <<"password_based">>,
        enable => <<"true">>,

        backend => <<"http">>,
        method => <<"get">>,
        url => <<"http://127.0.0.1:33333/auth">>,
        body => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        headers => #{<<"X-Test-Header">> => <<"Test Value">>}
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

                Req = cowboy_req:reply(200, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false}}
        },

        %% get request with json body response
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    jiffy:encode(#{is_superuser => true}),
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
                    <<"is_superuser=true">>,
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
            result => {ok, #{is_superuser => false}}
        },

        %% simple post request, application/json
        #{
            handler => fun(Req0, State) ->
                {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = jiffy:decode(RawBody, [return_maps]),
                Req = cowboy_req:reply(200, Req1),
                {ok, Req, State}
            end,
            config_params => #{
                method => post,
                headers => #{<<"content-type">> => <<"application/json">>}
            },
            result => {ok, #{is_superuser => false}}
        },

        %% simple post request, application/x-www-form-urlencoded
        #{
            handler => fun(Req0, State) ->
                {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),
                #{
                    <<"username">> := <<"plain">>,
                    <<"password">> := <<"plain">>
                } = maps:from_list(PostVars),
                Req = cowboy_req:reply(200, Req1),
                {ok, Req, State}
            end,
            config_params => #{
                method => post,
                headers => #{
                    <<"content-type">> =>
                        <<"application/x-www-form-urlencoded">>
                }
            },
            result => {ok, #{is_superuser => false}}
        }#{
            %% 204 code
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(204, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {ok, #{is_superuser => false}}
        },

        %% custom headers
        #{
            handler => fun(Req0, State) ->
                <<"Test Value">> = cowboy_req:header(<<"x-test-header">>, Req0),
                Req = cowboy_req:reply(200, Req0),
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
            result => {error, not_authorized}
        },

        %% 500 code
        #{
            handler => fun(Req0, State) ->
                Req = cowboy_req:reply(500, Req0),
                {ok, Req, State}
            end,
            config_params => #{},
            result => {error, not_authorized}
        },

        %% Handling error
        #{
            handler => fun(Req0, State) ->
                error(woops),
                {ok, Req0, State}
            end,
            config_params => #{},
            result => {error, not_authorized}
        }
    ].

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
