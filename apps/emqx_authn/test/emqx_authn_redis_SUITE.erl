%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").


-define(REDIS_HOST, "redis").
-define(REDIS_PORT, 6379).
-define(REDIS_RESOURCE, <<"emqx_authn_redis_SUITE">>).


-define(PATH, [authentication]).

all() ->
    [{group, require_seeds}, t_create, t_create_invalid].

groups() ->
    [{require_seeds, [], [t_authenticate, t_update, t_destroy]}].

init_per_testcase(_, Config) ->
    emqx_authentication:initialize_authentication(?GLOBAL, []),
    emqx_authn_test_lib:delete_authenticators(
      [authentication],
      ?GLOBAL),
    Config.

init_per_group(require_seeds, Config) ->
    ok = init_seeds(),
    Config.

end_per_group(require_seeds, Config) ->
    ok = drop_seeds(),
    Config.

init_per_suite(Config) ->
    case emqx_authn_test_lib:is_tcp_server_available(?REDIS_HOST, ?REDIS_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_authn]),
            ok = start_apps([emqx_resource, emqx_connector]),
            {ok, _} = emqx_resource:create_local(
              ?REDIS_RESOURCE,
              emqx_connector_redis,
              redis_config()),
            Config;
        false ->
            {skip, no_redis}
    end.

end_per_suite(_Config) ->
    emqx_authn_test_lib:delete_authenticators(
      [authentication],
      ?GLOBAL),
    ok = emqx_resource:remove_local(?REDIS_RESOURCE),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authn]).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    {ok, []} = emqx_authentication:list_authenticators(?GLOBAL),

    AuthConfig = raw_redis_auth_config(),
    {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, AuthConfig}),

    {ok, [#{provider := emqx_authn_redis}]} = emqx_authentication:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    AuthConfig = raw_redis_auth_config(),

    InvalidConfigs =
        [
         maps:without([server], AuthConfig),
         AuthConfig#{server => <<"unknownhost:3333">>},
         AuthConfig#{password => <<"wrongpass">>},
         AuthConfig#{database => <<"5678">>},
         AuthConfig#{
           query => <<"MGET password_hash:${username} salt:${username}">>},
         AuthConfig#{
           query => <<"HMGET mqtt_user:${username} password_hash invalid_field">>},
         AuthConfig#{
           query => <<"HMGET mqtt_user:${username} salt is_superuser">>}
        ],

    lists:foreach(
      fun(Config) ->
              {error, _} = emqx:update_config(
                             ?PATH,
                             {create_authenticator, ?GLOBAL, Config}),

              {ok, []} = emqx_authentication:list_authenticators(?GLOBAL)
      end,
      InvalidConfigs).

t_authenticate(_Config) ->
    ok = lists:foreach(
           fun(Sample) ->
                   ct:pal("test_user_auth sample: ~p", [Sample]),
                   test_user_auth(Sample)
           end,
           user_seeds()).

test_user_auth(#{credentials := Credentials0,
                 config_params := SpecificConfgParams,
                 result := Result}) ->
    AuthConfig = maps:merge(raw_redis_auth_config(), SpecificConfgParams),

    {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, AuthConfig}),

    Credentials = Credentials0#{
                    listener => 'tcp:default',
                    protocol => mqtt
                   },

    ?assertEqual(Result, emqx_access_control:authenticate(Credentials)),

    emqx_authn_test_lib:delete_authenticators(
      [authentication],
      ?GLOBAL).

t_destroy(_Config) ->
    AuthConfig = raw_redis_auth_config(),

    {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, AuthConfig}),

    {ok, [#{provider := emqx_authn_redis, state := State}]}
        = emqx_authentication:list_authenticators(?GLOBAL),

    {ok, _} = emqx_authn_redis:authenticate(
                #{username => <<"plain">>,
                  password => <<"plain">>
                 },
                State),

    emqx_authn_test_lib:delete_authenticators(
      [authentication],
      ?GLOBAL),

    % Authenticator should not be usable anymore
    ?assertException(
       error,
       _,
       emqx_authn_redis:authenticate(
         #{username => <<"plain">>,
           password => <<"plain">>
          },
         State)).

t_update(_Config) ->
    CorrectConfig = raw_redis_auth_config(),
    IncorrectConfig =
        CorrectConfig#{
             query => <<"HMGET invalid_key:${username} password_hash salt is_superuser">>},

    {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, IncorrectConfig}),

    {error, not_authorized} = emqx_access_control:authenticate(
                                #{username => <<"plain">>,
                                  password => <<"plain">>,
                                  listener => 'tcp:default',
                                  protocol => mqtt
                                 }),

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
                ?PATH,
                {update_authenticator, ?GLOBAL, <<"password-based:redis">>, CorrectConfig}),

    {ok,_} = emqx_access_control:authenticate(
               #{username => <<"plain">>,
                 password => <<"plain">>,
                 listener => 'tcp:default',
                 protocol => mqtt
                }).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_redis_auth_config() ->
    #{
        mechanism => <<"password-based">>,
        password_hash_algorithm => <<"plain">>,
        salt_position => <<"suffix">>,
        enable => <<"true">>,

        backend => <<"redis">>,
        query => <<"HMGET mqtt_user:${username} password_hash salt is_superuser">>,
        database => <<"1">>,
        password => <<"public">>,
        server => redis_server()
    }.

user_seeds() ->
    [#{data => #{
                 password_hash => "plainsalt",
                 salt => "salt",
                 is_superuser => "1"
                },
       credentials => #{
                        username => <<"plain">>,
                        password => <<"plain">>},
       key => "mqtt_user:plain",
       config_params => #{},
       result => {ok,#{is_superuser => true}}
      },

     #{data => #{
                 password_hash => "9b4d0c43d206d48279e69b9ad7132e22",
                 salt => "salt",
                 is_superuser => "0"
                },
       credentials => #{
                        username => <<"md5">>,
                        password => <<"md5">>
                       },
       key => "mqtt_user:md5",
       config_params => #{
                          password_hash_algorithm => <<"md5">>,
                          salt_position => <<"suffix">>
                         },
       result => {ok,#{is_superuser => false}}
      },

     #{data => #{
         password_hash => "ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf",
         salt => "salt",
         is_superuser => "1"
        },
       credentials => #{
                        clientid => <<"sha256">>,
                        password => <<"sha256">>
                       },
       key => "mqtt_user:sha256",
       config_params => #{
              query => <<"HMGET mqtt_user:${clientid} password_hash salt is_superuser">>,
              password_hash_algorithm => <<"sha256">>,
              salt_position => <<"prefix">>
             },
       result => {ok,#{is_superuser => true}}
      },

     #{data => #{
                 password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                 salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                 is_superuser => "0"
                },
       credentials => #{
                        username => <<"bcrypt">>,
                        password => <<"bcrypt">>
                       },
       key => "mqtt_user:bcrypt",
       config_params => #{
                          password_hash_algorithm => <<"bcrypt">>,
                          salt_position => <<"suffix">> % should be ignored
                         },
       result => {ok,#{is_superuser => false}}
      },

     #{data => #{
                 password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                 salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                 is_superuser => "0"
                },
       credentials => #{
                        username => <<"bcrypt0">>,
                        password => <<"bcrypt">>
                       },
       key => "mqtt_user:bcrypt0",
       config_params => #{
              % clientid variable & username credentials
              query => <<"HMGET mqtt_client:${clientid} password_hash salt is_superuser">>,
              password_hash_algorithm => <<"bcrypt">>,
              salt_position => <<"suffix">>
             },
       result => {error,not_authorized}
      },

     #{data => #{
                 password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                 salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                 is_superuser => "0"
                },
       credentials => #{
                        username => <<"bcrypt1">>,
                        password => <<"bcrypt">>
                       },
       key => "mqtt_user:bcrypt1",
       config_params => #{
              % Bad key in query
              query => <<"HMGET badkey:${username} password_hash salt is_superuser">>,
              password_hash_algorithm => <<"bcrypt">>,
              salt_position => <<"suffix">>
             },
       result => {error,not_authorized}
      },

     #{data => #{
                 password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                 salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                 is_superuser => "0"
                },
       credentials => #{
                        username => <<"bcrypt2">>,
                        % Wrong password
                        password => <<"wrongpass">>
                       },
       key => "mqtt_user:bcrypt2",
       config_params => #{
              query => <<"HMGET mqtt_user:${username} password_hash salt is_superuser">>,
              password_hash_algorithm => <<"bcrypt">>,
              salt_position => <<"suffix">>
             },
       result => {error,bad_username_or_password}
      }
    ].

init_seeds() ->
    ok = drop_seeds(),
    lists:foreach(
      fun(#{key := UserKey, data := Values}) ->
              lists:foreach(fun({Key, Value}) ->
                                    q(["HSET", UserKey, atom_to_list(Key), Value])
                            end,
                            maps:to_list(Values))
      end,
      user_seeds()).

q(Command) ->
    emqx_resource:query(
      ?REDIS_RESOURCE,
      {cmd, Command}).

drop_seeds() ->
    lists:foreach(
      fun(#{key := UserKey}) ->
              q(["DEL", UserKey])
      end,
      user_seeds()).

redis_server() ->
    iolist_to_binary(
      io_lib:format(
        "~s:~b",
        [?REDIS_HOST, ?REDIS_PORT])).

redis_config() ->
    #{auto_reconnect => true,
      database => 1,
      pool_size => 8,
      redis_type => single,
      password => "public",
      server => {?REDIS_HOST, ?REDIS_PORT},
      ssl => #{enable => false}
     }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
