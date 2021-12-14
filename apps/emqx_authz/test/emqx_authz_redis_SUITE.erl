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

-module(emqx_authz_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").


-define(REDIS_HOST, "redis").
-define(REDIS_PORT, 6379).
-define(REDIS_RESOURCE, <<"emqx_authz_redis_SUITE">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    case emqx_authn_test_lib:is_tcp_server_available(?REDIS_HOST, ?REDIS_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                   [emqx_conf, emqx_authz],
                   fun set_special_configs/1
                  ),
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
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = emqx_resource:remove_local(?REDIS_RESOURCE),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();

set_special_configs(_) ->
    ok.


%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_topic_rules(_Config) ->
    ClientInfo = #{clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    %% No rules

    ok = setup_sample(#{}),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{deny, subscribe, <<"#">>},
            {deny, subscribe, <<"subs">>},
            {deny, publish, <<"pub">>}]),

    %% Publish rules

    Sample0 = #{<<"mqtt_user:username">> =>
                   #{<<"testpub1/${username}">> => <<"publish">>,
                     <<"testpub2/${clientid}">> => <<"publish">>,
                     <<"testpub3/#">> => <<"publish">>
                    }
              },
    ok = setup_sample(Sample0),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, publish, <<"testpub1/username">>},
            {allow, publish, <<"testpub2/clientid">>},
            {allow, publish, <<"testpub3/foobar">>},

            {deny, publish, <<"testpub2/username">>},
            {deny, publish, <<"testpub1/clientid">>},


            {deny, subscribe, <<"testpub1/username">>},
            {deny, subscribe, <<"testpub2/clientid">>},
            {deny, subscribe, <<"testpub3/foobar">>}]),

    %% Subscribe rules

    Sample1 = #{<<"mqtt_user:username">> =>
                   #{<<"testsub1/${username}">> => <<"subscribe">>,
                     <<"testsub2/${clientid}">> => <<"subscribe">>,
                     <<"testsub3/#">> => <<"subscribe">>
                    }
              },
    ok = setup_sample(Sample1),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"testsub1/username">>},
            {allow, subscribe, <<"testsub2/clientid">>},
            {allow, subscribe, <<"testsub3/foobar">>},
            {allow, subscribe, <<"testsub3/+/foobar">>},
            {allow, subscribe, <<"testsub3/#">>},

            {deny, subscribe, <<"testsub2/username">>},
            {deny, subscribe, <<"testsub1/clientid">>},
            {deny, subscribe, <<"testsub4/foobar">>},
            {deny, publish, <<"testsub1/username">>},
            {deny, publish, <<"testsub2/clientid">>},
            {deny, publish, <<"testsub3/foobar">>}]),

    %% All rules

    Sample2 = #{<<"mqtt_user:username">> =>
                   #{<<"testall1/${username}">> => <<"all">>,
                     <<"testall2/${clientid}">> => <<"all">>,
                     <<"testall3/#">> => <<"all">>
                    }
              },
    ok = setup_sample(Sample2),
    ok = setup_config(#{}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"testall1/username">>},
            {allow, subscribe, <<"testall2/clientid">>},
            {allow, subscribe, <<"testall3/foobar">>},
            {allow, subscribe, <<"testall3/+/foobar">>},
            {allow, subscribe, <<"testall3/#">>},
            {allow, publish, <<"testall1/username">>},
            {allow, publish, <<"testall2/clientid">>},
            {allow, publish, <<"testall3/foobar">>},

            {deny, subscribe, <<"testall2/username">>},
            {deny, subscribe, <<"testall1/clientid">>},
            {deny, subscribe, <<"testall4/foobar">>},
            {deny, publish, <<"testall2/username">>},
            {deny, publish, <<"testall1/clientid">>},
            {deny, publish, <<"testall4/foobar">>}]).

t_lookups(_Config) ->
    ClientInfo = #{clientid => <<"clientid">>,
                   cn => <<"cn">>,
                   dn => <<"dn">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    ByClientid = #{<<"mqtt_user:clientid">> =>
                   #{<<"a">> => <<"all">>}},

    ok = setup_sample(ByClientid),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${clientid}">>}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}]),

    ByPeerhost = #{<<"mqtt_user:127.0.0.1">> =>
                   #{<<"a">> => <<"all">>}},

    ok = setup_sample(ByPeerhost),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${peerhost}">>}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}]),

    ByCN = #{<<"mqtt_user:cn">> =>
             #{<<"a">> => <<"all">>}},

    ok = setup_sample(ByCN),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${cert_common_name}">>}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}]),


    ByDN = #{<<"mqtt_user:dn">> =>
             #{<<"a">> => <<"all">>}},

    ok = setup_sample(ByDN),
    ok = setup_config(#{<<"cmd">> => <<"HGETALL mqtt_user:${cert_subject}">>}),

    ok = emqx_authz_test_lib:test_samples(
           ClientInfo,
           [{allow, subscribe, <<"a">>},
            {deny, subscribe, <<"b">>}]).

t_create_invalid(_Config) ->
    AuthzConfig = raw_redis_authz_config(),

    InvalidConfigs =
        [maps:without([<<"server">>], AuthzConfig),
         AuthzConfig#{<<"server">> => <<"unknownhost:3333">>},
         AuthzConfig#{<<"password">> => <<"wrongpass">>},
         AuthzConfig#{<<"database">> => <<"5678">>}],

    lists:foreach(
      fun(Config) ->
            {error, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
            [] = emqx_authz:lookup()

      end,
      InvalidConfigs).

t_redis_error(_Config) ->
    ok = setup_config(#{<<"cmd">> => <<"INVALID COMMAND">>}),

    ClientInfo = #{clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1},
                   zone => default,
                   listener => {tcp, default}
                  },

    deny = emqx_access_control:authorize(ClientInfo, subscribe, <<"a">>).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_sample(AuthzData) ->
    {ok, _} = q(["FLUSHDB"]),
    ok = lists:foreach(
           fun({Key, Values}) ->
                   lists:foreach(
                     fun({TopicFilter, Action}) ->
                             q(["HSET", Key, TopicFilter, Action])
                     end,
                     maps:to_list(Values))
           end,
           maps:to_list(AuthzData)).

setup_config(SpecialParams) ->
    ok = emqx_authz_test_lib:reset_authorizers(deny, false),
    Config = maps:merge(raw_redis_authz_config(), SpecialParams),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
    ok.

raw_redis_authz_config() ->
    #{
        <<"enable">> => <<"true">>,

        <<"type">> => <<"redis">>,
        <<"cmd">> => <<"HGETALL mqtt_user:${username}">>,
        <<"database">> => <<"1">>,
        <<"password">> => <<"public">>,
        <<"server">> => redis_server()
    }.

redis_server() ->
    iolist_to_binary(
      io_lib:format(
        "~s:~b",
        [?REDIS_HOST, ?REDIS_PORT])).

q(Command) ->
    emqx_resource:query(
      ?REDIS_RESOURCE,
      {cmd, Command}).

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
