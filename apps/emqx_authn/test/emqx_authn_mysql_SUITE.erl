%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

init_per_testcase(t_authn, Config) ->
    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create_local, fun(_, _, _) -> {ok, undefined} end),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_authn, _Config) ->
    meck:unload(emqx_resource),
    ok;
end_per_testcase(_, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authn(_) ->
    Password = <<"test">>,
    Salt = <<"salt">>,
    PasswordHash = emqx_authn_utils:hash(sha256, Password, Salt, prefix),

    Config = #{<<"mechanism">> => <<"password-based">>,
               <<"backend">> => <<"mysql">>,
               <<"server">> => <<"127.0.0.1:3306">>,
               <<"database">> => <<"mqtt">>,
               <<"query">> => 
                   <<"SELECT password_hash, salt FROM users where username = ",
                        ?PH_USERNAME/binary, " LIMIT 1">>
               },
    {ok, _} = update_config([authentication], {create_authenticator, ?GLOBAL, Config}),

    meck:expect(emqx_resource, query,
        fun(_, {sql, _, [<<"good">>], _}) ->
            {ok, [<<"password_hash">>, <<"salt">>], [[PasswordHash, Salt]]};
           (_, {sql, _, _, _}) ->
            {error, this_is_a_fictitious_reason}
        end),

    ClientInfo = #{zone => default,
                   listener => 'tcp:default',
                   protocol => mqtt,
                   username => <<"good">>,
			       password => Password},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_access_control:authenticate(ClientInfo)),

    ClientInfo2 = ClientInfo#{username => <<"bad">>},
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(ClientInfo2)),
    emqx_authn_test_lib:delete_config(<<"password-based:mysql">>),
    ?AUTHN:delete_chain(?GLOBAL).

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

