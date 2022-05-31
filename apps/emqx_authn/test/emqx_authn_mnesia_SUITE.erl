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

-module(emqx_authn_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include("emqx_authn.hrl").

-define(AUTHN_ID, <<"mechanism:backend">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    mria:clear_table(emqx_authn_mnesia),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

-define(CONF(Conf), #{?CONF_NS_BINARY => Conf}).

t_check_schema(_Config) ->
    ConfigOk = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"bcrypt">>,
            <<"salt_rounds">> => <<"6">>
        }
    },

    hocon_tconf:check_plain(emqx_authn_mnesia, ?CONF(ConfigOk)),

    ConfigNotOk = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"md6">>
        }
    },

    ?assertException(
        throw,
        {emqx_authn_mnesia, _},
        hocon_tconf:check_plain(emqx_authn_mnesia, ?CONF(ConfigNotOk))
    ).

t_create(_) ->
    Config0 = config(),

    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config1).

t_update(_) ->
    Config0 = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, _} = emqx_authn_mnesia:update(Config1, State).

t_destroy(_) ->
    Config = config(),
    OtherConfig = Config#{user_group => <<"stomp:global">>},
    {ok, State0} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {ok, StateOther} = emqx_authn_mnesia:create(?AUTHN_ID, OtherConfig),

    User = #{user_id => <<"u">>, password => <<"p">>},

    {ok, _} = emqx_authn_mnesia:add_user(User, State0),
    {ok, _} = emqx_authn_mnesia:add_user(User, StateOther),

    {ok, _} = emqx_authn_mnesia:lookup_user(<<"u">>, State0),
    {ok, _} = emqx_authn_mnesia:lookup_user(<<"u">>, StateOther),

    ok = emqx_authn_mnesia:destroy(State0),

    {ok, State1} = emqx_authn_mnesia:create(?AUTHN_ID, Config),
    {error, not_found} = emqx_authn_mnesia:lookup_user(<<"u">>, State1),
    {ok, _} = emqx_authn_mnesia:lookup_user(<<"u">>, StateOther).

t_authenticate(_) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    {ok, _} = emqx_authn_mnesia:authenticate(
        #{username => <<"u">>, password => <<"p">>},
        State
    ),
    {error, bad_username_or_password} = emqx_authn_mnesia:authenticate(
        #{username => <<"u">>, password => <<"badpass">>},
        State
    ),
    ignore = emqx_authn_mnesia:authenticate(
        #{clientid => <<"u">>, password => <<"p">>},
        State
    ).

t_add_user(_) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(User, State),
    {error, already_exist} = emqx_authn_mnesia:add_user(User, State).

t_delete_user(_) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    {error, not_found} = emqx_authn_mnesia:delete_user(<<"u">>, State),
    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    ok = emqx_authn_mnesia:delete_user(<<"u">>, State),
    {error, not_found} = emqx_authn_mnesia:delete_user(<<"u">>, State).

t_update_user(_) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    User = #{user_id => <<"u">>, password => <<"p">>},
    {ok, _} = emqx_authn_mnesia:add_user(User, State),

    {error, not_found} = emqx_authn_mnesia:update_user(<<"u1">>, #{password => <<"p1">>}, State),
    {ok, #{
        user_id := <<"u">>,
        is_superuser := true
    }} = emqx_authn_mnesia:update_user(
        <<"u">>,
        #{password => <<"p1">>, is_superuser => true},
        State
    ),

    {ok, _} = emqx_authn_mnesia:authenticate(
        #{username => <<"u">>, password => <<"p1">>},
        State
    ),

    {ok, #{is_superuser := true}} = emqx_authn_mnesia:lookup_user(<<"u">>, State).

t_list_users(_) ->
    Config = config(),
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    Users = [
        #{user_id => <<"u1">>, password => <<"p">>},
        #{user_id => <<"u2">>, password => <<"p">>},
        #{user_id => <<"u3">>, password => <<"p">>}
    ],

    lists:foreach(
        fun(U) -> {ok, _} = emqx_authn_mnesia:add_user(U, State) end,
        Users
    ),

    #{
        data := [
            #{is_superuser := false, user_id := _},
            #{is_superuser := false, user_id := _}
        ],
        meta := #{page := 1, limit := 2, count := 3}
    } = emqx_authn_mnesia:list_users(
        #{<<"page">> => 1, <<"limit">> => 2},
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := _}],
        meta := #{page := 2, limit := 2, count := 3}
    } = emqx_authn_mnesia:list_users(
        #{<<"page">> => 2, <<"limit">> => 2},
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := <<"u3">>}],
        meta := #{page := 1, limit := 20, count := 1}
    } = emqx_authn_mnesia:list_users(
        #{
            <<"page">> => 1,
            <<"limit">> => 20,
            <<"like_user_id">> => <<"3">>
        },
        State
    ).

t_import_users(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertEqual(
        ok,
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.json">>),
            State
        )
    ),

    ?assertEqual(
        ok,
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.csv">>),
            State
        )
    ),

    ?assertMatch(
        {error, {unsupported_file_format, _}},
        emqx_authn_mnesia:import_users(
            {<<"/file/with/unknown.extension">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, unknown_file_format},
        emqx_authn_mnesia:import_users(
            {<<"/file/with/no/extension">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, bad_format},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed-0.json">>),
            State
        )
    ),

    ?assertMatch(
        {error, {_, invalid_json}},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed-1.json">>),
            State
        )
    ),

    ?assertEqual(
        {error, bad_format},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials-malformed.csv">>),
            State
        )
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

sample_filename(Name) ->
    Dir = code:lib_dir(emqx_authn, test),
    filename:join([Dir, <<"data">>, Name]).

sample_filename_and_data(Name) ->
    Filename = sample_filename(Name),
    {ok, Data} = file:read_file(Filename),
    {Filename, Data}.

config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{
            name => bcrypt,
            salt_rounds => 8
        },
        user_group => <<"global:mqtt">>
    }.
