%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("common_test/include/ct.hrl").

-define(AUTHN_ID, <<"mechanism:backend">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_mnesia], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    mria:clear_table(emqx_authn_mnesia),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_) ->
    Config0 = config(),

    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config0),

    Config1 = Config0#{password_hash_algorithm => #{name => sha256}},
    {ok, _} = emqx_authn_mnesia:create(?AUTHN_ID, Config1),
    ok.

t_bootstrap_file(_) ->
    Config = config(),
    %% hash to hash
    HashConfig = Config#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    ?assertMatch(
        [
            {user_info, {_, <<"myuser1">>}, _, _, true},
            {user_info, {_, <<"myuser2">>}, _, _, false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials.json">>)
    ),
    ?assertMatch(
        [
            {user_info, {_, <<"myuser3">>}, _, _, true},
            {user_info, {_, <<"myuser4">>}, _, _, false}
        ],
        test_bootstrap_file(HashConfig, hash, <<"user-credentials.csv">>)
    ),

    %% plain to plain
    PlainConfig = Config#{
        password_hash_algorithm =>
            #{name => plain, salt_position => disable}
    },
    ?assertMatch(
        [
            {user_info, {_, <<"myuser1">>}, <<"password1">>, _, true},
            {user_info, {_, <<"myuser2">>}, <<"password2">>, _, false}
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain.json">>)
    ),
    ?assertMatch(
        [
            {user_info, {_, <<"myuser3">>}, <<"password3">>, _, true},
            {user_info, {_, <<"myuser4">>}, <<"password4">>, _, false}
        ],
        test_bootstrap_file(PlainConfig, plain, <<"user-credentials-plain.csv">>)
    ),
    %% plain to hash
    ?assertMatch(
        [
            {user_info, {_, <<"myuser1">>}, _, _, true},
            {user_info, {_, <<"myuser2">>}, _, _, false}
        ],
        test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain.json">>)
    ),
    Opts = #{clean => false},
    Result = test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain.csv">>, Opts),
    ?assertMatch(
        [
            {user_info, {_, <<"myuser3">>}, _, _, true},
            {user_info, {_, <<"myuser4">>}, _, _, false}
        ],
        Result
    ),
    %% Don't override the exist user id.
    ?assertMatch(
        Result, test_bootstrap_file(HashConfig, plain, <<"user-credentials-plain_v2.csv">>)
    ),
    ok.

test_bootstrap_file(Config0, Type, File) ->
    test_bootstrap_file(Config0, Type, File, #{clean => true}).

test_bootstrap_file(Config0, Type, File, Opts) ->
    {Type, Filename, _FileData} = sample_filename_and_data(Type, File),
    Config2 = Config0#{
        bootstrap_file => Filename,
        bootstrap_type => Type
    },
    {ok, State0} = emqx_authn_mnesia:create(?AUTHN_ID, Config2),
    Result = ets:tab2list(emqx_authn_mnesia),
    case maps:get(clean, Opts) of
        true ->
            ok = emqx_authn_mnesia:destroy(State0),
            ?assertMatch([], ets:tab2list(emqx_authn_mnesia));
        _ ->
            ok
    end,
    Result.

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
        meta := #{page := 1, limit := 2, count := 3, hasnext := true}
    } = emqx_authn_mnesia:list_users(
        #{<<"page">> => 1, <<"limit">> => 2},
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := _}],
        meta := #{page := 2, limit := 2, count := 3, hasnext := false}
    } = emqx_authn_mnesia:list_users(
        #{<<"page">> => 2, <<"limit">> => 2},
        State
    ),

    #{
        data := [#{is_superuser := false, user_id := <<"u3">>}],
        meta := #{page := 1, limit := 20, hasnext := false}
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

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.json">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(<<"user-credentials.csv">>),
            State
        )
    ),

    ?assertMatch(
        {error, {unsupported_file_format, _}},
        emqx_authn_mnesia:import_users(
            {hash, <<"/file/with/unknown.extension">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, unknown_file_format},
        emqx_authn_mnesia:import_users(
            {hash, <<"/file/with/no/extension">>, <<>>},
            State
        )
    ),
    %% import plain.json with hash method
    ?assertEqual(
        {error, "hash_import_requires_password_hash_field"},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(hash, <<"user-credentials-plain.json">>),
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
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, <<"empty_users.json">>, <<"[]">>},
            State
        )
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, <<"empty_users.csv">>, <<>>},
            State
        )
    ),

    ?assertEqual(
        {error, empty_users},
        emqx_authn_mnesia:import_users(
            {hash, prepared_user_list, []},
            State
        )
    ).

t_import_users_plain(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain.json">>),
            State
        )
    ),
    %% import hash.json with plain method
    ?assertEqual(
        {error, "plain_import_requires_password_field"},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials.json">>),
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain.csv">>),
            State
        )
    ).

t_import_users_prepared_list(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => sha256, salt_position => suffix}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    Users1 = [
        #{<<"user_id">> => <<"u1">>, <<"password">> => <<"p1">>, <<"is_superuser">> => true},
        #{<<"user_id">> => <<"u2">>, <<"password">> => <<"p2">>, <<"is_superuser">> => true}
    ],
    Users2 = [
        #{
            <<"user_id">> => <<"u3">>,
            <<"password_hash">> =>
                <<"c5e46903df45e5dc096dc74657610dbee8deaacae656df88a1788f1847390242">>,
            <<"salt">> => <<"e378187547bf2d6f0545a3f441aa4d8a">>,
            <<"is_superuser">> => true
        },
        #{
            <<"user_id">> => <<"u4">>,
            <<"password_hash">> =>
                <<"f4d17f300b11e522fd33f497c11b126ef1ea5149c74d2220f9a16dc876d4567b">>,
            <<"salt">> => <<"6d3f9bd5b54d94b98adbcfe10b6d181f">>,
            <<"is_superuser">> => true
        }
    ],

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {plain, prepared_user_list, Users1},
            State
        )
    ),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {hash, prepared_user_list, Users2},
            State
        )
    ).

t_import_users_duplicated_records(_) ->
    Config0 = config(),
    Config = Config0#{password_hash_algorithm => #{name => plain, salt_position => disable}},
    {ok, State} = emqx_authn_mnesia:create(?AUTHN_ID, Config),

    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain-dup.json">>),
            State
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            sample_filename_and_data(plain, <<"user-credentials-plain-dup.csv">>),
            State
        )
    ),
    Users1 = [
        #{
            <<"user_id">> => <<"myuser5">>,
            <<"password">> => <<"password5">>,
            <<"is_superuser">> => true
        },
        #{
            <<"user_id">> => <<"myuser5">>,
            <<"password">> => <<"password6">>,
            <<"is_superuser">> => false
        }
    ],
    ?assertMatch(
        {ok, _},
        emqx_authn_mnesia:import_users(
            {plain, prepared_user_list, Users1},
            State
        )
    ),

    %% assert: the last record overwrites the previous one
    ?assertMatch(
        [
            {user_info, {_, <<"myuser1">>}, <<"password2">>, _, false},
            {user_info, {_, <<"myuser3">>}, <<"password4">>, _, false},
            {user_info, {_, <<"myuser5">>}, <<"password6">>, _, false}
        ],
        ets:tab2list(emqx_authn_mnesia)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

sample_filename(Name) ->
    Dir = code:lib_dir(emqx_auth),
    filename:join([Dir, <<"test">>, <<"data">>, Name]).

sample_filename_and_data(Name) ->
    sample_filename_and_data(hash, Name).

sample_filename_and_data(Type, Name) ->
    Filename = sample_filename(Name),
    {ok, Data} = file:read_file(Filename),
    {Type, Filename, Data}.

config() ->
    #{
        user_id_type => username,
        password_hash_algorithm => #{
            name => bcrypt,
            salt_rounds => 8
        },
        user_group => <<"global:mqtt">>
    }.
