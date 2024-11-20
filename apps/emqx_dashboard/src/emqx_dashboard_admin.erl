%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Web dashboard admin authentication with username and password.

-module(emqx_dashboard_admin).

-feature(maybe_expr, enable).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(emqx_db_backup).

-export([create_tables/0]).

-export([
    add_user/4,
    force_add_user/4,
    remove_user/1,
    update_user/3,
    lookup_user/1,
    change_password/2,
    change_password/3,
    all_users/0,
    check/2
]).

-export([
    sign_token/2,
    verify_token/2,
    destroy_token_by_username/2
]).
-export([
    hash/1,
    verify_hash/2
]).

-export([
    add_default_user/0,
    default_username/0
]).

-export([role/1]).

-export([backup_tables/0]).

-if(?EMQX_RELEASE_EDITION == ee).
-export([add_sso_user/4, lookup_user/2]).
-endif.

-ifdef(TEST).
-export([unsafe_update_user/1]).
-endif.

-type emqx_admin() :: #?ADMIN{}.

-define(USERNAME_ALREADY_EXISTS_ERROR, <<"username_already_exists">>).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?ADMIN, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, ?ADMIN},
        {attributes, record_info(fields, ?ADMIN)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?ADMIN].

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"dashboard_users">>, [?ADMIN]}.

%%--------------------------------------------------------------------
%% bootstrap API
%%--------------------------------------------------------------------

-spec add_default_user() -> {ok, map() | empty | default_user_exists} | {error, any()}.
add_default_user() ->
    add_default_user(binenv(default_username), binenv(default_password)).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec add_user(dashboard_username(), binary(), dashboard_user_role(), binary()) ->
    {ok, map()} | {error, any()}.
add_user(Username, Password, Role, Desc) when is_binary(Username), is_binary(Password) ->
    case {legal_username(Username), legal_password(Password), legal_role(Role)} of
        {ok, ok, ok} -> do_add_user(Username, Password, Role, Desc);
        {{error, Reason}, _, _} -> {error, Reason};
        {_, {error, Reason}, _} -> {error, Reason};
        {_, _, {error, Reason}} -> {error, Reason}
    end.

do_add_user(Username, Password, Role, Desc) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun add_user_/4, [Username, Password, Role, Desc]),
    return(Res).

%% 0-9 or A-Z or a-z or $_
legal_username(<<>>) ->
    {error, <<"Username cannot be empty">>};
legal_username(UserName) ->
    case re:run(UserName, "^[_a-zA-Z0-9]*$", [{capture, none}]) of
        nomatch ->
            {error, <<
                "Bad Username."
                " Only upper and lower case letters, numbers and underscores are supported"
            >>};
        match ->
            ok
    end.

-define(LOW_LETTER_CHARS, "abcdefghijklmnopqrstuvwxyz").
-define(UPPER_LETTER_CHARS, "ABCDEFGHIJKLMNOPQRSTUVWXYZ").
-define(LETTER, ?LOW_LETTER_CHARS ++ ?UPPER_LETTER_CHARS).
-define(NUMBER, "0123456789").
-define(SPECIAL_CHARS, "!@#$%^&*()_+-=[]{}\"|;':,./<>?`~ ").
-define(INVALID_PASSWORD_MSG, <<
    "Bad password. "
    "At least two different kind of characters from groups of letters, numbers, and special characters. "
    "For example, if password is composed from letters, it must contain at least one number or a special character."
>>).
-define(BAD_PASSWORD_LEN, <<"The range of password length is 8~64">>).

legal_password(Password) when is_binary(Password) ->
    legal_password(binary_to_list(Password));
legal_password(Password) when is_list(Password) ->
    legal_password(Password, erlang:length(Password)).

legal_password(Password, Len) when Len >= 8 andalso Len =< 64 ->
    case is_mixed_password(Password) of
        true -> ascii_character_validate(Password);
        false -> {error, ?INVALID_PASSWORD_MSG}
    end;
legal_password(_Password, _Len) ->
    {error, ?BAD_PASSWORD_LEN}.

%% The password must contain at least two different kind of characters
%% from groups of letters, numbers, and special characters.
is_mixed_password(Password) -> is_mixed_password(Password, [?NUMBER, ?LETTER, ?SPECIAL_CHARS], 0).

is_mixed_password(_Password, _Chars, 2) ->
    true;
is_mixed_password(_Password, [], _Count) ->
    false;
is_mixed_password(Password, [Chars | Rest], Count) ->
    NewCount =
        case contain(Password, Chars) of
            true -> Count + 1;
            false -> Count
        end,
    is_mixed_password(Password, Rest, NewCount).

%% regex-non-ascii-character, such as Chinese, Japanese, Korean, etc.
ascii_character_validate(Password) ->
    case re:run(Password, "[^\\x00-\\x7F]+", [unicode, {capture, none}]) of
        match -> {error, <<"Only ascii characters are allowed in the password">>};
        nomatch -> ok
    end.

contain(Xs, Spec) -> lists:any(fun(X) -> lists:member(X, Spec) end, Xs).

%% black-magic: force overwrite a user
force_add_user(Username, Password, Role, Desc) ->
    AddFun = fun() ->
        mnesia:write(#?ADMIN{
            username = Username,
            pwdhash = hash(Password),
            role = Role,
            description = Desc,
            extra = #{password_ts => erlang:system_time(second)}
        })
    end,
    case mria:sync_transaction(?DASHBOARD_SHARD, AddFun) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%% @private
add_user_(Username, Password, Role, Desc) ->
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            Admin = #?ADMIN{
                username = Username,
                pwdhash = hash(Password),
                role = Role,
                description = Desc,
                extra = #{password_ts => erlang:system_time(second)}
            },
            mnesia:write(Admin),
            ?SLOG(info, #{msg => "dashboard_sso_user_added", username => Username, role => Role}),
            flatten_username(#{username => Username, role => Role, description => Desc});
        [_] ->
            ?SLOG(info, #{
                msg => "dashboard_sso_user_add_failed",
                reason => "username_already_exists",
                username => Username,
                role => Role
            }),
            mnesia:abort(?USERNAME_ALREADY_EXISTS_ERROR)
    end.

-spec remove_user(dashboard_username()) -> {ok, any()} | {error, any()}.
remove_user(Username) ->
    Trans = fun() ->
        case lookup_user(Username) of
            [] -> mnesia:abort(<<"username_not_found">>);
            _ -> mnesia:delete({?ADMIN, Username})
        end
    end,
    case return(mria:sync_transaction(?DASHBOARD_SHARD, Trans)) of
        {ok, Result} ->
            _ = emqx_dashboard_token:destroy_by_username(Username),
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

-spec update_user(dashboard_username(), dashboard_user_role(), binary()) ->
    {ok, map()} | {error, term()}.
update_user(Username, Role, Desc) ->
    case legal_role(Role) of
        ok ->
            case
                return(
                    mria:sync_transaction(
                        ?DASHBOARD_SHARD,
                        fun update_user_/3,
                        [Username, Role, Desc]
                    )
                )
            of
                {ok, {true, Result}} ->
                    {ok, Result};
                {ok, {false, Result}} ->
                    %% role has changed, destroy the related token
                    _ = emqx_dashboard_token:destroy_by_username(Username),
                    {ok, Result};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

hash(Password) ->
    SaltBin = emqx_dashboard_token:salt(),
    <<SaltBin/binary, (sha256(SaltBin, Password))/binary>>.

verify_hash(Origin, SaltHash) ->
    case SaltHash of
        <<Salt:4/binary, Hash/binary>> ->
            case Hash =:= sha256(Salt, Origin) of
                true -> ok;
                false -> error
            end;
        _ ->
            error
    end.

sha256(SaltBin, Password) ->
    crypto:hash('sha256', <<SaltBin/binary, Password/binary>>).

verify_password_expiration(0, _User) ->
    {ok, #{}};
verify_password_expiration(ValidityDuration, #?ADMIN{extra = #{password_ts := Ts}}) ->
    Diff = Ts + ValidityDuration - erlang:system_time(second),
    {ok, #{password_expire_in_seconds => Diff}};
verify_password_expiration(ValidityDuration, #?ADMIN{extra = Extra} = User) ->
    case
        unsafe_update_user(User#?ADMIN{extra = Extra#{password_ts => erlang:system_time(second)}})
    of
        ok ->
            {ok, #{password_expire_in_seconds => ValidityDuration}};
        Error ->
            Error
    end.

%% @private
update_user_(Username, Role, Desc) ->
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            mnesia:abort(<<"username_not_found">>);
        [Admin] ->
            mnesia:write(Admin#?ADMIN{role = Role, description = Desc}),
            {
                role(Admin) =:= Role,
                flatten_username(#{username => Username, role => Role, description => Desc})
            }
    end.

change_password(Username, OldPasswd, NewPasswd) when is_binary(Username) ->
    case check(Username, OldPasswd) of
        {ok, _} -> change_password(Username, NewPasswd);
        Error -> Error
    end.

change_password(Username, Password) when is_binary(Username), is_binary(Password) ->
    case legal_password(Password) of
        ok -> change_password_hash(Username, hash(Password));
        Error -> Error
    end.

change_password_hash(Username, PasswordHash) ->
    ChangePWD =
        fun(User) ->
            User#?ADMIN{pwdhash = PasswordHash}
        end,
    case update_pwd(Username, ChangePWD) of
        {ok, Result} ->
            _ = emqx_dashboard_token:destroy_by_username(Username),
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

update_pwd(Username, Fun) ->
    Trans =
        fun() ->
            User =
                case lookup_user(Username) of
                    [Admin] -> Admin;
                    [] -> mnesia:abort(<<"username_not_found">>)
                end,
            mnesia:write(Fun(User))
        end,
    return(mria:sync_transaction(?DASHBOARD_SHARD, Trans)).

%% used to directly update user data
unsafe_update_user(User) ->
    Trans = fun() -> mnesia:write(User) end,
    case mria:sync_transaction(?DASHBOARD_SHARD, Trans) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

-spec lookup_user(dashboard_username()) -> [emqx_admin()].
lookup_user(Username) ->
    Fun = fun() -> mnesia:read(?ADMIN, Username) end,
    {atomic, User} = mria:ro_transaction(?DASHBOARD_SHARD, Fun),
    User.

-spec all_users() -> [map()].
all_users() ->
    lists:map(
        fun(
            #?ADMIN{
                username = Username,
                description = Desc,
                role = Role
            }
        ) ->
            flatten_username(#{
                username => Username,
                description => Desc,
                role => ensure_role(Role)
            })
        end,
        ets:tab2list(?ADMIN)
    ).
-spec return({atomic | aborted, term()}) -> {ok, term()} | {error, Reason :: binary()}.
return({atomic, Result}) ->
    {ok, Result};
return({aborted, Reason}) ->
    {error, Reason}.

check(undefined, _) ->
    {error, <<"username_not_provided">>};
check(_, undefined) ->
    {error, <<"password_not_provided">>};
check(Username, Password) ->
    case lookup_user(Username) of
        [#?ADMIN{pwdhash = PwdHash} = User] ->
            case verify_hash(Password, PwdHash) of
                ok -> {ok, User};
                error -> {error, <<"password_error">>}
            end;
        [] ->
            {error, <<"username_not_found">>}
    end.

%%--------------------------------------------------------------------
%% token
sign_token(Username, Password) ->
    ExpiredTime = emqx:get_config([dashboard, password_expired_time], 0),
    maybe
        {ok, User} ?= check(Username, Password),
        {ok, Result} ?= verify_password_expiration(ExpiredTime, User),
        {ok, Role, Token} ?= emqx_dashboard_token:sign(User, Password),
        {ok, Result#{role => Role, token => Token}}
    end.

-spec verify_token(_, Token :: binary()) ->
    Result ::
        {ok, binary()}
        | {error, token_timeout | not_found | unauthorized_role}.
verify_token(Req, Token) ->
    emqx_dashboard_token:verify(Req, Token).

destroy_token_by_username(Username, Token) ->
    case emqx_dashboard_token:lookup(Token) of
        {ok, #?ADMIN_JWT{username = Username}} ->
            emqx_dashboard_token:destroy(Token);
        _ ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
default_username() ->
    binenv(default_username).

binenv(Key) ->
    iolist_to_binary(emqx_conf:get([dashboard, Key], "")).

add_default_user(Username, Password) when ?EMPTY_KEY(Username) orelse ?EMPTY_KEY(Password) ->
    {ok, empty};
add_default_user(Username, Password) ->
    case lookup_user(Username) of
        [] ->
            case do_add_user(Username, Password, ?ROLE_SUPERUSER, <<"administrator">>) of
                {error, ?USERNAME_ALREADY_EXISTS_ERROR} ->
                    %% race condition: multiple nodes booting at the same time?
                    {ok, default_user_exists};
                Res ->
                    Res
            end;
        _ ->
            {ok, default_user_exists}
    end.

%% ensure the `role` is correct when it is directly read from the table
%% this value in old data is `undefined`
-dialyzer({no_match, ensure_role/1}).
ensure_role(undefined) ->
    ?ROLE_SUPERUSER;
ensure_role(Role) when is_binary(Role) ->
    Role.

-if(?EMQX_RELEASE_EDITION == ee).
legal_role(Role) ->
    emqx_dashboard_rbac:valid_dashboard_role(Role).

role(Data) ->
    emqx_dashboard_rbac:role(Data).

flatten_username(#{username := ?SSO_USERNAME(Backend, Name)} = Data) ->
    Data#{
        username := Name,
        backend => Backend
    };
flatten_username(#{username := Username} = Data) when is_binary(Username) ->
    Data#{backend => ?BACKEND_LOCAL}.

-spec add_sso_user(dashboard_sso_backend(), binary(), dashboard_user_role(), binary()) ->
    {ok, map()} | {error, any()}.
add_sso_user(Backend, Username0, Role, Desc) when is_binary(Username0) ->
    case legal_role(Role) of
        ok ->
            Username = ?SSO_USERNAME(Backend, Username0),
            do_add_user(Username, <<>>, Role, Desc);
        {error, _} = Error ->
            Error
    end.

-spec lookup_user(dashboard_sso_backend(), binary()) -> [emqx_admin()].
lookup_user(Backend, Username) when is_atom(Backend) ->
    lookup_user(?SSO_USERNAME(Backend, Username)).
-else.

-dialyzer({no_match, [add_user/4, update_user/3]}).

legal_role(?ROLE_DEFAULT) ->
    ok;
legal_role(_) ->
    {error, <<"Role does not exist">>}.

role(_) ->
    ?ROLE_DEFAULT.

flatten_username(Data) ->
    Data.
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

legal_password_test() ->
    ?assertEqual({error, ?BAD_PASSWORD_LEN}, legal_password(<<"123">>)),
    MaxPassword = iolist_to_binary([lists:duplicate(63, "x"), "1"]),
    ?assertEqual(ok, legal_password(MaxPassword)),
    TooLongPassword = lists:duplicate(65, "y"),
    ?assertEqual({error, ?BAD_PASSWORD_LEN}, legal_password(TooLongPassword)),

    ?assertEqual({error, ?INVALID_PASSWORD_MSG}, legal_password(<<"12345678">>)),
    ?assertEqual({error, ?INVALID_PASSWORD_MSG}, legal_password(?LETTER)),
    ?assertEqual({error, ?INVALID_PASSWORD_MSG}, legal_password(?NUMBER)),
    ?assertEqual({error, ?INVALID_PASSWORD_MSG}, legal_password(?SPECIAL_CHARS)),
    ?assertEqual({error, ?INVALID_PASSWORD_MSG}, legal_password(<<"映映映映无天在请"/utf8>>)),
    ?assertEqual(
        {error, <<"Only ascii characters are allowed in the password">>},
        legal_password(<<"️test_for_non_ascii1中"/utf8>>)
    ),
    ?assertEqual(
        {error, <<"Only ascii characters are allowed in the password">>},
        legal_password(<<"云☁️test_for_unicode"/utf8>>)
    ),

    ?assertEqual(ok, legal_password(?LOW_LETTER_CHARS ++ ?NUMBER)),
    ?assertEqual(ok, legal_password(?UPPER_LETTER_CHARS ++ ?NUMBER)),
    ?assertEqual(ok, legal_password(?LOW_LETTER_CHARS ++ ?SPECIAL_CHARS)),
    ?assertEqual(ok, legal_password(?UPPER_LETTER_CHARS ++ ?SPECIAL_CHARS)),
    ?assertEqual(ok, legal_password(?SPECIAL_CHARS ++ ?NUMBER)),

    ?assertEqual(ok, legal_password(<<"abckldiekflkdf12">>)),
    ?assertEqual(ok, legal_password(<<"abckldiekflkdf w">>)),
    ?assertEqual(ok, legal_password(<<"# abckldiekflkdf w">>)),
    ?assertEqual(ok, legal_password(<<"# 12344858">>)),
    ?assertEqual(ok, legal_password(<<"# %12344858">>)),
    ok.

-endif.
