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

%% @doc Web dashboard admin authentication with username and password.

-module(emqx_dashboard_admin).

-include("emqx_dashboard.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-boot_mnesia({mnesia, [boot]}).

%% Mnesia bootstrap
-export([mnesia/1]).

-export([ add_user/3
        , force_add_user/3
        , remove_user/1
        , update_user/2
        , lookup_user/1
        , change_password/2
        , change_password/3
        , all_users/0
        , check/2
        ]).

-export([ sign_token/2
        , verify_token/1
        , destroy_token_by_username/2
        ]).

-export([add_default_user/0]).

-type emqx_admin() :: #?ADMIN{}.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = mria:create_table(?ADMIN, [
                {type, set},
                {rlog_shard, ?DASHBOARD_SHARD},
                {storage, disc_copies},
                {record_name, ?ADMIN},
                {attributes, record_info(fields, ?ADMIN)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(add_user(binary(), binary(), binary()) -> {ok, map()} | {error, any()}).
add_user(Username, Password, Desc)
  when is_binary(Username), is_binary(Password) ->
    return(mria:transaction(?DASHBOARD_SHARD, fun add_user_/3, [Username, Password, Desc])).

%% black-magic: force overwrite a user
force_add_user(Username, Password, Desc) ->
    AddFun = fun() ->
                 mnesia:write(#?ADMIN{username = Username,
                                      pwdhash = hash(Password),
                                      description = Desc})
             end,
    case mria:transaction(?DASHBOARD_SHARD, AddFun) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%% @private
add_user_(Username, Password, Desc) ->
    case mnesia:wread({?ADMIN, Username}) of
        []  ->
            Admin = #?ADMIN{username = Username, pwdhash = hash(Password), description = Desc},
            mnesia:write(Admin),
            #{username => Username, description => Desc};
        [_] ->
            mnesia:abort(<<"Username Already Exist">>)
    end.

-spec(remove_user(binary()) -> {ok, any()} | {error, any()}).
remove_user(Username) when is_binary(Username) ->
    Trans = fun() ->
                    case lookup_user(Username) of
                        [] -> mnesia:abort(<<"Username Not Found">>);
                        _  -> mnesia:delete({?ADMIN, Username})
                    end
            end,
    return(mria:transaction(?DASHBOARD_SHARD, Trans)).

-spec(update_user(binary(), binary()) -> {ok, map()} | {error, term()}).
update_user(Username, Desc) when is_binary(Username) ->
    return(mria:transaction(?DASHBOARD_SHARD, fun update_user_/2, [Username, Desc])).

%% @private
update_user_(Username, Desc) ->
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            mnesia:abort(<<"Username Not Found">>);
        [Admin] ->
            mnesia:write(Admin#?ADMIN{description = Desc}),
            #{username => Username, description => Desc}
    end.

change_password(Username, OldPasswd, NewPasswd) when is_binary(Username) ->
    case check(Username, OldPasswd) of
        ok -> change_password(Username, NewPasswd);
        Error -> Error
    end.

change_password(Username, Password) when is_binary(Username), is_binary(Password) ->
    change_password_hash(Username, hash(Password)).

change_password_hash(Username, PasswordHash) ->
    update_pwd(Username, fun(User) ->
                        User#?ADMIN{pwdhash = PasswordHash}
                end).

update_pwd(Username, Fun) ->
    Trans = fun() ->
                    User =
                    case lookup_user(Username) of
                    [Admin] -> Admin;
                    [] ->
                           mnesia:abort(<<"Username Not Found">>)
                    end,
                    mnesia:write(Fun(User))
            end,
    return(mria:transaction(?DASHBOARD_SHARD, Trans)).


-spec(lookup_user(binary()) -> [emqx_admin()]).
lookup_user(Username) when is_binary(Username) ->
    Fun = fun() -> mnesia:read(?ADMIN, Username) end,
    {atomic, User} = mria:ro_transaction(?DASHBOARD_SHARD, Fun),
    User.

-spec(all_users() -> [map()]).
all_users() ->
    lists:map(fun(#?ADMIN{username = Username,
                          description = Desc
                         }) ->
                      #{username => Username,
                        description => Desc
                       }
              end, ets:tab2list(?ADMIN)).

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
        [#?ADMIN{pwdhash = <<Salt:4/binary, Hash/binary>>}] ->
            case Hash =:= sha256(Salt, Password) of
                true  -> ok;
                false -> {error, <<"BAD_USERNAME_OR_PASSWORD">>}
            end;
        [] ->
            {error, <<"BAD_USERNAME_OR_PASSWORD">>}
    end.

%%--------------------------------------------------------------------
%% token
sign_token(Username, Password) ->
    case check(Username, Password) of
        ok ->
            emqx_dashboard_token:sign(Username, Password);
        Error ->
            Error
    end.

verify_token(Token) ->
    emqx_dashboard_token:verify(Token).

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

hash(Password) ->
    SaltBin = emqx_dashboard_token:salt(),
    <<SaltBin/binary, (sha256(SaltBin, Password))/binary>>.

sha256(SaltBin, Password) ->
    crypto:hash('sha256', <<SaltBin/binary, Password/binary>>).

add_default_user() ->
    add_default_user(binenv(default_username), binenv(default_password)).

binenv(Key) ->
    iolist_to_binary(emqx_conf:get([emqx_dashboard, Key], "")).

add_default_user(Username, Password) when ?EMPTY_KEY(Username) orelse ?EMPTY_KEY(Password) ->
    ok;

add_default_user(Username, Password) ->
    case lookup_user(Username) of
        [] -> add_user(Username, Password, <<"administrator">>);
        _  -> {ok, default_user_exists}
    end.
