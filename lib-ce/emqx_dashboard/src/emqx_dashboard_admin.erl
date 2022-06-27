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

%% @doc Web dashboard admin authentication with username and password.

-module(emqx_dashboard_admin).

-behaviour(gen_server).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-define(DEFAULT_PASSWORD, <<"public">>).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Mnesia bootstrap
-export([mnesia/1]).

%% API Function Exports
-export([start_link/0]).

%% mqtt_admin api
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

%% gen_server Function Exports
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(mqtt_admin, [
                {type, set},
                {disc_copies, [node()]},
                {record_name, mqtt_admin},
                {attributes, record_info(fields, mqtt_admin)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(mqtt_admin, disc_copies).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(add_user(binary(), binary(), binary()) -> ok | {error, any()}).
add_user(Username, Password, Tags) when is_binary(Username), is_binary(Password) ->
    case emqx_misc:is_sane_id(Username) of
        ok ->
            Admin = #mqtt_admin{username = Username, password = hash(Password), tags = Tags},
            return(mnesia:transaction(fun add_user_/1, [Admin]));
        {error, Reason} -> {error, Reason}
    end.

force_add_user(Username, Password, Tags) ->
    case emqx_misc:is_sane_id(Username) of
        ok ->
            AddFun = fun() ->
                mnesia:write(#mqtt_admin{username = Username, password = Password, tags = Tags})
                     end,
            case mnesia:transaction(AddFun) of
                {atomic, ok} -> ok;
                {aborted, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% @private
add_user_(Admin = #mqtt_admin{username = Username}) ->
    case mnesia:wread({mqtt_admin, Username}) of
        []  -> mnesia:write(Admin);
        [_] -> mnesia:abort(<<"Username Already Exist">>)
    end.

-spec(remove_user(binary()) -> ok | {error, any()}).
remove_user(Username) when is_binary(Username) ->
    Trans = fun() ->
                    case lookup_user(Username) of
                    [] ->
                        mnesia:abort(<<"Username Not Found">>);
                    _  -> ok
                    end,
                    mnesia:delete({mqtt_admin, Username})
            end,
    return(mnesia:transaction(Trans)).

-spec(update_user(binary(), binary()) -> ok | {error, term()}).
update_user(Username, Tags) when is_binary(Username) ->
    return(mnesia:transaction(fun update_user_/2, [Username, Tags])).

%% @private
update_user_(Username, Tags) ->
    case mnesia:wread({mqtt_admin, Username}) of
        [] -> mnesia:abort(<<"Username Not Found">>);
        [Admin] -> mnesia:write(Admin#mqtt_admin{tags = Tags})
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
                        User#mqtt_admin{password = PasswordHash}
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
    return(mnesia:transaction(Trans)).


-spec(lookup_user(binary()) -> [mqtt_admin()]).
lookup_user(Username) when is_binary(Username) ->
    IsDefaultUser = binenv(default_user_username) =:= Username,
    case mnesia:dirty_read(mqtt_admin, Username) of
        [] when IsDefaultUser ->
            _ = ensure_default_user_in_db(Username),
            %% try to read again
            mnesia:dirty_read(mqtt_admin, Username);
        Res ->
            Res
    end.

-spec(all_users() -> [#mqtt_admin{}]).
all_users() -> ets:tab2list(mqtt_admin).

return({atomic, _}) ->
    ok;
return({aborted, Reason}) ->
    {error, Reason}.

check(undefined, _) ->
    {error, <<"Username undefined">>};
check(_, undefined) ->
    {error, <<"Password undefined">>};
check(Username, Password) ->
    case lookup_user(Username) of
        [#mqtt_admin{password = PwdHash}] ->
            case is_valid_pwd(PwdHash, Password) of
                true  ->
                    ok;
                false ->
                    ok = bad_login_penalty(),
                    {error, <<"Username/Password error">>}
            end;
        [] ->
            ok = bad_login_penalty(),
            {error, <<"Username/Password error">>}
    end.

bad_login_penalty() ->
    timer:sleep(2000),
    ok.

is_valid_pwd(<<Salt:4/binary, Hash/binary>>, Password) ->
    Hash =:= md5_hash(Salt, Password).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    case binenv(default_user_username) of
        <<>> -> ok;
        UserName ->
            %% Add default admin user
            {ok, _} = mnesia:subscribe({table, mqtt_admin, simple}),
            PasswordHash = ensure_default_user_in_db(UserName),
            ok = ensure_default_user_passwd_hashed_in_pt(PasswordHash),
            ok = maybe_warn_default_pwd()
    end,
    {ok, state}.

handle_call(_Req, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({mnesia_table_event, {write, Admin, _}}, State) ->
    %% the password is changed from another node, sync it to persistent_term
    #mqtt_admin{username = Username, password = HashedPassword} = Admin,
    case binenv(default_user_username) of
        Username ->
            ok = ensure_default_user_passwd_hashed_in_pt(HashedPassword);
        _ ->
            ignore
    end,
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

hash(Password) ->
    SaltBin = salt(),
    <<SaltBin/binary, (md5_hash(SaltBin, Password))/binary>>.

md5_hash(SaltBin, Password) ->
    erlang:md5(<<SaltBin/binary, Password/binary>>).

salt() ->
    _ = emqx_misc:rand_seed(),
    Salt = rand:uniform(16#ffffffff),
    <<Salt:32>>.

binenv(Key) ->
    iolist_to_binary(application:get_env(emqx_dashboard, Key, <<>>)).

ensure_default_user_in_db(<<>>) -> <<>>;
ensure_default_user_in_db(Username) ->
    F =
        fun() ->
                case mnesia:wread({mqtt_admin, Username}) of
                    [] ->
                        PasswordHash = initial_default_user_passwd_hashed(),
                        Admin = #mqtt_admin{username = Username,
                                            password = PasswordHash,
                                            tags = <<"administrator">>},
                        ok = mnesia:write(Admin),
                        PasswordHash;
                    [#mqtt_admin{password = PasswordHash}] ->
                        PasswordHash
                end
        end,
    {atomic, PwdHash} = mnesia:transaction(F),
    PwdHash.

initial_default_user_passwd_hashed() ->
    case get_default_user_passwd_hashed_from_pt() of
        Empty when ?EMPTY_KEY(Empty) ->
            case binenv(default_user_passwd) of
                Empty when ?EMPTY_KEY(Empty) -> hash(?DEFAULT_PASSWORD);
                Pwd -> hash(Pwd)
            end;
        PwdHash ->
            PwdHash
    end.

%% use this persistent_term for a copy of the value in mnesia database
%% so that after the node leaves a cluster, db gets purged,
%% we can still find the changed password back from PT
ensure_default_user_passwd_hashed_in_pt(Hashed) ->
    ok = persistent_term:put({?MODULE, default_user_passwd_hashed}, Hashed).

get_default_user_passwd_hashed_from_pt() ->
    persistent_term:get({?MODULE, default_user_passwd_hashed}, <<>>).

maybe_warn_default_pwd() ->
    case is_valid_pwd(initial_default_user_passwd_hashed(), ?DEFAULT_PASSWORD) of
        true ->
            ?LOG(warning,
                 "[Dashboard] Using default password for dashboard 'admin' user. "
                 "Please use './bin/emqx_ctl admins' command to change it. "
                 "NOTE: the default password in config file is only "
                 "used to initialise the database record, changing the config "
                 "file after database is initialised has no effect."
                );
        false ->
            ok
    end.
