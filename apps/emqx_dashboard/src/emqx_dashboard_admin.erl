%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Web dashboard admin authentication with username and password.

-module(emqx_dashboard_admin).

-feature(maybe_expr, enable).

-include("emqx_dashboard.hrl").
-include("emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-behaviour(emqx_db_backup).

-export([create_tables/0]).

-export([
    add_user/4,
    disable_mfa/1,
    clear_mfa_state/1,
    set_mfa_state/2,
    get_mfa_state/1,
    clear_login_lock/1,
    set_login_lock/2,
    get_login_lock/1,
    force_add_user/5,
    remove_user/1,
    update_user/3,
    lookup_user/1,
    change_password_trusted/2,
    change_password/3,
    enable_mfa/2,
    all_users/0,
    admin_users/0,
    check/2,
    check/3
]).

-export([
    sign_token/2,
    sign_token/3,
    verify_token/3,
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

-export([role/1, namespace_of/1]).

-export([backup_tables/0]).

-export([add_sso_user/4, lookup_user/2]).

-export([remove_all_users_from_namespace/1]).

-ifdef(TEST).
-export([unsafe_update_user/1, default_password/0]).
-endif.

-export_type([
    dashboard_sso_backend/0,
    dashboard_username/0,
    dashboard_user_role/0,
    emqx_admin/0,
    actor_props/0
]).

-type emqx_admin() :: #?ADMIN{}.

-type actor_props() :: #{
    role := role(),
    namespace := emqx_config:maybe_namespace()
}.
-type role() :: binary().

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
    add_default_user(binenv(default_username), default_password()).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec add_user(
    dashboard_username(), _Password :: binary(), dashboard_user_role(), _Desc :: binary()
) ->
    {ok, map()} | {error, any()}.
add_user(Username, Password, Role0, Desc) when is_binary(Username), is_binary(Password) ->
    maybe
        ok ?= legal_username(Username),
        ok ?= legal_password(Password),
        {ok, ParsedRole} ?= parse_role(Role0),
        #{?role := Role} = ParsedRole,
        Extra = parsed_role_to_extra(ParsedRole),
        ActorProps = Extra#{
            ?role => Role
        },
        ok ?= emqx_hooks:run_fold('api_actor.pre_create', [ActorProps], ok),
        do_add_user(Username, Password, Role, Desc, Extra)
    end.

parsed_role_to_extra(#{?namespace := Ns}) ->
    #{?namespace => Ns};
parsed_role_to_extra(_) ->
    #{?namespace => ?global_ns}.

do_add_user(Username, Password, Role, Desc, Extra) ->
    Res = mria:sync_transaction(
        ?DASHBOARD_SHARD,
        fun add_user_/5,
        [Username, Password, Role, Desc, Extra]
    ),
    return(Res).

get_mfa_enabled_state(Username) ->
    case get_mfa_state(Username) of
        {ok, disabled} ->
            {error, disabled};
        Result ->
            Result
    end.

get_mfa_state(Username) ->
    case get_extra(Username) of
        {ok, #{mfa_state := S}} ->
            {ok, S};
        {ok, _} ->
            {error, no_mfa_state};
        {error, _} = Error ->
            Error
    end.

%% @doc Delete MFA state from user record.
%% This should allow the user to re-initialize MFA state according to `default_mfa' config.
clear_mfa_state(Username) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun clear_mfa_state2/1, [Username]),
    return(Res).

clear_mfa_state2(Username) ->
    update_extra(Username, fun(Extra) -> maps:without([mfa_state], Extra) end).

%% @doc Change `mfa_state' to `disabled'.
disable_mfa(Username) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun disable_mfa2/1, [Username]),
    return(Res).

disable_mfa2(Username) ->
    update_extra(Username, fun(Extra) -> Extra#{mfa_state => disabled} end).

%% @doc Enable MFA state.
%% Return error if it's already enabled.
enable_mfa(Username, Mechanism) ->
    case get_mfa_enabled_state(Username) of
        {ok, #{mechanism := Mechanism0}} ->
            {error, binfmt("MFA is already enabled using '~p'", [Mechanism0])};
        {error, username_not_found} ->
            {error, <<"username_not_found">>};
        {error, _} ->
            reinit_mfa(Username, Mechanism)
    end.

reinit_mfa(Username, Mechanism) ->
    {ok, State} = emqx_dashboard_mfa:init(Mechanism),
    {ok, ok} = set_mfa_state(Username, State),
    _ = emqx_dashboard_token:destroy_by_username(Username),
    ok.

%% @doc Set MFA state.
set_mfa_state(Username, MfaState) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun set_mfa_state2/2, [Username, MfaState]),
    return(Res).

set_mfa_state2(Username, MfaState) ->
    update_extra(Username, fun(Extra) -> Extra#{mfa_state => MfaState} end).

set_login_lock(Username, LockedUntil) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun set_login_lock2/2, [Username, LockedUntil]),
    return(Res).

set_login_lock2(Username, LockedUntil) ->
    update_extra(Username, fun(Extra) -> Extra#{login_lock => LockedUntil} end).

get_login_lock(Username) ->
    case get_extra(Username) of
        {ok, #{login_lock := LockedUntil}} ->
            {ok, LockedUntil};
        _ ->
            not_found
    end.

clear_login_lock(Username) ->
    Res = mria:sync_transaction(?DASHBOARD_SHARD, fun clear_login_lock2/1, [Username]),
    return(Res).

clear_login_lock2(Username) ->
    update_extra(Username, fun(Extra) -> maps:without([login_lock], Extra) end).

%% @doc Management of `extra' field.

update_extra(Username, Fun) ->
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            mnesia:abort(<<"username_not_found">>);
        [#?ADMIN{extra = Extra0} = Admin] ->
            ok = mnesia:write(Admin#?ADMIN{extra = Fun(Extra0)})
    end.

get_extra(Username) ->
    case ets:lookup(?ADMIN, Username) of
        [#?ADMIN{extra = Extra}] ->
            {ok, Extra};
        [] ->
            {error, username_not_found}
    end.

%% 0-9 or A-Z or a-z or $_
legal_username(<<>>) ->
    {error, <<"Username cannot be empty">>};
legal_username(Username) ->
    case re:run(Username, "^[_a-zA-Z0-9]*$", [{capture, none}]) of
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
force_add_user(Username, Password, Role, Desc, Extra) ->
    AddFun = fun() ->
        mnesia:write(#?ADMIN{
            username = Username,
            pwdhash = hash(Password),
            role = Role,
            description = Desc,
            extra = Extra#{password_ts => erlang:system_time(second)}
        })
    end,
    case mria:sync_transaction(?DASHBOARD_SHARD, AddFun) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%% @private
add_user_(Username, Password, Role, Desc, Extra) ->
    Namespace = maps:get(?namespace, Extra, ?global_ns),
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            Admin = #?ADMIN{
                username = Username,
                pwdhash = hash(Password),
                role = Role,
                description = Desc,
                extra = Extra#{password_ts => erlang:system_time(second)}
            },
            mnesia:write(Admin),
            ?SLOG(info, #{
                msg => "dashboard_sso_user_added",
                username => Username,
                ?role => Role,
                ?namespace => Namespace
            }),
            flatten_username(#{
                username => Username,
                ?role => Role,
                description => Desc,
                ?namespace => Namespace
            });
        [_] ->
            ?SLOG(info, #{
                msg => "dashboard_sso_user_add_failed",
                reason => "username_already_exists",
                username => Username,
                ?role => Role,
                ?namespace => Namespace
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
update_user(Username, Role0, Desc) ->
    case parse_role(Role0) of
        {ok, #{role := Role} = ParsedRole} ->
            Extra = parsed_role_to_extra(ParsedRole),
            case
                return(
                    mria:sync_transaction(
                        ?DASHBOARD_SHARD,
                        fun update_user_/4,
                        [Username, Role, Desc, Extra]
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

remove_all_users_from_namespace(Namespace) when is_binary(Namespace) ->
    MS = ets:fun2ms(
        fun(#?ADMIN{username = U, extra = #{?namespace := Ns}}) when Ns == Namespace ->
            U
        end
    ),
    Usernames = ets:select(?ADMIN, MS),
    lists:foreach(fun remove_user/1, Usernames).

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
update_user_(Username, Role, Desc, Extra) ->
    case mnesia:wread({?ADMIN, Username}) of
        [] ->
            mnesia:abort(<<"username_not_found">>);
        [Admin] ->
            #?ADMIN{extra = Extra0} = Admin,
            {OldExtra, NewExtra} =
                case is_map(Extra0) of
                    false -> {#{}, Extra};
                    true -> {Extra0, maps:merge(Extra0, Extra)}
                end,
            NewNamespace = maps:get(?namespace, NewExtra, ?global_ns),
            PreviousNamespace = maps:get(?namespace, OldExtra, ?global_ns),
            maybe
                true ?= PreviousNamespace /= NewNamespace,
                mnesia:abort(<<"changing_namespace_is_forbidden">>)
            end,
            mnesia:write(Admin#?ADMIN{role = Role, description = Desc, extra = NewExtra}),
            {
                role(Admin) =:= Role,
                flatten_username(#{
                    username => Username,
                    role => Role,
                    description => Desc,
                    ?namespace => maps:get(?namespace, NewExtra, ?global_ns)
                })
            }
    end.

%% @doc Change password from API/Dashboard, must check old password.
change_password(Username, OldPasswd, NewPasswd) when is_binary(Username) ->
    change_password(Username, OldPasswd, NewPasswd, ?TRUSTED_MFA_TOKEN).

%% @doc Change password from dashboard, must provide MFA token.
change_password(Username, OldPasswd, NewPasswd, MfaToken) ->
    case check(Username, OldPasswd, MfaToken) of
        {ok, _} -> change_password_trusted(Username, NewPasswd);
        Error -> Error
    end.

%% @doc Change password from CLI.
change_password_trusted(Username, Password) when is_binary(Username), is_binary(Password) ->
    case legal_password(Password) of
        ok ->
            ok = emqx_dashboard_login_lock:reset(Username),
            change_password_hash(Username, hash(Password));
        Error ->
            Error
    end.

change_password_hash(Username, PasswordHash) ->
    ChangePWD =
        fun(User) ->
            Extra = User#?ADMIN.extra,
            User#?ADMIN{
                pwdhash = PasswordHash,
                extra = Extra#{password_ts => erlang:system_time(second)}
            }
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
    lists:map(fun to_external_user/1, ets:tab2list(?ADMIN)).

-spec admin_users() -> [map()].
admin_users() ->
    MS = ets:fun2ms(fun(#?ADMIN{role = ?ROLE_SUPERUSER} = User) -> User end),
    Admins = ets:select(?ADMIN, MS),
    lists:map(fun to_external_user/1, Admins).

to_external_user(UserRecord) ->
    #?ADMIN{
        username = Username,
        description = Desc,
        role = Role,
        extra = Extra
    } = UserRecord,
    Namespace = namespace_of(UserRecord),
    flatten_username(#{
        username => Username,
        description => Desc,
        ?role => ensure_role(Role),
        ?namespace => Namespace,
        mfa => format_mfa(Extra)
    }).

format_mfa(#{mfa_state := #{mechanism := Mechanism}}) -> Mechanism;
format_mfa(#{mfa_state := disabled}) -> disabled;
format_mfa(_) -> none.

-spec return({atomic | aborted, term()}) -> {ok, term()} | {error, Reason :: binary()}.
return({atomic, Result}) ->
    {ok, Result};
return({aborted, Reason}) ->
    {error, Reason}.

check(Username, Password) ->
    check(Username, Password, ?NO_MFA_TOKEN).

check(undefined, _, _) ->
    {error, <<"username_not_provided">>};
check(_, undefined, _) ->
    {error, <<"password_not_provided">>};
check(Username, Password, MfaToken) ->
    case lookup_user(Username) of
        [#?ADMIN{pwdhash = PwdHash} = User] ->
            IsPwdOk = (ok =:= verify_hash(Password, PwdHash)),
            MfaVerifyResult = verify_mfa_token(Username, MfaToken, IsPwdOk),
            check_mfa_pwd(MfaVerifyResult, IsPwdOk, User);
        [] ->
            {error, <<"username_not_found">>}
    end.

check_mfa_pwd(ok, true, User) ->
    {ok, User};
check_mfa_pwd(ok, false, _User) ->
    {error, <<"password_error">>};
check_mfa_pwd({error, MfaError}, IsPwdOk, _User) ->
    %% MFA error
    case emqx_dashboard_mfa:is_need_setup_error(MfaError) of
        true when not IsPwdOk ->
            %% MFA needs setup, but password is NOT OK,
            %% return password check result.
            %% Because only valid login can trigger MFA initialization.
            %% Meaning: before MFA is setup, this user is still
            %% vulnerable to brute-force attack (like no MFA).
            {error, <<"password_error">>};
        _ ->
            %% - MFA needs setup (password is OK)
            %% - MFA is setup, but token is NOT provided
            %% - MFA is setup, but bad token prvoided
            {error, MfaError}
    end.

verify_mfa_token(_Username, ?TRUSTED_MFA_TOKEN, _IsPwdOk) ->
    %% e.g. when handing change_pwd request which is authenticated
    %% by bearer token
    ok;
verify_mfa_token(Username, MfaToken, IsPwdOk) ->
    ok = maybe_init_mfa_state(Username, IsPwdOk),
    do_verify_mfa_token(Username, MfaToken, IsPwdOk).

do_verify_mfa_token(Username, MfaToken, _IsPwdOk) when ?IS_NO_MFA_TOKEN(MfaToken) ->
    %% MFA token is not provided
    case get_mfa_enabled_state(Username) of
        {ok, State} ->
            %% Expected to receive MFA token, but not provided
            %% Disregard IsPwdOk because we do not want to return
            %% password_error when token is missing (so the to protect from
            %% brute password being forced)
            {error, emqx_dashboard_mfa:make_token_missing_error(State)};
        _ ->
            %% No MFA state, proceed to password check
            ok
    end;
do_verify_mfa_token(Username, MfaToken, IsPwdOk) ->
    %% MFA token is provided
    case get_mfa_enabled_state(Username) of
        {ok, State} ->
            %% MFA is configured, and a token is provided
            case emqx_dashboard_mfa:verify(State, MfaToken) of
                ok ->
                    %% Token is ok, no state change
                    ok;
                {ok, _NewState} when not IsPwdOk ->
                    %% Bad password, ignore state change
                    ok;
                {ok, NewState} when IsPwdOk ->
                    %% Token is ok, state changed, should update DB if password is OK
                    {ok, ok} = set_mfa_state(Username, NewState),
                    ok;
                {error, Reason} ->
                    %% Token is not OK
                    {error, Reason}
            end;
        _ ->
            %% MFA is not enabled, but provided with a token
            %% Can happen if MFA is disabled for this user but the
            %% dashboad is still providing a token for some reason,
            %% proceed to password check
            ok
    end.

%% Initialize MFA state if there is a default MFA settings configured.
maybe_init_mfa_state(Username, true) ->
    case emqx:get_config([dashboard, default_mfa], none) of
        none ->
            ok;
        #{mechanism := Mechanism} ->
            case get_mfa_state(Username) of
                {ok, _} ->
                    %% already enabled
                    %% or explicitly disabled
                    ok;
                _ ->
                    reinit_mfa(Username, Mechanism)
            end
    end;
maybe_init_mfa_state(_Username, false) ->
    %% do nothing
    ok.

%%--------------------------------------------------------------------
%% token

sign_token(Username, Password) ->
    sign_token(Username, Password, ?NO_MFA_TOKEN).

sign_token(Username, Password, MfaToken) ->
    ExpiredTime = emqx:get_config([dashboard, password_expired_time], 0),
    maybe
        ok ?= emqx_dashboard_login_lock:verify(Username),
        {ok, User} ?= check(Username, Password, MfaToken),
        {ok, Result} ?= verify_password_expiration(ExpiredTime, User),
        {ok, Role, Token, Namespace} ?= emqx_dashboard_token:sign(User),
        {ok, Result#{?role => Role, token => Token, namespace => Namespace}}
    end.

-spec verify_token(emqx_dashboard:request(), emqx_dashboard:handler_info(), Token :: binary()) ->
    {ok, emqx_dashboard_rbac:actor_context()}
    | {error, token_timeout | not_found | unauthorized_role}.
verify_token(Req, HandlerInfo, Token) ->
    emqx_dashboard_token:verify(Req, HandlerInfo, Token).

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
binfmt(Fmt, Args) ->
    iolist_to_binary(io_lib:format(Fmt, Args)).

default_username() ->
    binenv(default_username).

default_password() ->
    RawWrapped = emqx_conf:get([dashboard, default_password], <<"">>),
    Raw = emqx_secret:unwrap(RawWrapped),
    iolist_to_binary(Raw).

binenv(Key) ->
    iolist_to_binary(emqx_conf:get([dashboard, Key], "")).

add_default_user(Username, Password) when ?EMPTY_KEY(Username) orelse ?EMPTY_KEY(Password) ->
    {ok, empty};
add_default_user(Username, Password) ->
    case lookup_user(Username) of
        [] ->
            AllAdminUsers = admin_users(),
            OtherAdminUsers = lists:filter(
                fun(#{username := U}) -> U /= Username end, AllAdminUsers
            ),
            case OtherAdminUsers of
                [_ | _] ->
                    %% Other admins exist; no need to create default one.
                    {ok, other_admins_exist};
                [] ->
                    do_add_default_user(Username, Password)
            end;
        _ ->
            {ok, default_user_exists}
    end.

do_add_default_user(Username, Password) ->
    Extra = #{},
    maybe
        {error, ?USERNAME_ALREADY_EXISTS_ERROR} ?=
            do_add_user(Username, Password, ?ROLE_SUPERUSER, <<"administrator">>, Extra),
        %% race condition: multiple nodes booting at the same time?
        {ok, default_user_exists}
    end.

%% ensure the `role` is correct when it is directly read from the table
%% this value in old data is `undefined`
ensure_role(undefined) ->
    ?ROLE_SUPERUSER;
ensure_role(Role) when is_binary(Role) ->
    Role.

parse_role(Role) ->
    emqx_dashboard_rbac:parse_dashboard_role(Role).

%% For compatibility
role(#?ADMIN{role = undefined}) ->
    ?ROLE_SUPERUSER;
role(#?ADMIN{role = Role}) ->
    Role;
%% For compatibility
role([]) ->
    ?ROLE_SUPERUSER;
role(#{?role := Role}) ->
    Role;
role(Role) when is_binary(Role) ->
    Role.

namespace_of(#?ADMIN{extra = #{?namespace := Namespace}}) when is_binary(Namespace) ->
    Namespace;
namespace_of(#?ADMIN{}) ->
    ?global_ns.

flatten_username(#{username := ?SSO_USERNAME(Backend, Name)} = Data) ->
    Data#{
        username := Name,
        backend => Backend
    };
flatten_username(#{username := Username} = Data) when is_binary(Username) ->
    Data#{backend => ?BACKEND_LOCAL}.

-spec add_sso_user(dashboard_sso_backend(), binary(), dashboard_user_role(), binary()) ->
    {ok, map()} | {error, any()}.
add_sso_user(Backend, Username0, Role0, Desc) when is_binary(Username0) ->
    case parse_role(Role0) of
        {ok, #{?role := Role} = ParsedRole} ->
            Username = ?SSO_USERNAME(Backend, Username0),
            Extra = parsed_role_to_extra(ParsedRole),
            do_add_user(Username, <<>>, Role, Desc, Extra);
        {error, _} = Error ->
            Error
    end.

-spec lookup_user(dashboard_sso_backend(), binary()) -> [emqx_admin()].
lookup_user(Backend, Username) when is_atom(Backend) ->
    lookup_user(?SSO_USERNAME(Backend, Username)).

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
