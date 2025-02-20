%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_cli).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([admins/1]).

admins(["add", Username, Password]) ->
    admins(["add", Username, Password, ""]);
admins(["add", Username, Password, Desc]) ->
    case emqx_dashboard_admin:add_user(bin(Username), bin(Password), ?ROLE_DEFAULT, bin(Desc)) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["add", Username, Password, Desc, Role]) ->
    case emqx_dashboard_admin:add_user(bin(Username), bin(Password), bin(Role), bin(Desc)) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["passwd", Username, Password]) ->
    case emqx_dashboard_admin:change_password_trusted(bin(Username), bin(Password)) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["del", Username]) ->
    delete_user(bin(Username));
admins(["del", Username, BackendName]) ->
    case atom(BackendName) of
        {ok, ?BACKEND_LOCAL} ->
            delete_user(bin(Username));
        {ok, Backend} ->
            delete_user(?SSO_USERNAME(Backend, bin(Username)));
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["mfa", Username, "disable" | _]) ->
    case emqx_dashboard_admin:disable_mfa(bin(Username)) of
        {ok, ok} ->
            ok;
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["mfa", Username, Action]) ->
    admins(["mfa", Username, Action, "totp"]);
admins(["mfa", Username, "enable", Mechanism0]) ->
    try emqx_dashboard_mfa:mechanism(bin(Mechanism0)) of
        Mechanism ->
            case emqx_dashboard_admin:enable_mfa(bin(Username), Mechanism) of
                ok ->
                    emqx_ctl:print("ok~n");
                {error, Reason} ->
                    print_error(Reason)
            end
    catch
        throw:unsupported_mfa_mechanism ->
            print_error(<<"Unsupported MFA mechanism">>)
    end;
admins(_) ->
    emqx_ctl:usage(
        [
            {"admins add <Username> <Password> <Description> <Role>", "Add dashboard user"},
            {"admins passwd <Username> <Password>", "Reset dashboard user password"},
            {"admins del <Username> <Backend>",
                "Delete dashboard user, <Backend> can be omitted, the default value is 'local'"},
            {"admins mfa <Username> disable", "disable Multi-factor authentication for the user"},
            {"admins mfa <Username> enable [<Mechanism>]",
                "Enable Multi-factor authentication. "
                "Default mechanism is 'totp' (Authenticator App)"}
        ]
    ).

atom(S) ->
    emqx_utils:safe_to_existing_atom(S).

delete_user(Username) ->
    case emqx_dashboard_admin:remove_user(Username) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end.

bin(X) -> iolist_to_binary(X).

print_error(X) ->
    emqx_dashboard_cli:print_error(X).
