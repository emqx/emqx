%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_cli).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([admins/1]).

-import(emqx_dashboard_cli, [bin/1, print_error/1]).

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
    case emqx_dashboard_admin:change_password(bin(Username), bin(Password)) of
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
admins(_) ->
    emqx_ctl:usage(
        [
            {"admins add <Username> <Password> <Description> <Role>", "Add dashboard user"},
            {"admins passwd <Username> <Password>", "Reset dashboard user password"},
            {"admins del <Username> <Backend>",
                "Delete dashboard user, <Backend> can be omitted, the default value is 'local'"}
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
