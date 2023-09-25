%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    case emqx_dashboard_admin:remove_user(bin(Username)) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(["del", Username, Backend]) ->
    case emqx_dashboard_admin:remove_user(?SSO_USERNAME(atom(Backend), bin(Username))) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(_) ->
    emqx_ctl:usage(
        [
            {"admins add <Username> <Password> <Description> <Role>", "Add dashboard user"},
            {"admins passwd <Username> <Password>", "Reset dashboard user password"},
            {"admins del <Username> <Backend>", "Delete dashboard user"}
        ]
    ).

atom(S) ->
    erlang:list_to_atom(S).
