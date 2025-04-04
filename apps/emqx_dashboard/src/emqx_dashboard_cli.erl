%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_cli).

-include("emqx_dashboard.hrl").

-export([
    load/0,
    admins/1,
    unload/0
]).

-export([bin/1, print_error/1]).

-if(?EMQX_RELEASE_EDITION == ee).
-define(CLI_MOD, emqx_dashboard_sso_cli).
-else.
-define(CLI_MOD, ?MODULE).
-endif.

load() ->
    emqx_ctl:register_command(admins, {?CLI_MOD, admins}, []).

admins(["add", Username, Password]) ->
    admins(["add", Username, Password, ""]);
admins(["add", Username, Password, Desc]) ->
    case emqx_dashboard_admin:add_user(bin(Username), bin(Password), ?ROLE_DEFAULT, bin(Desc)) of
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
    case emqx_dashboard_admin:remove_user(bin(Username)) of
        {ok, _} ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            print_error(Reason)
    end;
admins(_) ->
    emqx_ctl:usage(
        [
            {"admins add <Username> <Password> <Description>", "Add dashboard user"},
            {"admins passwd <Username> <Password>", "Reset dashboard user password"},
            {"admins del <Username>", "Delete dashboard user"}
        ]
    ).

unload() ->
    emqx_ctl:unregister_command(admins).

bin(S) -> unicode:characters_to_binary(S).

print_error(Reason) when is_binary(Reason) ->
    emqx_ctl:print("Error: ~s~n", [Reason]).
%% Maybe has more types of error, but there is only binary now. So close it for dialyzer.
% print_error(Reason) ->
%     emqx_ctl:print("Error: ~p~n", [Reason]).
