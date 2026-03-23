%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_cli).

-include("emqx_dashboard.hrl").

-export([
    load/0,
    admins/1,
    api_keys/1,
    unload/0
]).

-export([bin/1, print_error/1]).

-define(CLI_MOD, emqx_dashboard_sso_cli).

load() ->
    emqx_ctl:register_command(admins, {?CLI_MOD, admins}, []),
    emqx_ctl:register_command(api_keys, {?MODULE, api_keys}, []).

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

api_keys(["list"]) ->
    print_json(lists:map(fun api_key_public_info/1, emqx_mgmt_auth:list()));
api_keys(["show" | Args]) ->
    case api_key_name_arg(Args) of
        {ok, Name} ->
            show_api_key(Name);
        {error, Reason} ->
            print_json_error(Reason)
    end;
api_keys(["add" | Args]) ->
    case parse_api_key_add_args(Args) of
        {ok, Opts} ->
            add_api_key(Opts);
        {error, Reason} ->
            print_json_error(Reason)
    end;
api_keys(["del" | Args]) ->
    case api_key_name_arg(Args) of
        {ok, Name} ->
            delete_api_key(Name);
        {error, Reason} ->
            print_json_error(Reason)
    end;
api_keys(["enable" | Args]) ->
    case api_key_name_arg(Args) of
        {ok, Name} ->
            update_api_key_enable(Name, true);
        {error, Reason} ->
            print_json_error(Reason)
    end;
api_keys(["disable" | Args]) ->
    case api_key_name_arg(Args) of
        {ok, Name} ->
            update_api_key_enable(Name, false);
        {error, Reason} ->
            print_json_error(Reason)
    end;
api_keys(_) ->
    emqx_ctl:usage(
        [
            {"api_keys list", "List API keys"},
            {"api_keys show --name <Name>", "Show API key details"},
            {
                "api_keys add --name <Name> [--api-secret <Secret>]\n"
                "[--valid-days <infinity|days>] [--role <Role>] [--desc <Desc>]",
                "Create API key"
            },
            {"api_keys enable --name <Name>", "Enable API key"},
            {"api_keys disable --name <Name>", "Disable API key"},
            {"api_keys del --name <Name>", "Delete API key"}
        ]
    ).

unload() ->
    emqx_ctl:unregister_command(admins),
    emqx_ctl:unregister_command(api_keys).

show_api_key(Name) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, APIKey} ->
            print_json(api_key_public_info(APIKey));
        {error, Reason} ->
            print_json_error(Reason)
    end.

add_api_key(Opts) ->
    Name = maps:get("--name", Opts),
    Desc = maps:get("--desc", Opts, default_api_key_desc()),
    Role = maps:get("--role", Opts, ?ROLE_API_DEFAULT),
    ExpiredAt = maps:get("--valid-days", Opts, infinity),
    case maps:find("--api-secret", Opts) of
        {ok, ApiSecret} ->
            case emqx_mgmt_auth:create(Name, ApiSecret, true, ExpiredAt, Desc, Role) of
                {ok, APIKey} ->
                    print_json(api_key_public_info(APIKey));
                {error, Reason} ->
                    print_json_error(Reason)
            end;
        error ->
            case emqx_mgmt_auth:create(Name, true, ExpiredAt, Desc, Role) of
                {ok, APIKey} ->
                    print_json(api_key_create_info(APIKey));
                {error, Reason} ->
                    print_json_error(Reason)
            end
    end.

delete_api_key(Name) ->
    case emqx_mgmt_auth:delete(Name) of
        {ok, _} ->
            print_json(#{result => <<"ok">>, name => Name});
        {error, Reason} ->
            print_json_error(Reason)
    end.

update_api_key_enable(Name, Enable) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, #{desc := Desc, role := Role, expired_at := ExpiredAt}} ->
            case emqx_mgmt_auth:update(Name, Enable, ExpiredAt, Desc, Role) of
                {ok, APIKey} ->
                    print_json(api_key_public_info(APIKey));
                {error, Reason} ->
                    print_json_error(Reason)
            end;
        {error, Reason} ->
            print_json_error(Reason)
    end.

api_key_public_info(APIKey) ->
    maps:without([api_secret], APIKey).

api_key_create_info(APIKey) ->
    (api_key_public_info(APIKey))#{
        api_secret => maps:get(api_secret, APIKey)
    }.

api_key_name_arg(Args) ->
    case collect_api_key_args(Args) of
        {ok, Opts} ->
            case maps:find("--name", Opts) of
                {ok, Name} ->
                    case ensure_allowed_opts(Opts, ["--name"]) of
                        ok -> {ok, Name};
                        {error, _} = Error -> Error
                    end;
                error ->
                    {error, <<"missing required option: --name">>}
            end;
        {error, _} = Error ->
            Error
    end.

parse_api_key_add_args(Args) ->
    case collect_api_key_args(Args) of
        {ok, Opts} ->
            case maps:find("--name", Opts) of
                {ok, _Name} ->
                    case
                        ensure_allowed_opts(Opts, [
                            "--name", "--desc", "--role", "--api-secret", "--valid-days"
                        ])
                    of
                        ok -> {ok, Opts};
                        {error, _} = Error -> Error
                    end;
                error ->
                    {error, <<"missing required option: --name">>}
            end;
        {error, _} = Error ->
            Error
    end.

collect_api_key_args(Args) ->
    do_collect_api_key_args(Args, #{}).

do_collect_api_key_args([], Acc) ->
    {ok, Acc};
do_collect_api_key_args(["--name", Name | Rest], Acc) ->
    do_collect_api_key_args(Rest, Acc#{"--name" => bin(Name)});
do_collect_api_key_args(["--desc", Desc | Rest], Acc) ->
    do_collect_api_key_args(Rest, Acc#{"--desc" => bin(Desc)});
do_collect_api_key_args(["--role", Role | Rest], Acc) ->
    do_collect_api_key_args(Rest, Acc#{"--role" => bin(Role)});
do_collect_api_key_args(["--api-secret", Secret | Rest], Acc) ->
    do_collect_api_key_args(Rest, Acc#{"--api-secret" => bin(Secret)});
do_collect_api_key_args(["--valid-days", Days | Rest], Acc) ->
    case parse_valid_days(Days) of
        {ok, ValidDays} ->
            do_collect_api_key_args(Rest, Acc#{"--valid-days" => ValidDays});
        {error, _} = Error ->
            Error
    end;
do_collect_api_key_args([Option], _Acc) ->
    case lists:prefix("--", Option) of
        true ->
            {error, iolist_to_binary(io_lib:format("missing value for option: ~ts", [Option]))};
        false ->
            {error, iolist_to_binary(io_lib:format("bad argument: ~ts", [Option]))}
    end;
do_collect_api_key_args([Arg | _Rest], _Acc) ->
    {error, iolist_to_binary(io_lib:format("bad argument: ~ts", [Arg]))}.

ensure_allowed_opts(Opts, Allowed) ->
    case maps:keys(Opts) -- Allowed of
        [] ->
            ok;
        [Unknown | _] ->
            {error, iolist_to_binary(io_lib:format("unknown option: ~ts", [Unknown]))}
    end.

parse_valid_days("infinity") ->
    {ok, infinity};
parse_valid_days(DaysStr) ->
    try
        Days = list_to_integer(DaysStr),
        case Days > 0 of
            true ->
                {ok, erlang:system_time(second) + Days * 24 * 60 * 60};
            false ->
                {error, <<"--valid-days must be infinity or a positive integer">>}
        end
    catch
        error:badarg ->
            {error, <<"--valid-days must be infinity or a positive integer">>}
    end.

default_api_key_desc() ->
    Timestamp = emqx_utils_calendar:epoch_to_rfc3339(erlang:system_time(second), second),
    <<"Created from emqx ctl command at ", Timestamp/binary>>.

print_json(Info) ->
    emqx_ctl:print("~ts~n", [emqx_utils_json:best_effort_json(Info)]).

print_json_error(Reason) ->
    print_json(#{error => emqx_utils:readable_error_msg(Reason)}).

bin(S) -> unicode:characters_to_binary(S).

print_error(Reason) when is_binary(Reason) ->
    emqx_ctl:print("Error: ~s~n", [Reason]);
print_error(#{reason := unknown_namespace, namespace := Namespace}) ->
    emqx_ctl:print("Error: unknown namespace: \"~s\"~n", [Namespace]);
print_error(Reason) ->
    emqx_ctl:print("Error: ~p~n", [Reason]).
