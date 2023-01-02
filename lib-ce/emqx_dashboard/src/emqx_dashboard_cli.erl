%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_cli).

-export([ load/0
        , admins/1
        , unload/0
        ]).

load() ->
    emqx_ctl:register_command(admins, {?MODULE, admins}, []).

admins(["add", Username, Password]) ->
    admins(["add", Username, Password, ""]);

admins(["add", Username, Password, Tag]) ->
    case emqx_dashboard_admin:add_user(bin(Username), bin(Password), bin(Tag)) of
        ok ->
            emqx_ctl:print("ok~n");
        {error, already_existed} ->
            emqx_ctl:print("Error: already existed~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

admins(["passwd", Username, Password]) ->
    Status  = emqx_dashboard_admin:change_password(bin(Username), bin(Password)),
    emqx_ctl:print("~p~n", [Status]);

admins(["del", Username]) ->
    Status  = emqx_dashboard_admin:remove_user(bin(Username)),
    emqx_ctl:print("~p~n", [Status]);

admins(_) ->
    emqx_ctl:usage([{"admins add <Username> <Password> <Tags>",  "Add dashboard user"},
                    {"admins passwd <Username> <Password>",      "Reset dashboard user password"},
                    {"admins del <Username>",                    "Delete dashboard user" }]).

unload() ->
    emqx_ctl:unregister_command(admins).

bin(S) -> iolist_to_binary(S).
