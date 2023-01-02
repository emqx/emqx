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

-module(emqx_acl_mnesia_cli).

-export([cli/1]).

%%--------------------------------------------------------------------
%% ACL Cli
%%--------------------------------------------------------------------

cli(["list"]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:all_acls()];

cli(["list", "clientid"]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:all_acls(clientid)];

cli(["list", "username"]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:all_acls(username)];

cli(["list", "_all"]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:all_acls(all)];

cli(["add", "clientid", Clientid, Topic, Action, Access]) ->
    case validate(action, Action) andalso validate(access, Access) of
        true ->
            case emqx_acl_mnesia_db:add_acl(
                   {clientid, iolist_to_binary(Clientid)},
                   iolist_to_binary(Topic),
                   list_to_existing_atom(Action),
                   list_to_existing_atom(Access)
                  ) of
                ok -> emqx_ctl:print("ok~n");
                {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
            end;
        _ ->
             emqx_ctl:print("Error: Input is illegal~n")
    end;

cli(["add", "username", Username, Topic, Action, Access]) ->
    case validate(action, Action) andalso validate(access, Access) of
        true ->
            case emqx_acl_mnesia_db:add_acl(
                   {username, iolist_to_binary(Username)},
                   iolist_to_binary(Topic),
                   list_to_existing_atom(Action),
                   list_to_existing_atom(Access)
                  ) of
                ok -> emqx_ctl:print("ok~n");
                {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
            end;
        _ ->
             emqx_ctl:print("Error: Input is illegal~n")
    end;

cli(["add", "_all", Topic, Action, Access]) ->
    case validate(action, Action) andalso validate(access, Access) of
        true ->
            case emqx_acl_mnesia_db:add_acl(
                   all,
                   iolist_to_binary(Topic),
                   list_to_existing_atom(Action),
                   list_to_existing_atom(Access)
                  ) of
                ok -> emqx_ctl:print("ok~n");
                {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
            end;
        _ ->
             emqx_ctl:print("Error: Input is illegal~n")
    end;

cli(["show", "clientid", Clientid]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:lookup_acl({clientid, iolist_to_binary(Clientid)})];

cli(["show", "username", Username]) ->
    [print_acl(Acl) || Acl <- emqx_acl_mnesia_db:lookup_acl({username, iolist_to_binary(Username)})];

cli(["del", "clientid", Clientid, Topic])->
    cli(["delete", "clientid", Clientid, Topic]);

cli(["delete", "clientid", Clientid, Topic])->
    case emqx_acl_mnesia_db:remove_acl({clientid, iolist_to_binary(Clientid)}, iolist_to_binary(Topic)) of
         ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

cli(["del", "username", Username, Topic])->
    cli(["delete", "username", Username, Topic]);

cli(["delete", "username", Username, Topic])->
    case emqx_acl_mnesia_db:remove_acl({username, iolist_to_binary(Username)}, iolist_to_binary(Topic)) of
         ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

cli(["del", "_all", Topic])->
    cli(["delete", "_all", Topic]);

cli(["delete", "_all", Topic])->
    case emqx_acl_mnesia_db:remove_acl(all, iolist_to_binary(Topic)) of
         ok -> emqx_ctl:print("ok~n");
        {error, Reason} -> emqx_ctl:print("Error: ~p~n", [Reason])
    end;

cli(_) ->
    emqx_ctl:usage([ {"acl list clientid", "List clientid acls"}
                   , {"acl list username", "List username acls"}
                   , {"acl list _all", "List $all acls"}
                   , {"acl list ", "List all acls"}
                   , {"acl show clientid <Clientid>", "Lookup clientid acl detail"}
                   , {"acl show username <Username>", "Lookup username acl detail"}
                   , {"acl add clientid <Clientid> <Topic> <Action> <Access>", "Add clientid acl"}
                   , {"acl add username <Username> <Topic> <Action> <Access>", "Add username acl"}
                   , {"acl add _all <Topic> <Action> <Access>", "Add $all acl"}
                   , {"acl delete clientid <Clientid> <Topic>", "Delete clientid acl"}
                   , {"acl delete username <Username> <Topic>", "Delete username acl"}
                   , {"acl delete _all <Topic>", "Delete $all acl"}
                   ]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

validate(action, "pub") -> true;
validate(action, "sub") -> true;
validate(action, "pubsub") -> true;
validate(access, "allow") -> true;
validate(access, "deny") -> true;
validate(_, _) -> false.

print_acl({{clientid, Clientid}, Topic, Action, Access, _}) ->
    emqx_ctl:print(
        "Acl(clientid = ~p topic = ~p action = ~p access = ~p)~n",
        [Clientid, Topic, Action, Access]
      );
print_acl({{username, Username}, Topic, Action, Access, _}) ->
    emqx_ctl:print(
        "Acl(username = ~p topic = ~p action = ~p access = ~p)~n",
        [Username, Topic, Action, Access]
      );
print_acl({all, Topic, Action, Access, _}) ->
    emqx_ctl:print(
        "Acl($all topic = ~p action = ~p access = ~p)~n",
        [Topic, Action, Access]
     ).
