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

-module(emqx_authz_redis).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% ACL Callbacks
-export([ authorize/4
        , description/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with redis".

authorize(Client, PubSub, Topic,
            #{<<"resource_id">> := ResourceID,
              <<"cmd">> := CMD 
             }) ->
    NCMD = string:tokens(replvar(CMD, Client), " "),
    case emqx_resource:query(ResourceID, {cmd, NCMD}) of
        {ok, []} -> nomatch;
        {ok, Rows} ->
            do_authorize(Client, PubSub, Topic, Rows);
        {error, Reason} ->
            ?LOG(error, "[AuthZ] Query redis error: ~p", [Reason]),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [TopicFilter, Action | Tail]) ->
    case match(Client, PubSub, Topic, 
               #{topics => TopicFilter,
                 action => Action
                }) 
    of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

match(Client, PubSub, Topic, 
      #{topics := TopicFilter,
        action := Action
       }) ->
    Rule = #{<<"principal">> => all,
             <<"topics">> => [TopicFilter],
             <<"action">> => Action,
             <<"permission">> => allow
            },
    #{<<"simple_rule">> := NRule
     } = hocon_schema:check_plain(
            emqx_authz_schema,
            #{<<"simple_rule">> => Rule},
            #{},
            [simple_rule]),
    case emqx_authz:match(Client, PubSub, Topic, emqx_authz:compile(NRule)) of
        true -> {matched, allow};
        false -> nomatch
    end.

replvar(Cmd, Client = #{cn := CN}) ->
    replvar(repl(Cmd, "%C", CN), maps:remove(cn, Client));
replvar(Cmd, Client = #{dn := DN}) ->
    replvar(repl(Cmd, "%d", DN), maps:remove(dn, Client));
replvar(Cmd, Client = #{clientid := ClientId}) ->
    replvar(repl(Cmd, "%c", ClientId), maps:remove(clientid, Client));
replvar(Cmd, Client = #{username := Username}) ->
    replvar(repl(Cmd, "%u", Username), maps:remove(username, Client));
replvar(Cmd, _) ->
    Cmd.

repl(S, _Var, undefined) ->
    S;
repl(S, Var, Val) ->
    NVal = re:replace(Val, "&", "\\\\&", [global, {return, list}]),
    re:replace(S, Var, NVal, [{return, list}]).
