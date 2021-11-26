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
-include_lib("emqx/include/emqx_placeholder.hrl").

%% AuthZ Callbacks
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
            #{cmd := CMD,
              annotations := #{id := ResourceID}
             }) ->
    NCMD = string:tokens(replvar(CMD, Client), " "),
    case emqx_resource:query(ResourceID, {cmd, NCMD}) of
        {ok, []} -> nomatch;
        {ok, Rows} ->
            do_authorize(Client, PubSub, Topic, Rows);
        {error, Reason} ->
            ?SLOG(error, #{ msg => "query_redis_error"
                          , reason => Reason
                          , resource_id => ResourceID}),
            nomatch
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [TopicFilter, Action | Tail]) ->
    case emqx_authz_rule:match(Client, PubSub, Topic,
                               emqx_authz_rule:compile({allow, all, Action, [TopicFilter]})
                              )of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

replvar(Cmd, Client = #{cn := CN}) ->
    replvar(repl(Cmd, ?PH_S_CERT_CN_NAME, CN), maps:remove(cn, Client));
replvar(Cmd, Client = #{dn := DN}) ->
    replvar(repl(Cmd, ?PH_S_CERT_SUBJECT, DN), maps:remove(dn, Client));
replvar(Cmd, Client = #{clientid := ClientId}) ->
    replvar(repl(Cmd, ?PH_S_CLIENTID, ClientId), maps:remove(clientid, Client));
replvar(Cmd, Client = #{username := Username}) ->
    replvar(repl(Cmd, ?PH_S_USERNAME, Username), maps:remove(username, Client));
replvar(Cmd, _) ->
    Cmd.

repl(S, _VarPH, undefined) ->
    S;
repl(S, VarPH, Val) ->
    NVal   = re:replace(Val, "&", "\\\\&", [global, {return, list}]),
    NVarPH = emqx_authz:ph_to_re(VarPH),
    re:replace(S, NVarPH, NVal, [{return, list}]).
