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

-module(emqx_auth_redis_cli).

-behaviour(ecpool_worker).

-include("emqx_auth_redis.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([ connect/1
        , q/5
        ]).

%%--------------------------------------------------------------------
%% Redis Connect/Query
%%--------------------------------------------------------------------

connect(Opts) ->
    Sentinel = get_value(sentinel, Opts),
    Host = case Sentinel =:= "" of
        true -> get_value(host, Opts);
        false ->
            _ = eredis_sentinel:start_link(get_value(servers, Opts), get_value(options, Opts, [])),
            "sentinel:" ++ Sentinel
    end,
    case eredis:start_link(Host,
                           get_value(port, Opts, 6379),
                           get_value(database, Opts, 0),
                           get_value(password, Opts, ""),
                           3000,
                           5000,
                           get_value(options, Opts, [])) of
            {ok, Pid} -> {ok, Pid};
            {error, Reason = {connection_error, _}} ->
                ?LOG(error, "[Redis] Can't connect to Redis server: Connection refused."),
                {error, Reason};
            {error, Reason = {authentication_error, _}} ->
                ?LOG(error, "[Redis] Can't connect to Redis server: Authentication failed."),
                {error, Reason};
            {error, Reason} ->
                ?LOG_SENSITIVE(error, "[Redis] Can't connect to Redis server: ~p", [Reason]),
                {error, Reason}
    end.

%% Redis Query.
-spec(q(atom(), atom(), string(), emqx_types:credentials(), timeout())
      -> {ok, undefined | binary() | list()} | {error, atom() | binary()}).
q(Pool, Type, CmdStr, Credentials, Timeout) ->
    Cmd = string:tokens(replvar(CmdStr, Credentials), " "),
    case Type of
        cluster -> eredis_cluster:q(Pool, Cmd);
        _ -> ecpool:with_client(Pool, fun(C) -> eredis:q(C, Cmd, Timeout) end)
    end.

replvar(Cmd, Credentials = #{cn := CN}) ->
    replvar(repl(Cmd, "%C", CN), maps:remove(cn, Credentials));
replvar(Cmd, Credentials = #{dn := DN}) ->
    replvar(repl(Cmd, "%d", DN), maps:remove(dn, Credentials));
replvar(Cmd, Credentials = #{clientid := ClientId}) ->
    replvar(repl(Cmd, "%c", ClientId), maps:remove(clientid, Credentials));
replvar(Cmd, Credentials = #{username := Username}) ->
    replvar(repl(Cmd, "%u", Username), maps:remove(username, Credentials));
replvar(Cmd, _) ->
    Cmd.

repl(S, _Var, undefined) ->
    S;
repl(S, Var, Val) ->
    NVal = re:replace(Val, "&", "\\\\&", [global, {return, list}]),
    re:replace(S, Var, NVal, [{return, list}]).
