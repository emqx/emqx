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

-module(emqx_auth_pgsql_cli).

-behaviour(ecpool_worker).

-include("emqx_auth_pgsql.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-export([connect/1]).
-export([parse_query/2, pgvar/2]).
-export([ equery/4
        , equery/3
        ]).

-export([ get_sql_conf/1
        , put_sql_conf/2
        , clear_sql_conf/1
        , get_cached_statement/1
        , put_cached_statement/2
        , clear_cached_statement/1
        ]).

-type client_info() :: #{username := _,
                         clientid := _,
                         peerhost := _,
                         _ => _}.

%%--------------------------------------------------------------------
%% Avoid SQL Injection: Parse SQL to Parameter Query.
%%--------------------------------------------------------------------

parse_query(_Par, undefined) ->
    undefined;
parse_query(Par, Sql) ->
    case re:run(Sql, "'%[ucCad]'", [global, {capture, all, list}]) of
        {match, Variables} ->
            Params = [Var || [Var] <- Variables],
            {str(Par), Params};
        nomatch ->
            {str(Par), []}
    end.

pgvar(Sql, Params) ->
    Vars = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(Params))],
    lists:foldl(fun({Param, Var}, S) ->
            re:replace(S, Param, Var, [{return, list}])
        end, Sql, lists:zip(Params, Vars)).

%%--------------------------------------------------------------------
%% PostgreSQL Connect/Query
%%--------------------------------------------------------------------

%% Due to a bug in epgsql the caluse for `econnrefused` is not recognised by
%% dialyzer, result in this error:
%% The pattern {'error', Reason = 'econnrefused'} can never match the type ...
%% https://github.com/epgsql/epgsql/issues/246
-dialyzer([{nowarn_function, [connect/1]}]).
connect(Opts) ->
    Host     = proplists:get_value(host, Opts),
    Username = proplists:get_value(username, Opts),
    Password = proplists:get_value(password, Opts),
    case epgsql:connect(Host, Username, Password, conn_opts(Opts)) of
        {ok, C} ->
            conn_post(C),
            {ok, C};
        {error, Reason = econnrefused} ->
            ?LOG(error, "[Postgres] Can't connect to Postgres server: Connection refused."),
            {error, Reason};
        {error, Reason = invalid_authorization_specification} ->
            ?LOG(error, "[Postgres] Can't connect to Postgres server: Invalid authorization specification."),
            {error, Reason};
        {error, Reason = invalid_password} ->
            ?LOG(error, "[Postgres] Can't connect to Postgres server: Invalid password."),
            {error, Reason};
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "[Postgres] Can't connect to Postgres server: ~p", [Reason]),
            {error, Reason}
    end.

conn_post(Conn) ->
    lists:foreach(fun(PreparedStKey) ->
            case prepare_statement(Conn, PreparedStKey) of
                {ok, St} -> put_cached_statement(PreparedStKey, St);
                Error -> Error
            end
        end, [auth_query, acl_query, super_query]).

prepare_statement(Conn, PreparedStKey) ->
    %% for emqx_auth_pgsql plugin, the PreparedStKey is an atom(),
    %% but for emqx_module_auth_pgsql, it is a list().
    Sql0 = get_sql_conf(PreparedStKey),
    case parse_query(PreparedStKey, Sql0) of
        undefined -> ok;
        {_, Params} ->
            Sql = pgvar(Sql0, Params),
            epgsql:parse(Conn, str(PreparedStKey), Sql, [])
    end.

conn_opts(Opts) ->
    conn_opts(Opts, []).
conn_opts([], Acc) ->
    Acc;
conn_opts([Opt = {database, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {ssl, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {port, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {timeout, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([Opt = {ssl_opts, _}|Opts], Acc) ->
    conn_opts(Opts, [Opt|Acc]);
conn_opts([_Opt|Opts], Acc) ->
    conn_opts(Opts, Acc).

-spec(equery(atom(), string() | epgsql:statement(), Parameters::[any()]) -> {ok, ColumnsDescription :: [any()], RowsValues :: [any()]} | {error, any()} ).
equery(Pool, PreparedStKey, Params) ->
    do_equery(Pool, PreparedStKey, Params).

-spec(equery(atom(), string() | epgsql:statement(), Parameters::[any()], client_info()) ->  {ok, ColumnsDescription :: [any()], RowsValues :: [any()]} | {error, any()} ).
equery(Pool, PreparedStKey, Params, ClientInfo) ->
    Params1 = replvar(Params, ClientInfo),
    equery(Pool, PreparedStKey, Params1).

get_sql_conf(PreparedStKey) ->
    application:get_env(?APP, prpst_key_to_atom(PreparedStKey), undefined).

put_sql_conf(PreparedStKey, Sql) ->
    application:set_env(?APP, prpst_key_to_atom(PreparedStKey), Sql).

clear_sql_conf(PreparedStKey) ->
    application:unset_env(?APP, prpst_key_to_atom(PreparedStKey)).

get_cached_statement(PreparedStKey) ->
    application:get_env(?APP, statement_key(PreparedStKey), PreparedStKey).

put_cached_statement(PreparedStKey, St) ->
    application:set_env(?APP, statement_key(PreparedStKey), St).

clear_cached_statement(PreparedStKey) ->
    application:unset_env(?APP, statement_key(PreparedStKey)).

do_equery(Pool, PreparedStKey, Params) ->
    ecpool:with_client(Pool, fun(Conn) ->
        StOrKey = get_cached_statement(PreparedStKey),
        case epgsql:prepared_query(Conn, StOrKey, Params) of
            {error, #error{severity = error, code = <<"26000">>}} ->
                %% invalid_sql_statement_name, we need to prepare the statement and try again
                case prepare_statement(Conn, PreparedStKey) of
                    {ok, St} ->
                        ok = put_cached_statement(PreparedStKey, St),
                        epgsql:prepared_query(Conn, St, Params);
                    {error, _} = Error ->
                        Error
                end;
            Return ->
                Return
        end

    end).

replvar(Params, ClientInfo) ->
    replvar(Params, ClientInfo, []).

replvar([], _ClientInfo, Acc) ->
    lists:reverse(Acc);

replvar(["'%u'" | Params], ClientInfo = #{username := Username}, Acc) ->
    replvar(Params, ClientInfo, [Username | Acc]);
replvar(["'%c'" | Params], ClientInfo = #{clientid := ClientId}, Acc) ->
    replvar(Params, ClientInfo, [ClientId | Acc]);
replvar(["'%a'" | Params], ClientInfo = #{peerhost := IpAddr}, Acc) ->
    replvar(Params, ClientInfo, [inet_parse:ntoa(IpAddr) | Acc]);
replvar(["'%C'" | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(cn, ClientInfo)| Acc]);
replvar(["'%d'" | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [safe_get(dn, ClientInfo)| Acc]);
replvar([Param | Params], ClientInfo, Acc) ->
    replvar(Params, ClientInfo, [Param | Acc]).

safe_get(K, ClientInfo) ->
    bin(maps:get(K, ClientInfo, undefined)).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(X) -> X.

str(B) when is_binary(B) -> binary_to_list(B);
str(A) when is_atom(A) -> atom_to_list(A);
str(S) when is_list(S) -> S.

prpst_key_to_atom(super_query) -> super_query;
prpst_key_to_atom("super_query") -> super_query;
prpst_key_to_atom(auth_query) -> auth_query;
prpst_key_to_atom("auth_query") -> auth_query;
prpst_key_to_atom(acl_query) -> acl_query;
prpst_key_to_atom("acl_query") -> acl_query;
prpst_key_to_atom(PreparedStKey) ->
    throw({unsupported_prepared_statement_key, PreparedStKey}).

%% the spec of application:get_env(App, Key, Def) requires an atom() Key
statement_key(super_query) -> statement_super_query;
statement_key("super_query") -> statement_super_query;
statement_key(auth_query) -> statement_auth_query;
statement_key("auth_query") -> statement_auth_query;
statement_key(acl_query) -> statement_acl_query;
statement_key("acl_query") -> statement_acl_query;
statement_key(PreparedStKey) ->
    throw({unsupported_prepared_statement_key, PreparedStKey}).
