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

-module(emqx_auth_mnesia_api).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("emqx_auth_mnesia.hrl").

-define(TABLE, emqx_user).

-import(proplists, [get_value/2]).
-import(minirest,  [return/1]).

-export([ list_clientid/2
        , lookup_clientid/2
        , add_clientid/2
        , update_clientid/2
        , delete_clientid/2
        , query_clientid/3
        , query_username/3
        ]).

-rest_api(#{name   => list_clientid,
            method => 'GET',
            path   => "/auth_clientid",
            func   => list_clientid,
            descr  => "List available clientid in the cluster"
           }).

-rest_api(#{name   => lookup_clientid,
            method => 'GET',
            path   => "/auth_clientid/:bin:clientid",
            func   => lookup_clientid,
            descr  => "Lookup clientid in the cluster"
           }).

-rest_api(#{name   => add_clientid,
            method => 'POST',
            path   => "/auth_clientid",
            func   => add_clientid,
            descr  => "Add clientid in the cluster"
           }).

-rest_api(#{name   => update_clientid,
            method => 'PUT',
            path   => "/auth_clientid/:bin:clientid",
            func   => update_clientid,
            descr  => "Update clientid in the cluster"
           }).

-rest_api(#{name   => delete_clientid,
            method => 'DELETE',
            path   => "/auth_clientid/:bin:clientid",
            func   => delete_clientid,
            descr  => "Delete clientid in the cluster"
           }).

-export([ list_username/2
        , lookup_username/2
        , add_username/2
        , update_username/2
        , delete_username/2
        ]).

-rest_api(#{name   => list_username,
            method => 'GET',
            path   => "/auth_username",
            func   => list_username,
            descr  => "List available username in the cluster"
           }).

-rest_api(#{name   => lookup_username,
            method => 'GET',
            path   => "/auth_username/:bin:username",
            func   => lookup_username,
            descr  => "Lookup username in the cluster"
           }).

-rest_api(#{name   => add_username,
            method => 'POST',
            path   => "/auth_username",
            func   => add_username,
            descr  => "Add username in the cluster"
           }).

-rest_api(#{name   => update_username,
            method => 'PUT',
            path   => "/auth_username/:bin:username",
            func   => update_username,
            descr  => "Update username in the cluster"
           }).

-rest_api(#{name   => delete_username,
            method => 'DELETE',
            path   => "/auth_username/:bin:username",
            func   => delete_username,
            descr  => "Delete username in the cluster"
           }).

-define(CLIENTID_SCHEMA, {?TABLE,
    [
        {<<"clientid">>, binary},
        {<<"_like_clientid">>, binary}
    ]}).

-define(USERNAME_SCHEMA, {?TABLE,
    [
        {<<"username">>, binary},
        {<<"_like_username">>, binary}
    ]}).

-define(query_clientid, {?MODULE, query_clientid}).
-define(query_username, {?MODULE, query_username}).

%%------------------------------------------------------------------------------
%% Auth Clientid Api
%%------------------------------------------------------------------------------

list_clientid(_Bindings, Params) ->
    SortFun = fun(#{created_at := C1}, #{created_at := C2}) -> C1 > C2 end,
    CountFun = fun() ->
        MatchSpec = [{{?TABLE, {clientid, '_'}, '_', '_'}, [], [true]}],
        ets:select_count(?TABLE, MatchSpec)
               end,
    return({ok, emqx_mgmt_api:node_query(node(), Params, ?CLIENTID_SCHEMA, ?query_clientid, SortFun, CountFun)}).

lookup_clientid(#{clientid := Clientid}, _Params) ->
    return({ok, format(emqx_auth_mnesia_cli:lookup_user({clientid, urldecode(Clientid)}))}).

add_clientid(_Bindings, Params) ->
    [ P | _] = Params,
    case is_list(P) of
        true -> return(do_add_clientid(Params, []));
        false ->
            Re = do_add_clientid(Params),
            case Re of
                ok -> return(ok);
                {error, Error} -> return({error, format_msg(Error)})
            end
    end.

do_add_clientid([ Params | ParamsN ], ReList ) ->
    Clientid = get_value(<<"clientid">>, Params),
    do_add_clientid(ParamsN, [{Clientid, format_msg(do_add_clientid(Params))} | ReList]);

do_add_clientid([], ReList) ->
    {ok, ReList}.

do_add_clientid(Params) ->
    Clientid = get_value(<<"clientid">>, Params),
    Password = get_value(<<"password">>, Params),
    Login = {clientid, Clientid},
    case validate([login, password], [Login, Password]) of
        ok ->
            emqx_auth_mnesia_cli:add_user(Login, Password);
        Err -> Err
    end.

update_clientid(#{clientid := Clientid}, Params) ->
    Password = get_value(<<"password">>, Params),
    case validate([password], [Password]) of
        ok -> return(emqx_auth_mnesia_cli:update_user({clientid, urldecode(Clientid)}, Password));
        Err -> return(Err)
    end.

delete_clientid(#{clientid := Clientid}, _) ->
    return(emqx_auth_mnesia_cli:remove_user({clientid, urldecode(Clientid)})).

%%------------------------------------------------------------------------------
%% Auth Username Api
%%------------------------------------------------------------------------------

list_username(_Bindings, Params) ->
    SortFun = fun(#{created_at := C1}, #{created_at := C2}) -> C1 > C2 end,
    CountFun = fun() ->
        MatchSpec = [{{?TABLE, {username, '_'}, '_', '_'}, [], [true]}],
        ets:select_count(?TABLE, MatchSpec)
               end,
    return({ok, emqx_mgmt_api:node_query(node(), Params, ?USERNAME_SCHEMA, ?query_username, SortFun, CountFun)}).

lookup_username(#{username := Username}, _Params) ->
    return({ok, format(emqx_auth_mnesia_cli:lookup_user({username, urldecode(Username)}))}).

add_username(_Bindings, Params) ->
    [ P | _] = Params,
    case is_list(P) of
        true -> return(do_add_username(Params, []));
        false ->
            case do_add_username(Params) of
                ok -> return(ok);
                {error, Error} -> return({error, format_msg(Error)})
            end
    end.

do_add_username([ Params | ParamsN ], ReList ) ->
    Username = get_value(<<"username">>, Params),
    do_add_username(ParamsN, [{Username, format_msg(do_add_username(Params))} | ReList]);

do_add_username([], ReList) ->
    {ok, ReList}.

do_add_username(Params) ->
    Username = get_value(<<"username">>, Params),
    Password = get_value(<<"password">>, Params),
    Login = {username, Username},
    case validate([login, password], [Login, Password]) of
        ok ->
            emqx_auth_mnesia_cli:add_user(Login, Password);
        Err -> Err
    end.

update_username(#{username := Username}, Params) ->
    Password = get_value(<<"password">>, Params),
    case validate([password], [Password]) of
        ok -> return(emqx_auth_mnesia_cli:update_user({username, urldecode(Username)}, Password));
        Err -> return(Err)
    end.

delete_username(#{username := Username}, _) ->
    return(emqx_auth_mnesia_cli:remove_user({username, urldecode(Username)})).

%%------------------------------------------------------------------------------
%% Paging Query
%%------------------------------------------------------------------------------
query_clientid(Qs, Start, Limit) -> query(clientid, Qs, Start, Limit).
query_username(Qs, Start, Limit) -> query(username, Qs, Start, Limit).

query(Type, {Qs, []}, Start, Limit) ->
    Ms = qs2ms(Type, Qs),
    emqx_mgmt_api:select_table(?TABLE, Ms, Start, Limit, fun format/1);

query(Type, {Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Type, Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(?TABLE, MatchFun, Start, Limit, fun format/1).

-spec qs2ms(clientid | username, list()) -> ets:match_spec().
qs2ms(Type, Qs) ->
    Init = #?TABLE{login = {Type, '_'}, password  = '_', created_at = '_'},
    MatchHead = lists:foldl(fun(Q, Acc) ->  match_ms(Q, Acc) end, Init, Qs),
    [{MatchHead, [], ['$_']}].

match_ms({Type, '=:=', Value}, MatchHead) -> MatchHead#?TABLE{login = {Type, Value}};
match_ms(_, MatchHead) -> MatchHead.

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    fun(Rows) ->
        Ls = ets:match_spec_run(Rows, MsC),
        lists:filter(fun(E) -> run_fuzzy_match(E, Fuzzy) end, Ls)
    end.

run_fuzzy_match(_, []) -> true;
run_fuzzy_match(E = #?TABLE{login = {Key, Str}}, [{Key, like, SubStr}|Fuzzy]) ->
    binary:match(Str, SubStr) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(_E, [{_Key, like, _SubStr}| _Fuzzy]) -> false.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

format([{?TABLE, {clientid, ClientId}, _Password, CreatedAt}]) ->
    #{clientid => ClientId, created_at => CreatedAt};

format([{?TABLE, {username, Username}, _Password, CreatedAt}]) ->
    #{username => Username, created_at => CreatedAt};

format([]) ->
    #{};
format(User) -> format([User]).

validate([], []) ->
    ok;
validate([K|Keys], [V|Values]) ->
   case do_validation(K, V) of
       false -> {error, K};
       true  -> validate(Keys, Values)
   end.

do_validation(login, {clientid, V}) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(login, {username, V}) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(password, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(_, _) ->
    false.

format_msg(Message)
  when is_atom(Message);
       is_binary(Message) -> Message;

format_msg(Message) when is_tuple(Message) ->
    iolist_to_binary(io_lib:format("~p", [Message])).

urldecode(S) ->
    emqx_http_lib:uri_decode(S).
