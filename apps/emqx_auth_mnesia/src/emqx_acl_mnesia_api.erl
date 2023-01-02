%c%--------------------------------------------------------------------
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

-module(emqx_acl_mnesia_api).

-include_lib("stdlib/include/ms_transform.hrl").

-import(proplists, [ get_value/2
                   , get_value/3
                   ]).

-import(minirest,  [return/1]).

-rest_api(#{name   => list_clientid,
            method => 'GET',
            path   => "/acl/clientid",
            func   => list_clientid,
            descr  => "List available mnesia in the cluster"
           }).

-rest_api(#{name   => list_username,
            method => 'GET',
            path   => "/acl/username",
            func   => list_username,
            descr  => "List available mnesia in the cluster"
           }).

-rest_api(#{name   => list_all,
            method => 'GET',
            path   => "/acl/$all",
            func   => list_all,
            descr  => "List available mnesia in the cluster"
           }).

-rest_api(#{name   => lookup_clientid,
            method => 'GET',
            path   => "/acl/clientid/:bin:clientid",
            func   => lookup,
            descr  => "Lookup mnesia in the cluster"
           }).

-rest_api(#{name   => lookup_username,
            method => 'GET',
            path   => "/acl/username/:bin:username",
            func   => lookup,
            descr  => "Lookup mnesia in the cluster"
           }).

-rest_api(#{name   => add,
            method => 'POST',
            path   => "/acl",
            func   => add,
            descr  => "Add mnesia in the cluster"
           }).

-rest_api(#{name   => delete_clientid,
            method => 'DELETE',
            path   => "/acl/clientid/:bin:clientid/topic/:bin:topic",
            func   => delete,
            descr  => "Delete mnesia in the cluster"
           }).

-rest_api(#{name   => delete_username,
            method => 'DELETE',
            path   => "/acl/username/:bin:username/topic/:bin:topic",
            func   => delete,
            descr  => "Delete mnesia in the cluster"
           }).

-rest_api(#{name   => delete_all,
            method => 'DELETE',
            path   => "/acl/$all/topic/:bin:topic",
            func   => delete,
            descr  => "Delete mnesia in the cluster"
           }).


-export([ list_clientid/2
        , list_username/2
        , list_all/2
        , lookup/2
        , add/2
        , delete/2
        ]).

-define(CLIENTID_SCHEMA, [{<<"clientid">>, binary}, {<<"_like_clientid">>, binary}] ++ ?COMMON_SCHEMA).
-define(USERNAME_SCHEMA, [{<<"username">>, binary}, {<<"_like_username">>, binary}] ++ ?COMMON_SCHEMA).
-define(COMMON_SCHEMA, [{<<"topic">>, binary}, {<<"action">>, atom}, {<<"access">>, atom}]).

list_clientid(_Bindings, Params) ->
    {_, Params1 = {_Qs, _Fuzzy}} = emqx_mgmt_api:params2qs(Params, ?CLIENTID_SCHEMA),
    Table = emqx_acl_mnesia_db:login_acl_table(clientid, Params1),
    return({ok, paginate_qh(Table, count(Table), Params, fun emqx_acl_mnesia_db:comparing/2, fun format/1)}).

list_username(_Bindings, Params) ->
    {_, Params1 = {_Qs, _Fuzzy}} = emqx_mgmt_api:params2qs(Params, ?USERNAME_SCHEMA),
    Table = emqx_acl_mnesia_db:login_acl_table(username, Params1),
    return({ok, paginate_qh(Table, count(Table), Params, fun emqx_acl_mnesia_db:comparing/2, fun format/1)}).

list_all(_Bindings, Params) ->
    {_, Params1 = {_Qs, _Fuzzy}} = emqx_mgmt_api:params2qs(Params, ?COMMON_SCHEMA),
    Table = emqx_acl_mnesia_db:login_acl_table(all, Params1),
    return({ok, paginate_qh(Table, count(Table), Params, fun emqx_acl_mnesia_db:comparing/2, fun format/1)}).

lookup(#{clientid := Clientid}, _Params) ->
    return({ok, format(emqx_acl_mnesia_db:lookup_acl({clientid, urldecode(Clientid)}))});
lookup(#{username := Username}, _Params) ->
    return({ok, format(emqx_acl_mnesia_db:lookup_acl({username, urldecode(Username)}))}).

add(_Bindings, Params) ->
    [ P | _] = Params,
    case is_list(P) of
        true -> return(do_add(Params, []));
        false ->
            Re = do_add(Params),
            case Re of
                #{result := ok} -> return({ok, Re});
                #{result := <<"ok">>} -> return({ok, Re});
                _ -> return({error, {add, Re}})
            end
    end.

do_add([ Params | ParamsN ], ReList) ->
    do_add(ParamsN, [do_add(Params) | ReList]);

do_add([], ReList) ->
    {ok, ReList}.

do_add(Params) ->
    Clientid = get_value(<<"clientid">>, Params, undefined),
    Username = get_value(<<"username">>, Params, undefined),
    Login = case {Clientid, Username} of
                {undefined, undefined} -> all;
                {_, undefined} -> {clientid, Clientid};
                {undefined, _} -> {username, Username}
            end,
    Topic = get_value(<<"topic">>, Params),
    Action = get_value(<<"action">>, Params),
    Access = get_value(<<"access">>, Params),
    Re = case validate([login, topic, action, access], [Login, Topic, Action, Access]) of
        ok ->
            emqx_acl_mnesia_db:add_acl(Login, Topic, erlang:binary_to_atom(Action, utf8), erlang:binary_to_atom(Access, utf8));
        Err -> Err
    end,
    maps:merge(#{topic => Topic,
                 action => Action,
                 access => Access,
                 result => format_msg(Re)
                }, case Login of
                     all -> #{all => '$all'};
                     _ -> maps:from_list([Login])
                   end).

delete(#{clientid := Clientid, topic := Topic}, _) ->
    return(emqx_acl_mnesia_db:remove_acl({clientid, urldecode(Clientid)}, urldecode(Topic)));
delete(#{username := Username, topic := Topic}, _) ->
    return(emqx_acl_mnesia_db:remove_acl({username, urldecode(Username)}, urldecode(Topic)));
delete(#{topic := Topic}, _) ->
    return(emqx_acl_mnesia_db:remove_acl(all, urldecode(Topic))).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

count(QH) ->
    Count = qlc:fold(fun(_, Sum) -> Sum + 1 end, 0, QH),
    case is_integer(Count) of
        true -> Count;
        false -> 0
    end.

format({{clientid, Clientid}, Topic, Action, Access, _CreatedAt}) ->
    #{clientid => Clientid, topic => Topic, action => Action, access => Access};
format({{username, Username}, Topic, Action, Access, _CreatedAt}) ->
    #{username => Username, topic => Topic, action => Action, access => Access};
format({all, Topic, Action, Access, _CreatedAt}) ->
    #{all => '$all', topic => Topic, action => Action, access => Access};
format(List) when is_list(List) ->
    format(List, []).

format([L | List], Relist) ->
    format(List, [format(L) | Relist]);
format([], ReList) -> lists:reverse(ReList).

validate([], []) ->
    ok;
validate([K|Keys], [V|Values]) ->
   case do_validation(K, V) of
       false -> {error, K};
       true  -> validate(Keys, Values)
   end.
do_validation(login, all) ->
    true;
do_validation(login, {clientid, V}) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(login, {username, V}) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(topic, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(action, V) when is_binary(V) ->
    case V =:= <<"pub">> orelse V =:= <<"sub">> orelse V =:= <<"pubsub">> of
        true -> true;
        false -> false
    end;
do_validation(access, V) when V =:= <<"allow">> orelse V =:= <<"deny">> ->
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

paginate_qh(Qh, Count, Params, ComparingFun, RowFun) ->
    Page = page(Params),
    Limit = limit(Params),
    Cursor = qlc:cursor(Qh),
    case Page > 1 of
        true  ->
            _ = qlc:next_answers(Cursor, (Page - 1) * Limit),
            ok;
        false -> ok
    end,
    Rows = qlc:next_answers(Cursor, Limit),
    qlc:delete_cursor(Cursor),
    #{meta  => #{page => Page, limit => Limit, count => Count},
        data  => [RowFun(Row) || Row <- lists:sort(ComparingFun, Rows)]}.

page(Params) ->
    binary_to_integer(proplists:get_value(<<"_page">>, Params, <<"1">>)).

limit(Params) ->
    case proplists:get_value(<<"_limit">>, Params) of
        undefined -> 50;
        Size      -> binary_to_integer(Size)
    end.
