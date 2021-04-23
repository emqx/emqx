%c%--------------------------------------------------------------------
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

-module(emqx_acl_mnesia_api).

-include("emqx_auth_mnesia.hrl").

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

list_clientid(_Bindings, Params) ->
    MatchSpec = ets:fun2ms(
                  fun({emqx_acl, {{clientid, Clientid}, Topic}, Action, Access, CreatedAt}) -> {{clientid,Clientid}, Topic, Action,Access, CreatedAt} end),
    return({ok, emqx_auth_mnesia_api:paginate(emqx_acl, MatchSpec, Params, fun emqx_acl_mnesia_cli:comparing/2, fun format/1)}).

list_username(_Bindings, Params) ->
    MatchSpec = ets:fun2ms(
                  fun({emqx_acl, {{username, Username}, Topic}, Action, Access, CreatedAt}) -> {{username, Username}, Topic, Action,Access, CreatedAt} end),
    return({ok, emqx_auth_mnesia_api:paginate(emqx_acl, MatchSpec, Params, fun emqx_acl_mnesia_cli:comparing/2, fun format/1)}).

list_all(_Bindings, Params) ->
    MatchSpec = ets:fun2ms(
                  fun({emqx_acl, {all, Topic}, Action, Access, CreatedAt}) -> {all, Topic, Action,Access, CreatedAt}end
                 ),
    return({ok, emqx_auth_mnesia_api:paginate(emqx_acl, MatchSpec, Params, fun emqx_acl_mnesia_cli:comparing/2, fun format/1)}).


lookup(#{clientid := Clientid}, _Params) ->
    return({ok, format(emqx_acl_mnesia_cli:lookup_acl({clientid, urldecode(Clientid)}))});
lookup(#{username := Username}, _Params) ->
    return({ok, format(emqx_acl_mnesia_cli:lookup_acl({username, urldecode(Username)}))}).

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
                {_, undefined} -> {clientid, urldecode(Clientid)};
                {undefined, _} -> {username, urldecode(Username)}
            end,
    Topic = urldecode(get_value(<<"topic">>, Params)),
    Action = urldecode(get_value(<<"action">>, Params)),
    Access = urldecode(get_value(<<"access">>, Params)),
    Re = case validate([login, topic, action, access], [Login, Topic, Action, Access]) of
        ok ->
            emqx_acl_mnesia_cli:add_acl(Login, Topic, erlang:binary_to_atom(Action, utf8), erlang:binary_to_atom(Access, utf8));
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
    return(emqx_acl_mnesia_cli:remove_acl({clientid, urldecode(Clientid)}, urldecode(Topic)));
delete(#{username := Username, topic := Topic}, _) ->
    return(emqx_acl_mnesia_cli:remove_acl({username, urldecode(Username)}, urldecode(Topic)));
delete(#{topic := Topic}, _) ->
    return(emqx_acl_mnesia_cli:remove_acl(all, urldecode(Topic))).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------
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
