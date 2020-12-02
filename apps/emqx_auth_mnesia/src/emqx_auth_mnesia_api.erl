%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(proplists, [get_value/2]).

-import(minirest,  [return/1]).

-rest_api(#{name   => list_emqx_user,
            method => 'GET',
            path   => "/mqtt_user",
            func   => list,
            descr  => "List available mnesia in the cluster"
           }).

-rest_api(#{name   => lookup_emqx_user,
            method => 'GET',
            path   => "/mqtt_user/:bin:login",
            func   => lookup,
            descr  => "Lookup mnesia in the cluster"
           }).

-rest_api(#{name   => add_emqx_user,
            method => 'POST',
            path   => "/mqtt_user",
            func   => add,
            descr  => "Add mnesia in the cluster"
           }).

-rest_api(#{name   => update_emqx_user,
            method => 'PUT',
            path   => "/mqtt_user/:bin:login",
            func   => update,
            descr  => "Update mnesia in the cluster"
           }).

-rest_api(#{name   => delete_emqx_user,
            method => 'DELETE',
            path   => "/mqtt_user/:bin:login",
            func   => delete,
            descr  => "Delete mnesia in the cluster"
           }).

-export([ list/2
        , lookup/2
        , add/2
        , update/2
        , delete/2
        ]).

-export([paginate/3]).

list(_Bindings, Params) ->
    return({ok, paginate(emqx_user, Params, fun format/1)}).

lookup(#{login := Login}, _Params) ->
    return({ok, format(emqx_auth_mnesia_cli:lookup_user(urldecode(Login)))}).

add(_Bindings, Params) ->
    [ P | _] = Params,
    case is_list(P) of
        true -> return(add_user(Params, []));
        false -> return(add_user([Params], []))
    end.

add_user([ Params | ParamsN ], ReList ) ->
    Login = urldecode(get_value(<<"login">>, Params)),
    Password = urldecode(get_value(<<"password">>, Params)),
    IsSuperuser = get_value(<<"is_superuser">>, Params),
    Re = case validate([login, password, is_superuser], [Login, Password, IsSuperuser]) of
        ok -> 
            emqx_auth_mnesia_cli:add_user(Login, Password, IsSuperuser);
        Err -> Err
    end,
    add_user(ParamsN, [{Login, format_msg(Re)} | ReList]);   
    
add_user([], ReList) ->
    {ok, ReList}.

update(#{login := Login}, Params) ->
    Password = get_value(<<"password">>, Params),
    IsSuperuser = get_value(<<"is_superuser">>, Params),
    case validate([password, is_superuser], [Password, IsSuperuser]) of
        ok -> return(emqx_auth_mnesia_cli:update_user(urldecode(Login), urldecode(Password), IsSuperuser));
        Err -> return(Err)
    end.

delete(#{login := Login}, _) ->
    return(emqx_auth_mnesia_cli:remove_user(urldecode(Login))).

%%------------------------------------------------------------------------------
%% Paging Query
%%------------------------------------------------------------------------------

paginate(Tables, Params, RowFun) ->
    Qh = query_handle(Tables),
    Count = count(Tables),
    Page = page(Params),
    Limit = limit(Params),
    Cursor = qlc:cursor(Qh),
    case Page > 1 of
        true  -> qlc:next_answers(Cursor, (Page - 1) * Limit);
        false -> ok
    end,
    Rows = qlc:next_answers(Cursor, Limit),
    qlc:delete_cursor(Cursor),
    #{meta  => #{page => Page, limit => Limit, count => Count},
      data  => [RowFun(Row) || Row <- Rows]}.

query_handle(Table) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table)]);
query_handle([Table]) when is_atom(Table) ->
    qlc:q([R|| R <- ets:table(Table)]);
query_handle(Tables) ->
    qlc:append([qlc:q([E || E <- ets:table(T)]) || T <- Tables]).

count(Table) when is_atom(Table) ->
    ets:info(Table, size);
count([Table]) when is_atom(Table) ->
    ets:info(Table, size);
count(Tables) ->
    lists:sum([count(T) || T <- Tables]).

page(Params) ->
    binary_to_integer(proplists:get_value(<<"_page">>, Params, <<"1">>)).

limit(Params) ->
    case proplists:get_value(<<"_limit">>, Params) of
        undefined -> 10;
        Size      -> binary_to_integer(Size)
    end.



%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

format({emqx_user, Login, Password, IsSuperuser}) ->
    #{login => Login,
      password => Password,
      is_superuser => IsSuperuser};

format([]) ->
    #{};

format([{emqx_user, Login, Password, IsSuperuser}]) ->
    #{login => Login,
      password => Password,
      is_superuser => IsSuperuser}.

validate([], []) ->
    ok;
validate([K|Keys], [V|Values]) ->
   case do_validation(K, V) of
       false -> {error, K};
       true  -> validate(Keys, Values)
   end.

do_validation(login, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(password, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(is_superuser, V) when is_boolean(V) ->
    true;
do_validation(_, _) ->
    false.

format_msg(Message)
  when is_atom(Message);
       is_binary(Message) -> Message;

format_msg(Message) when is_tuple(Message) ->
    iolist_to_binary(io_lib:format("~p", [Message])).

-if(?OTP_RELEASE >= 23).
urldecode(S) ->
    [{R, _}] = uri_string:dissect_query(S), R.
-else.
urldecode(S) ->
    http_uri:decode(S).
-endif.

