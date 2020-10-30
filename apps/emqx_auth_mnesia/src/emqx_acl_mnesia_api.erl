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

-module(emqx_acl_mnesia_api).

-include("emqx_auth_mnesia.hrl").

-import(proplists, [get_value/2]).

-import(minirest,  [return/1]).

-rest_api(#{name   => list_emqx_acl,
            method => 'GET',
            path   => "/mqtt_acl",
            func   => list,
            descr  => "List available mnesia in the cluster"
           }).

-rest_api(#{name   => lookup_emqx_acl,
            method => 'GET',
            path   => "/mqtt_acl/:bin:login",
            func   => lookup,
            descr  => "Lookup mnesia in the cluster"
           }).

-rest_api(#{name   => add_emqx_acl,
            method => 'POST',
            path   => "/mqtt_acl",
            func   => add,
            descr  => "Add mnesia in the cluster"
           }).

-rest_api(#{name   => delete_emqx_acl,
            method => 'DELETE',
            path   => "/mqtt_acl/:bin:login/:bin:topic",
            func   => delete,
            descr  => "Delete mnesia in the cluster"
           }).

-export([ list/2
        , lookup/2
        , add/2
        , delete/2
        ]).

list(_Bindings, Params) ->
    return({ok, emqx_auth_mnesia_api:paginate(emqx_acl, Params, fun format/1)}).

lookup(#{login := Login}, _Params) ->
    return({ok, format(emqx_auth_mnesia_cli:lookup_acl(urldecode(Login)))}).

add(_Bindings, Params) ->
    [ P | _] = Params,
    case is_list(P) of
        true -> return(add_acl(Params, []));
        false -> return(add_acl([Params], []))
    end.

add_acl([ Params | ParamsN ], ReList ) ->
    Login = urldecode(get_value(<<"login">>, Params)),
    Topic = urldecode(get_value(<<"topic">>, Params)),
    Action = urldecode(get_value(<<"action">>, Params)),
    Allow = get_value(<<"allow">>, Params),
    Re = case validate([login, topic, action, allow], [Login, Topic, Action, Allow]) of
        ok -> 
            emqx_auth_mnesia_cli:add_acl(Login, Topic, Action, Allow);
        Err -> Err
    end,
    add_acl(ParamsN, [{Login, format_msg(Re)} | ReList]);   
    
add_acl([], ReList) ->
    {ok, ReList}.

delete(#{login := Login, topic := Topic}, _) ->
    return(emqx_auth_mnesia_cli:remove_acl(urldecode(Login), urldecode(Topic))).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

format(#emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow}) ->
    #{login => Login, topic => Topic, action => Action, allow => Allow };

format([]) ->
    #{};

format([#emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow}]) ->
    format(#emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow});

format([ #emqx_acl{login = _Key, topic = _Topic, action = _Action, allow = _Allow}| _] = List) ->
    format(List, []).
    
format([#emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow} | List], ReList) ->
    format(List, [ format(#emqx_acl{login = Login, topic = Topic, action = Action, allow = Allow}) | ReList]);
format([], ReList) -> ReList.

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
do_validation(topic, V) when is_binary(V)
                     andalso byte_size(V) > 0 ->
    true;
do_validation(action, V) when is_binary(V) ->
    case V =:= <<"pub">> orelse V =:= <<"sub">> orelse V =:= <<"pubsub">> of
        true -> true;
        false -> false
    end;
do_validation(allow, V) when is_boolean(V) ->
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

