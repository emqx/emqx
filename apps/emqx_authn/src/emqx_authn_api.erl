%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_api).

-include("emqx_authn.hrl").

-export([ create_authenticator/2
        , delete_authenticator/2
        , update_authenticator/2
        , lookup_authenticator/2
        , list_authenticators/2
        , move_authenticator/2
        , import_users/2
        , add_user/2
        , delete_user/2
        , update_user/2
        , lookup_user/2
        , list_users/2
        ]).

-rest_api(#{name   => create_authenticator,
            method => 'POST',
            path   => "/authentication/authenticators",
            func   => create_authenticator,
            descr  => "Create authenticator"
           }).

-rest_api(#{name   => delete_authenticator,
            method => 'DELETE',
            path   => "/authentication/authenticators/:bin:name",
            func   => delete_authenticator,
            descr  => "Delete authenticator"
           }).

-rest_api(#{name   => update_authenticator,
            method => 'PUT',
            path   => "/authentication/authenticators/:bin:name",
            func   => update_authenticator,
            descr  => "Update authenticator"
           }).

-rest_api(#{name   => lookup_authenticator,
            method => 'GET',
            path   => "/authentication/authenticators/:bin:name",
            func   => lookup_authenticator,
            descr  => "Lookup authenticator"
           }).

-rest_api(#{name   => list_authenticators,
            method => 'GET',
            path   => "/authentication/authenticators",
            func   => list_authenticators,
            descr  => "List authenticators"
           }).

-rest_api(#{name   => move_authenticator,
            method => 'POST',
            path   => "/authentication/authenticators/:bin:name/position",
            func   => move_authenticator,
            descr  => "Change the order of authenticators"
           }).

-rest_api(#{name   => import_users,
            method => 'POST',
            path   => "/authentication/authenticators/:bin:name/import-users",
            func   => import_users,
            descr  => "Import users"
           }).

-rest_api(#{name   => add_user,
            method => 'POST',
            path   => "/authentication/authenticators/:bin:name/users",
            func   => add_user,
            descr  => "Add user"
           }).

-rest_api(#{name   => delete_user,
            method => 'DELETE',
            path   => "/authentication/authenticators/:bin:name/users/:bin:user_id",
            func   => delete_user,
            descr  => "Delete user"
           }).

-rest_api(#{name   => update_user,
            method => 'PUT',
            path   => "/authentication/authenticators/:bin:name/users/:bin:user_id",
            func   => update_user,
            descr  => "Update user"
           }).

-rest_api(#{name   => lookup_user,
            method => 'GET',
            path   => "/authentication/authenticators/:bin:name/users/:bin:user_id",
            func   => lookup_user,
            descr  => "Lookup user"
           }).

%% TODO: Support pagination
-rest_api(#{name   => list_users,
            method => 'GET',
            path   => "/authentication/authenticators/:bin:name/users",
            func   => list_users,
            descr  => "List all users"
           }).

create_authenticator(Binding, Params) ->
    do_create_authenticator(uri_decode(Binding), lists_to_map(Params)).

do_create_authenticator(_Binding, Authenticator0) ->
    Config = #{<<"emqx_authn">> => #{
               <<"authenticators">> => [Authenticator0]
              }},
    #{emqx_authn := #{authenticators := [Authenticator1]}}
        = hocon_schema:check_plain(emqx_authn_schema, Config,
                                   #{atom_key => true, nullable => true}),
    case emqx_authn:create_authenticator(?CHAIN, Authenticator1) of
        {ok, Authenticator2} ->
            return({ok, Authenticator2});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_authenticator(Binding, Params) ->
    do_delete_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_delete_authenticator(#{name := Name}, _Params) ->
    case emqx_authn:delete_authenticator(?CHAIN, Name) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

%% TODO: Support incremental update
update_authenticator(Binding, Params) ->
    do_update_authenticator(uri_decode(Binding), lists_to_map(Params)).

%% TOOD: PUT method supports creation and update
do_update_authenticator(#{name := Name}, NewConfig0) ->
    case emqx_authn:lookup_authenticator(?CHAIN, Name) of
        {ok, #{mechanism := Mechanism}} ->
            Authenticator = #{<<"name">> => Name,
                              <<"mechanism">> => Mechanism,
                              <<"config">> => NewConfig0},
            Config = #{<<"emqx_authn">> => #{
                          <<"authenticators">> => [Authenticator]
                      }},
            #{
                emqx_authn := #{
                    authenticators := [#{
                        config := NewConfig1
                    }]
                }
            } = hocon_schema:check_plain(emqx_authn_schema, Config,
                                         #{atom_key => true, nullable => true}),
            case emqx_authn:update_authenticator(?CHAIN, Name, NewConfig1) of
                {ok, NAuthenticator} ->
                    return({ok, NAuthenticator});
                {error, Reason} ->
                    return(serialize_error(Reason))
            end;
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_authenticator(Binding, Params) ->
    do_lookup_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_lookup_authenticator(#{name := Name}, _Params) ->
    case emqx_authn:lookup_authenticator(?CHAIN, Name) of
        {ok, Authenticator} ->
            return({ok, Authenticator});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_authenticators(Binding, Params) ->
    do_list_authenticators(uri_decode(Binding), maps:from_list(Params)).

do_list_authenticators(_Binding, _Params) ->
    case emqx_authn:list_authenticators(?CHAIN) of
        {ok, Authenticators} ->
            return({ok, Authenticators});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

move_authenticator(Binding, Params) ->
    do_move_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_move_authenticator(#{name := Name}, #{<<"position">> := <<"the front">>}) ->
    case emqx_authn:move_authenticator_to_the_front(?CHAIN, Name) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(#{name := Name}, #{<<"position">> := <<"the end">>}) ->
    case emqx_authn:move_authenticator_to_the_end(?CHAIN, Name) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(#{name := Name}, #{<<"position">> := N}) when is_number(N) ->
    case emqx_authn:move_authenticator_to_the_nth(?CHAIN, Name, N) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(_Binding, _Params) ->
    return(serialize_error({missing_parameter, <<"position">>})).

import_users(Binding, Params) ->
    do_import_users(uri_decode(Binding), maps:from_list(Params)).

do_import_users(#{name := Name},
                #{<<"filename">> := Filename}) ->
    case emqx_authn:import_users(?CHAIN, Name, Filename) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_import_users(_Binding, Params) ->
    Missed = get_missed_params(Params, [<<"filename">>, <<"file_format">>]),
    return(serialize_error({missing_parameter, Missed})).

add_user(Binding, Params) ->
    do_add_user(uri_decode(Binding), maps:from_list(Params)).

do_add_user(#{name := Name}, UserInfo) ->
    case emqx_authn:add_user(?CHAIN, Name, UserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_user(Binding, Params) ->
    do_delete_user(uri_decode(Binding), maps:from_list(Params)).

do_delete_user(#{name := Name,
                 user_id := UserID}, _Params) ->
    case emqx_authn:delete_user(?CHAIN, Name, UserID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

update_user(Binding, Params) ->
    do_update_user(uri_decode(Binding), maps:from_list(Params)).

do_update_user(#{name := Name,
                 user_id := UserID}, NewUserInfo) ->
    case emqx_authn:update_user(?CHAIN, Name, UserID, NewUserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_user(Binding, Params) ->
    do_lookup_user(uri_decode(Binding), maps:from_list(Params)).

do_lookup_user(#{name := Name,
                 user_id := UserID}, _Params) ->
    case emqx_authn:lookup_user(?CHAIN, Name, UserID) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_users(Binding, Params) ->
    do_list_users(uri_decode(Binding), maps:from_list(Params)).

do_list_users(#{name := Name}, _Params) ->
    case emqx_authn:list_users(?CHAIN, Name) of
        {ok, Users} ->
            return({ok, Users});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

uri_decode(Params) ->
    maps:fold(fun(K, V, Acc) ->
                  Acc#{K => emqx_http_lib:uri_decode(V)}
              end, #{}, Params).

lists_to_map(L) ->
    lists_to_map(L, #{}).

lists_to_map([], Acc) ->
    Acc;
lists_to_map([{K, V} | More], Acc) when is_list(V) ->
    NV = lists_to_map(V),
    lists_to_map(More, Acc#{K => NV});
lists_to_map([{K, V} | More], Acc) ->
    lists_to_map(More, Acc#{K => V});
lists_to_map([_ | _] = L, _) ->
    L.

serialize_error({already_exists, {Type, ID}}) ->
    {error, <<"ALREADY_EXISTS">>, list_to_binary(io_lib:format("~s '~s' already exists", [serialize_type(Type), ID]))};
serialize_error({not_found, {Type, ID}}) ->
    {error, <<"NOT_FOUND">>, list_to_binary(io_lib:format("~s '~s' not found", [serialize_type(Type), ID]))};
serialize_error({duplicate, Name}) ->
    {error, <<"INVALID_PARAMETER">>, list_to_binary(io_lib:format("Authenticator name '~s' is duplicated", [Name]))};
serialize_error({missing_parameter, Names = [_ | Rest]}) ->
    Format = ["~s," || _ <- Rest] ++ ["~s"],
    NFormat = binary_to_list(iolist_to_binary(Format)),
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameters " ++ NFormat ++ " that are mandatory for processing this request are not supplied.", Names))};
serialize_error({missing_parameter, Name}) ->
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameter '~s' that is mandatory for processing this request is not supplied.", [Name]))};
serialize_error(_) ->
    {error, <<"UNKNOWN_ERROR">>, <<"Unknown error">>}.

serialize_type(authenticator) ->
    "Authenticator".

get_missed_params(Actual, Expected) ->
    Keys = lists:foldl(fun(Key, Acc) ->
                           case maps:is_key(Key, Actual) of
                               true -> Acc;
                               false -> [Key | Acc]
                           end
                       end, [], Expected),
    lists:reverse(Keys).

return(_) ->
%%    TODO: V5 API
    ok.
