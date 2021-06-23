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

-export([ create_chain/2
        , delete_chain/2
        , lookup_chain/2
        , list_chains/2
        , create_service/2
        , delete_service/2
        , update_service/2
        , lookup_service/2
        , list_services/2
        , move_service/2
        , import_users/2
        , add_user/2
        , delete_user/2
        , update_user/2
        , lookup_user/2
        , list_users/2
        ]).

-import(minirest,  [return/1]).

-rest_api(#{name   => create_chain,
            method => 'POST',
            path   => "/authentication/chains",
            func   => create_chain,
            descr  => "Create a chain"
           }).

-rest_api(#{name   => delete_chain,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id",
            func   => delete_chain,
            descr  => "Delete chain"
           }).

-rest_api(#{name   => lookup_chain,
            method => 'GET',
            path   => "/authentication/chains/:bin:id",
            func   => lookup_chain,
            descr  => "Lookup chain"
           }).

-rest_api(#{name   => list_chains,
            method => 'GET',
            path   => "/authentication/chains",
            func   => list_chains,
            descr  => "List all chains"
           }).

-rest_api(#{name   => create_service,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/services",
            func   => create_service,
            descr  => "Create service to chain"
           }).

-rest_api(#{name   => delete_service,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name",
            func   => delete_service,
            descr  => "Delete service from chain"
           }).

-rest_api(#{name   => update_service,
            method => 'PUT',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name",
            func   => update_service,
            descr  => "Update service in chain"
           }).

-rest_api(#{name   => lookup_service,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name",
            func   => lookup_service,
            descr  => "Lookup service in chain"
           }).

-rest_api(#{name   => list_services,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/services",
            func   => list_services,
            descr  => "List services in chain"
           }).

-rest_api(#{name   => move_service,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/position",
            func   => move_service,
            descr  => "Change the order of services"
           }).

-rest_api(#{name   => import_users,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/import-users",
            func   => import_users,
            descr  => "Import users"
           }).

-rest_api(#{name   => add_user,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/users",
            func   => add_user,
            descr  => "Add user"
           }).

-rest_api(#{name   => delete_user,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/users/:bin:user_id",
            func   => delete_user,
            descr  => "Delete user"
           }).

-rest_api(#{name   => update_user,
            method => 'PUT',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/users/:bin:user_id",
            func   => update_user,
            descr  => "Update user"
           }).

-rest_api(#{name   => lookup_user,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/users/:bin:user_id",
            func   => lookup_user,
            descr  => "Lookup user"
           }).

%% TODO: Support pagination
-rest_api(#{name   => list_users,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/services/:bin:service_name/users",
            func   => list_users,
            descr  => "List all users"
           }).

create_chain(Binding, Params) ->
    do_create_chain(uri_decode(Binding), maps:from_list(Params)).

do_create_chain(_Binding, #{<<"id">> := ChainID}) ->
    case emqx_authn:create_chain(#{id => ChainID}) of
        {ok, Chain} ->
            return({ok, Chain});
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_create_chain(_Binding, _Params) ->
    return(serialize_error({missing_parameter, id})).

delete_chain(Binding, Params) ->
    do_delete_chain(uri_decode(Binding), maps:from_list(Params)).

do_delete_chain(#{id := ChainID}, _Params) ->
    case emqx_authn:delete_chain(ChainID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_chain(Binding, Params) ->
    do_lookup_chain(uri_decode(Binding), maps:from_list(Params)).

do_lookup_chain(#{id := ChainID}, _Params) ->
    case emqx_authn:lookup_chain(ChainID) of
        {ok, Chain} ->
            return({ok, Chain});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_chains(Binding, Params) ->
    do_list_chains(uri_decode(Binding), maps:from_list(Params)).

do_list_chains(_Binding, _Params) ->
    {ok, Chains} = emqx_authn:list_chains(),
    return({ok, Chains}).

create_service(Binding, Params) ->
    do_create_service(uri_decode(Binding), lists_to_map(Params)).


lists_to_map(L) ->
    lists_to_map(L, #{}).

lists_to_map([], Acc) ->
    Acc;
lists_to_map([{K, V} | More], Acc) when is_list(V) ->
    NV = lists_to_map(V),
    lists_to_map(More, Acc#{K => NV});
lists_to_map([{K, V} | More], Acc) ->
    lists_to_map(More, Acc#{K => V}).

% hocon_schema:check_plain(emqx_authn_schema,
%                                    #{<<"chain">> => #{<<"id">> => <<"chain 1">>,
%                                      <<"services">> => [#{<<"config">> => #{<<"user_id_type">> => <<"username">>,
%                                                                             <<"password_hash_algorithm">> => <<"sha256">>},
%                                                           <<"name">> => <<"service 1">>,
%                                                           <<"type">> => <<"mnesia">>}]}},
%                                    #{atom_key => true, nullable => true},
%                                    [chain]).


do_create_service(#{id := ChainID}, Service0) ->
    #{chain := #{services := [Service]}}
        = hocon_schema:check_plain(emqx_authn_schema,
                                   #{<<"chain">> => #{<<"id">> => ChainID,
                                                      <<"services">> => [Service0]}},
                                   #{atom_key => true, nullable => true},
                                   [chain]),
    case emqx_authn:create_service(ChainID, Service) of
        {ok, NService} ->
            return({ok, NService});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_service(Binding, Params) ->
    do_delete_service(uri_decode(Binding), maps:from_list(Params)).

do_delete_service(#{id := ChainID,
                    service_name := ServiceName}, _Params) ->
    case emqx_authn:delete_services(ChainID, [ServiceName]) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

%% TODO: Support incremental update
update_service(Binding, Params) ->
    do_update_service(uri_decode(Binding), lists_to_map(Params)).

%% TOOD: PUT method supports creation and update
do_update_service(#{id := ChainID,
                    service_name := ServiceName}, ServiceConfig) ->
    case emqx_authn:lookup_service(ChainID, ServiceName) of
        {ok, #{type := Type}} ->
            Service = #{<<"name">> => ServiceName,
                        <<"type">> => Type,
                        <<"config">> => ServiceConfig},
            #{chain := #{services := [#{config := NServiceConfig}]}}
                = hocon_schema:check_plain(emqx_authn_schema,
                                           #{<<"chain">> => #{<<"id">> => ChainID,
                                                              <<"services">> => [Service]}},
                                           #{atom_key => true, nullable => true},
                                           [chain]),
            case emqx_authn:update_service(ChainID, ServiceName, NServiceConfig) of
                {ok, NService} ->
                    return({ok, NService});
                {error, Reason} ->
                    return(serialize_error(Reason))
            end;
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_service(Binding, Params) ->
    do_lookup_service(uri_decode(Binding), maps:from_list(Params)).

do_lookup_service(#{id := ChainID,
                    service_name := ServiceName}, _Params) ->
    case emqx_authn:lookup_service(ChainID, ServiceName) of
        {ok, Service} ->
            return({ok, Service});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_services(Binding, Params) ->
    do_list_services(uri_decode(Binding), maps:from_list(Params)).

do_list_services(#{id := ChainID}, _Params) ->
    case emqx_authn:list_services(ChainID) of
        {ok, Services} ->
            return({ok, Services});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

move_service(Binding, Params) ->
    do_move_service(uri_decode(Binding), maps:from_list(Params)).

do_move_service(#{id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := <<"the front">>}) ->
    case emqx_authn:move_service_to_the_front(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(#{id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := <<"the end">>}) ->
    case emqx_authn:move_service_to_the_end(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(#{id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := N}) when is_number(N) ->
    case emqx_authn:move_service_to_the_nth(ChainID, ServiceName, N) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(_Binding, _Params) ->
    return(serialize_error({missing_parameter, <<"position">>})).

import_users(Binding, Params) ->
    do_import_users(uri_decode(Binding), maps:from_list(Params)).

do_import_users(#{id := ChainID, service_name := ServiceName},
                #{<<"filename">> := Filename}) ->
    case emqx_authn:import_users(ChainID, ServiceName, Filename) of
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

do_add_user(#{id := ChainID,
              service_name := ServiceName}, UserInfo) ->
    case emqx_authn:add_user(ChainID, ServiceName, UserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_user(Binding, Params) ->
    do_delete_user(uri_decode(Binding), maps:from_list(Params)).

do_delete_user(#{id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, _Params) ->
    case emqx_authn:delete_user(ChainID, ServiceName, UserID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

update_user(Binding, Params) ->
    do_update_user(uri_decode(Binding), maps:from_list(Params)).

do_update_user(#{id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, NewUserInfo) ->
    case emqx_authn:update_user(ChainID, ServiceName, UserID, NewUserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_user(Binding, Params) ->
    do_lookup_user(uri_decode(Binding), maps:from_list(Params)).

do_lookup_user(#{id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, _Params) ->
    case emqx_authn:lookup_user(ChainID, ServiceName, UserID) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_users(Binding, Params) ->
    do_list_users(uri_decode(Binding), maps:from_list(Params)).

do_list_users(#{id := ChainID,
                service_name := ServiceName}, _Params) ->
    case emqx_authn:list_users(ChainID, ServiceName) of
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

serialize_error({already_exists, {Type, ID}}) ->
    {error, <<"ALREADY_EXISTS">>, list_to_binary(io_lib:format("~s '~s' already exists", [serialize_type(Type), ID]))};
serialize_error({not_found, {Type, ID}}) ->
    {error, <<"NOT_FOUND">>, list_to_binary(io_lib:format("~s '~s' not found", [serialize_type(Type), ID]))};
serialize_error({duplicate, Name}) ->
    {error, <<"INVALID_PARAMETER">>, list_to_binary(io_lib:format("Service name '~s' is duplicated", [Name]))};
serialize_error({missing_parameter, Names = [_ | Rest]}) ->
    Format = ["~s," || _ <- Rest] ++ ["~s"],
    NFormat = binary_to_list(iolist_to_binary(Format)),
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameters " ++ NFormat ++ " that are mandatory for processing this request are not supplied.", Names))};
serialize_error({missing_parameter, Name}) ->
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameter '~s' that is mandatory for processing this request is not supplied.", [Name]))};
serialize_error(_) ->
    {error, <<"UNKNOWN_ERROR">>, <<"Unknown error">>}.

serialize_type(service) ->
    "Service";
serialize_type(chain) ->
    "Chain";
serialize_type(service_type) ->
    "Service type".

get_missed_params(Actual, Expected) ->
    Keys = lists:foldl(fun(Key, Acc) ->
                           case maps:is_key(Key, Actual) of
                               true -> Acc;
                               false -> [Key | Acc]
                           end
                       end, [], Expected),
    lists:reverse(Keys).
