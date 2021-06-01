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

-module(emqx_authentication_api).

-export([ create_chain/2
        , delete_chain/2
        , lookup_chain/2
        , list_chains/2
        , add_service/2
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
            path   => "/authentication/chains/:bin:chain_id",
            func   => delete_chain,
            descr  => "Delete chain"
           }).

-rest_api(#{name   => lookup_chain,
            method => 'GET',
            path   => "/authentication/chains/:bin:chain_id",
            func   => lookup_chain,
            descr  => "Lookup chain"
           }).

-rest_api(#{name   => list_chains,
            method => 'GET',
            path   => "/authentication/chains",
            func   => list_chains,
            descr  => "List all chains"
           }).

-rest_api(#{name   => add_service,
            method => 'POST',
            path   => "/authentication/chains/:bin:chain_id/services",
            func   => add_service,
            descr  => "Add service to chain"
           }).

-rest_api(#{name   => delete_service,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name",
            func   => delete_service,
            descr  => "Delete service from chain"
           }).

-rest_api(#{name   => update_service,
            method => 'PUT',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name",
            func   => update_service,
            descr  => "Update service in chain"
           }).

-rest_api(#{name   => lookup_service,
            method => 'GET',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name",
            func   => lookup_service,
            descr  => "Lookup service in chain"
           }).

-rest_api(#{name   => list_services,
            method => 'GET',
            path   => "/authentication/chains/:bin:chain_id/services",
            func   => list_services,
            descr  => "List services in chain"
           }).

-rest_api(#{name   => move_service,
            method => 'POST',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/position",
            func   => move_service,
            descr  => "Change the order of services"
           }).

-rest_api(#{name   => import_users,
            method => 'POST',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/import-users",
            func   => import_users,
            descr  => "Import users"
           }).

-rest_api(#{name   => add_user,
            method => 'POST',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/users",
            func   => add_user,
            descr  => "Add user"
           }).

-rest_api(#{name   => delete_user,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/users/:bin:user_id",
            func   => delete_user,
            descr  => "Delete user"
           }).

-rest_api(#{name   => update_user,
            method => 'PUT',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/users/:bin:user_id",
            func   => update_user,
            descr  => "Update user"
           }).

-rest_api(#{name   => lookup_user,
            method => 'GET',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/users/:bin:user_id",
            func   => lookup_user,
            descr  => "Lookup user"
           }).

%% TODO: Support pagination
-rest_api(#{name   => list_users,
            method => 'GET',
            path   => "/authentication/chains/:bin:chain_id/services/:bin:service_name/users",
            func   => list_users,
            descr  => "List all users"
           }).

create_chain(Binding, Params) ->
    do_create_chain(uri_decode(Binding), maps:from_list(Params)).

do_create_chain(_Binding, #{<<"chain_id">> := ChainID}) ->
    case emqx_authentication:create_chain(ChainID) of
        {ok, Chain} ->
            return({ok, Chain});
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_create_chain(_Binding, _Params) ->
    return(serialize_error({missing_parameter, chain_id})).

delete_chain(Binding, Params) ->
    do_delete_chain(uri_decode(Binding), maps:from_list(Params)).

do_delete_chain(#{chain_id := ChainID}, _Params) ->
    case emqx_authentication:delete_chain(ChainID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_chain(Binding, Params) ->
    do_lookup_chain(uri_decode(Binding), maps:from_list(Params)).

do_lookup_chain(#{chain_id := ChainID}, _Params) ->
    case emqx_authentication:lookup_chain(ChainID) of
        {ok, Chain} ->
            return({ok, Chain});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_chains(Binding, Params) ->
    do_list_chains(uri_decode(Binding), maps:from_list(Params)).

do_list_chains(_Binding, _Params) ->
    {ok, Chains} = emqx_authentication:list_chains(),
    return({ok, Chains}).

add_service(Binding, Params) ->
    do_add_service(uri_decode(Binding), maps:from_list(Params)).

do_add_service(#{chain_id := ChainID}, #{<<"name">> := Name,
                                         <<"type">> := Type,
                                         <<"params">> := Params}) ->
    case emqx_authentication:add_services(ChainID, [#{name => Name,
                                                      type => binary_to_existing_atom(Type, utf8),
                                                      params => maps:from_list(Params)}]) of
        {ok, Services} ->
            return({ok, Services});
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
%% TODO: Check missed field in params
do_add_service(_Binding, Params) ->
    Missed = get_missed_params(Params, [<<"name">>, <<"type">>, <<"params">>]),
    return(serialize_error({missing_parameter, Missed})).

delete_service(Binding, Params) ->
    do_delete_service(uri_decode(Binding), maps:from_list(Params)).

do_delete_service(#{chain_id := ChainID,
                    service_name := ServiceName}, _Params) ->
    case emqx_authentication:delete_services(ChainID, [ServiceName]) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

update_service(Binding, Params) ->
    do_update_service(uri_decode(Binding), maps:from_list(Params)).

%% TOOD: PUT 方法支持创建和更新
do_update_service(#{chain_id := ChainID,
                    service_name := ServiceName}, Params) ->
    case emqx_authentication:update_service(ChainID, ServiceName, Params) of
        {ok, Service} ->
            return({ok, Service});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_service(Binding, Params) ->
    do_lookup_service(uri_decode(Binding), maps:from_list(Params)).

do_lookup_service(#{chain_id := ChainID,
                    service_name := ServiceName}, _Params) ->
    case emqx_authentication:lookup_service(ChainID, ServiceName) of
        {ok, Service} ->
            return({ok, Service});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_services(Binding, Params) ->
    do_list_services(uri_decode(Binding), maps:from_list(Params)).

do_list_services(#{chain_id := ChainID}, _Params) ->
    case emqx_authentication:list_services(ChainID) of
        {ok, Services} ->
            return({ok, Services});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

move_service(Binding, Params) ->
    do_move_service(uri_decode(Binding), maps:from_list(Params)).

do_move_service(#{chain_id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := <<"the front">>}) ->
    case emqx_authentication:move_service_to_the_front(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(#{chain_id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := <<"the end">>}) ->
    case emqx_authentication:move_service_to_the_end(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(#{chain_id := ChainID,
                  service_name := ServiceName}, #{<<"position">> := N}) when is_number(N) ->
    case emqx_authentication:move_service_to_the_nth(ChainID, ServiceName, N) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_service(_Binding, _Params) ->
    return(serialize_error({missing_parameter, <<"position">>})).

import_users(Binding, Params) ->
    do_import_users(uri_decode(Binding), maps:from_list(Params)).

do_import_users(#{chain_id := ChainID, service_name := ServiceName},
                #{<<"filename">> := Filename}) ->
    case emqx_authentication:import_users(ChainID, ServiceName, Filename) of
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

do_add_user(#{chain_id := ChainID,
              service_name := ServiceName}, UserInfo) ->
    case emqx_authentication:add_user(ChainID, ServiceName, UserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_user(Binding, Params) ->
    do_delete_user(uri_decode(Binding), maps:from_list(Params)).

do_delete_user(#{chain_id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, _Params) ->
    case emqx_authentication:delete_user(ChainID, ServiceName, UserID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

update_user(Binding, Params) ->
    do_update_user(uri_decode(Binding), maps:from_list(Params)).

do_update_user(#{chain_id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, NewUserInfo) ->
    case emqx_authentication:update_user(ChainID, ServiceName, UserID, NewUserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_user(Binding, Params) ->
    do_lookup_user(uri_decode(Binding), maps:from_list(Params)).

do_lookup_user(#{chain_id := ChainID,
                 service_name := ServiceName,
                 user_id := UserID}, _Params) ->
    case emqx_authentication:lookup_user(ChainID, ServiceName, UserID) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_users(Binding, Params) ->
    do_list_users(uri_decode(Binding), maps:from_list(Params)).

do_list_users(#{chain_id := ChainID,
                service_name := ServiceName}, _Params) ->
    case emqx_authentication:list_users(ChainID, ServiceName) of
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
    {error, <<"ALREADY_EXISTS">>, list_to_binary(io_lib:format("~p ~p already exists", [serialize_type(Type), ID]))};
serialize_error({not_found, {Type, ID}}) ->
    {error, <<"NOT_FOUND">>, list_to_binary(io_lib:format("~p ~p not found", [serialize_type(Type), ID]))};
serialize_error({duplicate, Name}) ->
    {error, <<"INVALID_PARAMETER">>, list_to_binary(io_lib:format("Service name ~p is duplicated", [Name]))};
serialize_error({missing_parameter, Names = [_ | Rest]}) ->
    Format = ["~p," || _ <- Rest] ++ ["~p"],
    NFormat = binary_to_list(iolist_to_binary(Format)),
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameters " ++ NFormat ++ " that are mandatory for processing this request are not supplied.", Names))};
serialize_error({missing_parameter, Name}) ->
    {error, <<"MISSING_PARAMETER">>, list_to_binary(io_lib:format("The input parameter ~p that is mandatory for processing this request is not supplied.", [Name]))};
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
