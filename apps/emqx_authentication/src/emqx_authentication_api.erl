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
        , add_services/2
        , delete_services/2
        , update_service/2
        , lookup_service/2
        , list_services/2
        , move_service/2
        , import_user_credentials/2
        , add_user_creadential/2
        ]).

-import(minirest,  [return/1]).

-rest_api(#{name   => list_chains,
            method => 'GET',
            path   => "/authentication/chains",
            func   => list_chains,
            descr  => "List all chains"
           }).

-rest_api(#{name   => create_chain,
            method => 'POST',
            path   => "/authentication/chains",
            func   => create_chain,
            descr  => "Create a chain"
           }).

create_chain(_Binding, Params = #{chain_id := ChainID}) ->
    case emqx_authentication:create_chain(Params) of
        {ok, ChainID} ->
            return({ok, ChainID});
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
create_chain(_Binding, _Params) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

delete_chain(_Binding, #{chain_id := ChainID}) ->
    case emqx_authentication:delete_chain(ChainID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end.

lookup_chain(_Binding, #{chain_id := ChainID}) ->
    case emqx_authentication:lookup_chain(ChainID) of
        {ok, Chain} ->
            return({ok, Chain});
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
lookup_chain(_Binding, _Params) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

list_chains(_Binding, _Params) ->
    emqx_authentication:list_chains().

add_services(_Binding, Params = #{chain_id := ChainID}) ->
    case maps:get(services, Params, []) of
        [] -> return(ok);
        Services ->
            case emqx_authentication:add_services(ChainID, Services) of
                ok ->
                    return(ok);
                {error, Reason} ->
                    return({error, serialize_error(Reason)})
            end
    end;
add_services(_Binding, _Params) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

delete_services(_Binding, #{chain_id := ChainID,
                            service_names := ServiceNames}) ->
    case emqx_authentication:delete_services(ChainID, ServiceNames) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
delete_services(_Binding, #{chain_id := _}) ->
    return({error, serialize_error({missing_parameter, service_names})});
delete_services(_Binding, #{service_names := _}) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

%% TODO: better input parameters
update_service(_Binding, #{chain_id := ChainID,
                           service_name := ServiceName,
                           service_params := Params}) ->
    case emqx_authentication:update_service(ChainID, ServiceName, Params) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
update_service(_Binding, #{chain_id := _}) ->
    return({error, serialize_error({missing_parameter, service_name})});
update_service(_Binding, #{service_name := _}) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

lookup_service(_Binding, #{chain_id := ChainID,
                           service_name := ServiceName}) ->
    case emqx_authentication:lookup_service(ChainID, ServiceName) of
        {ok, Service} ->
            return({ok, Service});
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
lookup_service(_Binding, #{chain_id := _}) ->
    return({error, serialize_error({missing_parameter, service_name})});
lookup_service(_Binding, #{service_name := _}) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

list_services(_Binding, #{chain_id := ChainID}) ->
    case emqx_authentication:list_services(ChainID) of
        {ok, Service} ->
            return({ok, Service});
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
list_services(_Binding, _Params) ->
    return({error, serialize_error({missing_parameter, chain_id})}).

move_service(_Binding, #{chain_id := ChainID,
                         service_name := ServiceName,
                         to := <<"the front">>}) ->
    case emqx_authenticaiton:move_service_to_the_front(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
move_service(_Binding, #{chain_id := ChainID,
                         service_name := ServiceName,
                         to := <<"the end">>}) ->
    case emqx_authenticaiton:move_service_to_the_end(ChainID, ServiceName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
move_service(_Binding, #{chain_id := ChainID,
                         service_name := ServiceName,
                         to := N}) when is_number(N) ->
    case emqx_authenticaiton:move_service_to_the_nth(ChainID, ServiceName, N) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
move_service(_Binding, Params) ->
    Missed = get_missed_params(Params, [chain_id, service_name, to]),
    return({error, serialize_error({missing_parameter, Missed})}).

import_user_credentials(_Binding, #{chain_id := ChainID,
                                    service_name := ServiceName,
                                    filename := Filename,
                                    file_format := FileFormat}) ->
    case emqx_authentication:import_user_credentials(ChainID, ServiceName, Filename, FileFormat) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
import_user_credentials(_Binding, Params) ->
    Missed = get_missed_params(Params, [chain_id, service_name, filename, file_format]),
    return({error, serialize_error({missing_parameter, Missed})}).

add_user_creadential(_Binding, #{chain_id := ChainID,
                                 service_name := ServiceName,
                                 credential := Credential}) ->
    case emqx_authentication:add_user_creadentials(ChainID, ServiceName, Credential) of
        ok ->
            return(ok);
        {error, Reason} ->
            return({error, serialize_error(Reason)})
    end;
add_user_creadential(_Binding, Params) ->
    Missed = get_missed_params(Params, [chain_id, service_name, credential]),
    return({error, serialize_error({missing_parameter, Missed})}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

serialize_error(Reason) when not is_map(Reason) ->
    Error = serialize_error_(Reason),
    emqx_json:encode(Error).

serialize_error_({already_exists, {Type, ID}}) ->
    #{code => "ALREADY_EXISTS",
      message => io_lib:format("~p ~p already exists", [serialize_type(Type), ID])};
serialize_error_({not_found, {Type, ID}}) ->
    #{code => "NOT_FOUND",
      message => io_lib:format("~p ~p not found", [serialize_type(Type), ID])};
serialize_error_({duplicate, Name}) ->
    #{code => "INVALID_PARAMETER",
      message => io_lib:format("Service name ~p is duplicated", [Name])};
serialize_error_({missing_parameter, Names = [_ | Rest]}) ->
    Format = ["~p," || _ <- Rest] ++ ["~p"],
    NFormat = binary_to_list(iolist_to_binary(Format)),
    #{code => "MISSING_PARAMETER",
      message => io_lib:format("The input parameters " ++ NFormat ++ " that are mandatory for processing this request are not supplied.", Names)};
serialize_error_({missing_parameter, Name}) ->
    #{code => "MISSING_PARAMETER",
      message => io_lib:format("The input parameter ~p that is mandatory for processing this request is not supplied.", [Name])};
serialize_error_(_) ->
    #{code => "UNKNOWN_ERROR"}.

serialize_type(service) ->
    "Service";
serialize_type(chain) ->
    "Chain";
serialize_type(service_type) ->
    "Service type".

get_missed_params(Actual, Expected) ->
    Keys = lists:fold(fun(Key, Acc) ->
                          case maps:is_key(Key, Actual) of
                              true -> Acc;
                              false -> [Key | Acc]
                          end
                      end, [], Expected),
    lists:reverse(Keys).