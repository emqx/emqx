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

-module(emqx_authentication).

-include("emqx_authentication.hrl").

-export([ enable/0
        , disable/0
        ]).

-export([authenticate/1]).

-export([register_service_types/0]).

-export([ create_chain/1
        , delete_chain/1
        , add_services_to_chain/2
        , delete_services_from_chain/2
        , move_service_to_the_front_of_chain/2
        , move_service_to_the_end_of_chain/2
        , move_service_to_the_nth_of_chain/3
        ]).

-export([ import_user_credentials/4
        , add_user_credential/3
        , delete_user_credential/3
        , update_user_credential/3
        , lookup_user_credential/3
        ]).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(CHAIN_TAB, emqx_authentication_chain).
-define(SERVICE_TYPE_TAB, emqx_authentication_service_type).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true}]}],
    %% Chain table
    ok = ekka_mnesia:create_table(?CHAIN_TAB, [
                {disc_copies, [node()]},
                {record_name, chain},
                {attributes, record_info(fields, chain)},
                {storage_properties, StoreProps}]),
    %% Service type table
    ok = ekka_mnesia:create_table(?SERVICE_TYPE_TAB, [
                {ram_copies, [node()]},
                {record_name, service_type},
                {attributes, record_info(fields, service_type)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    %% Copy chain table
    ok = ekka_mnesia:copy_table(?CHAIN_TAB, disc_copies),
    %% Copy service type table
    ok = ekka_mnesia:copy_table(?SERVICE_TYPE_TAB, ram_copies).

enable() ->
    case emqx:hook('client.authenticate', fun emqx_authentication:authenticate/2) of
        ok -> ok;
        {error, already_exists} -> ok
    end,
    case emqx:hook('client.enhanced_authenticate', fun emqx_authentication:enhanced_authenticate/2) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

disable() ->
    emqx:unhook('client.authenticate', {}),
    emqx:unhook('client.enhanced_authenticate', {}),
    ok.

authenticate(#{chain_id := ChainID} = ClientInfo) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [#chain{services = []}] ->
            {error, todo};
        [#chain{services = Services}] ->
            do_authenticate(Services, ClientInfo);
        [] ->
            {error, todo}
    end.

do_authenticate([], _) ->
    {error, user_credential_not_found};
do_authenticate([{_, #service{provider = Provider, state = State}} | More], ClientInfo) ->
    case Provider:authenticate(ClientInfo, State) of
        ignore -> do_authenticate(More, ClientInfo);
        ok -> ok;
        {ok, NewClientInfo} -> {ok, NewClientInfo};
        {stop, Reason} -> {error, Reason}
    end.

register_service_types() ->
    Attrs = find_attrs(?APP, service_type),
    register_service_types(Attrs).

register_service_types(Attrs) ->
    register_service_types(Attrs, []).

register_service_types([], Acc) ->
    do_register_service_types(Acc);
register_service_types([{_App, Mod, #{name := Name,
                                      params_spec := ParamsSpec}} | Types], Acc) ->
    %% TODO: Temporary realization
    ok = emqx_rule_validator:validate_spec(ParamsSpec),
    ServiceType = #service_type{name = Name,
                                provider = Mod,
                                params_spec = ParamsSpec},
    register_service_types(Types, [ServiceType | Acc]).

create_chain(Params = #{chain_id := ChainID}) ->
    ServiceParams = maps:get(service_params, Params, []),
    case validate_service_params(ServiceParams) of
        {ok, NServiceParams} ->
            trans(
                fun() ->
                    case mnesia:read(?CHAIN_TAB, ChainID, write) of
                        [] ->
                            case create_services(ChainID, NServiceParams) of
                                {ok, Services} ->
                                    Chain = #chain{id = ChainID,
                                                   services = Services,
                                                   created_at = erlang:system_time(millisecond)},
                                    mnesia:write(?CHAIN_TAB, Chain, write),
                                    {ok, Chain};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        [_ | _] ->
                            {error, {already_exists, {chain, ChainID}}}
                    end
                end);
        {error, Reason} ->
            {error, Reason}
    end.

delete_chain(ChainID) ->
    mnesia:transaction(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ChainID, write) of
                [] ->
                    {error, {not_found, {chain, ChainID}}};
                [#chain{services = Services}] ->
                    ok = delete_services(Services),
                    mnesia:delete(?CHAIN_TAB, ChainID, write)
            end
        end).

add_services_to_chain(ChainID, ServiceParams) ->
    case validate_service_params(ServiceParams) of
        {ok, NServiceParams} ->
            UpdateFun = fun(Chain = #chain{services = Services}) ->
                            Names = [Name || {Name, _} <- Services] ++ [Name || #{name := Name} <- NServiceParams],
                            case no_duplicate_names(Names) of
                                ok ->
                                    case create_services(ChainID, NServiceParams) of
                                        {ok, NServices} ->
                                            NChain = Chain#chain{services = Services ++ NServices},
                                            mnesia:write(?CHAIN_TAB, NChain, write);
                                        {error, Reason} ->
                                            {error, Reason}
                                    end;
                                {error, {duplicate, Name}} ->
                                    {error, {already_exists, {service, Name}}}
                            end
                        end,
            update_chain(ChainID, UpdateFun);
        {error, Reason} ->
            {error, Reason}
    end.

delete_services_from_chain(ChainID, ServiceNames) ->
    case no_duplicate_names(ServiceNames) of
        ok ->
            UpdateFun = fun(Chain = #chain{services = Services}) ->
                            case extract_services(ServiceNames, Services) of
                                {ok, Extracted, Rest} ->
                                    ok = delete_services(Extracted),
                                    NChain = Chain#chain{services = Rest},
                                    mnesia:write(?CHAIN_TAB, NChain, write);
                                {error, Reason} ->
                                    {error, Reason}
                            end
                        end,
            update_chain(ChainID, UpdateFun);
        {error, Reason} ->
            {error, Reason}
    end.

move_service_to_the_front_of_chain(ChainID, ServiceName) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_the_front(ServiceName, Services) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_service_to_the_end_of_chain(ChainID, ServiceName) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_the_end(ServiceName, Services) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_service_to_the_nth_of_chain(ChainID, ServiceName, N) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_nth(ServiceName, Services, N) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

update_chain(ChainID, UpdateFun) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ChainID, write) of
                [] ->
                    {error, {not_found, {chain, ChainID}}};
                [Chain] ->
                    UpdateFun(Chain)
            end
        end).

import_user_credentials(ChainID, ServiceName, Filename, FileFormat) ->
    call_service(ChainID, ServiceName, import_user_credentials, [Filename, FileFormat]).

add_user_credential(ChainID, ServiceName, Credential) ->
    call_service(ChainID, ServiceName, add_user_credential, [Credential]).

delete_user_credential(ChainID, ServiceName, UserIdentity) ->
    call_service(ChainID, ServiceName, delete_user_credential, [UserIdentity]).

update_user_credential(ChainID, ServiceName, Credential) ->
    call_service(ChainID, ServiceName, update_user_credential, [Credential]).

lookup_user_credential(ChainID, ServiceName, UserIdentity) ->
    call_service(ChainID, ServiceName, lookup_user_credential, [UserIdentity]).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

find_attrs(App, AttrName) ->
    [{App, Mod, Attr} || {ok, Modules} <- [application:get_key(App, modules)],
                         Mod <- Modules,
                         {Name, Attrs} <- module_attributes(Mod), Name =:= AttrName,
                         Attr <- Attrs].

module_attributes(Module) ->
    try Module:module_info(attributes)
    catch
        error:undef -> []
    end.

do_register_service_types(ServiceTypes) ->
    trans(fun lists:foreach/2, [fun insert_service_type/1, ServiceTypes]).

insert_service_type(ServiceType) ->
    mnesia:write(?SERVICE_TYPE_TAB, ServiceType, write).

find_service_type(Name) ->
    case mnesia:dirty_read(?SERVICE_TYPE_TAB, Name) of
        [ServiceType] -> {ok, ServiceType};
        [] -> {error, not_found}
    end.

validate_service_params(ServiceParams) ->
    case validate_service_names(ServiceParams) of
        ok ->
            validate_other_service_params(ServiceParams);
        {error, Reason} ->
            {error, Reason}
    end.

validate_service_names(ServiceParams) ->
    Names = [Name || #{name := Name} <- ServiceParams],
    no_duplicate_names(Names).

validate_other_service_params(ServiceParams) ->
    validate_other_service_params(ServiceParams, []).

validate_other_service_params([], Acc) ->
    {ok, lists:reverse(Acc)};
validate_other_service_params([#{type := Type, params := Params} = ServiceParams | More], Acc) ->
    case find_service_type(Type) of
        {ok, #service_type{provider = Provider, params_spec = ParamsSpec}} ->
            NParams = emqx_rule_validator:validate_params(Params, ParamsSpec),
            validate_other_service_params(More,
                                          [ServiceParams#{params => NParams,
                                                          provider => Provider} | Acc]);
        {error, not_found} ->
            {error, {not_found, {service_type, Type}}}
    end.
    
no_duplicate_names(Names) ->
    no_duplicate_names(Names, #{}).

no_duplicate_names([], _) ->
    ok;
no_duplicate_names([Name | More], Acc) ->
    case maps:is_key(Name, Acc) of
        false -> no_duplicate_names(More, Acc#{Name => true});
        true -> {error, {duplicate, Name}}
    end.

create_services(ChainID, ServiceParams) ->
    create_services(ChainID, ServiceParams, []).

create_services(_ChainID, [], Acc) ->
    {ok, lists:reverse(Acc)};
create_services(ChainID, [#{name := Name, type := Type, provider := Provider, params := Params} | More], Acc) ->
    case Provider:create(ChainID, Name, Params) of
        {ok, State} ->
            Service = #service{name = Name,
                               type = Type,
                               provider = Provider,
                               state = State},
            create_services(ChainID, More, [{Name, Service} | Acc]);
        {error, Reason} ->
            delete_services(Acc),
            {error, Reason}
    end.

delete_services([]) ->
    ok;
delete_services([{_, #service{provider = Provider, state = State}} | More]) ->
    Provider:destroy(State),
    delete_services(More).

extract_services(ServiceNames, Services) ->
    extract_services(ServiceNames, Services, []).

extract_services([], Rest, Extracted) ->
    {ok, lists:reverse(Extracted), Rest};
extract_services([ServiceName | More], Services, Acc) ->
    case lists:keytake(ServiceName, 1, Services) of
        {value, Extracted, Rest} ->
            extract_services(More, Rest, [Extracted | Acc]);
        false ->
            {error, {not_found, {service, ServiceName}}}
    end.
    
move_service_to_the_front(ServiceName, Services) ->
    move_service_to_the_front(ServiceName, Services, []).

move_service_to_the_front(ServiceName, [], _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_the_front(ServiceName, [{ServiceName, _} = Service | More], Passed) ->
    {ok, [Service | (lists:reverse(Passed) ++ More)]};
move_service_to_the_front(ServiceName, [Service | More], Passed) ->
    move_service_to_the_front(ServiceName, More, [Service | Passed]).

move_service_to_the_end(ServiceName, Services) ->
    move_service_to_the_end(ServiceName, Services, []).

move_service_to_the_end(ServiceName, [], _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_the_end(ServiceName, [{ServiceName, _} = Service | More], Passed) ->
    {ok, lists:reverse(Passed) ++ More ++ [Service]};
move_service_to_the_end(ServiceName, [Service | More], Passed) ->
    move_service_to_the_end(ServiceName, More, [Service | Passed]).

move_service_to_nth(ServiceName, Services, N)
  when length(Services) < N ->
    move_service_to_nth(ServiceName, Services, N, []);
move_service_to_nth(_, _, _) ->
    {error, out_of_range}.

move_service_to_nth(ServiceName, [], _, _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_nth(ServiceName, [{ServiceName, _} = Service | More], N, Passed)
  when N =< length(Passed) ->
    {L1, L2} = lists:split(N - 1, lists:reverse(Passed)),
    {ok, L1 ++ [Service] + L2 + More};
move_service_to_nth(ServiceName, [{ServiceName, _} = Service | More], N, Passed) ->
    {L1, L2} = lists:split(N - length(Passed) - 1, More),
    {ok, lists:reverse(Passed) ++ L1 ++ [Service] ++ L2}.

call_service(ChainID, ServiceName, Func, Args) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{services = Services}] ->
            case proplists:get_value(ServiceName, Services, undefined) of
                undefined ->
                    {error, {not_found, {service, ServiceName}}};
                #service{provider = Provider,
                         state = State} ->
                    case erlang:function_exported(Provider, Func, length(Args) + 1) of
                        true ->
                            erlang:apply(Provider, Func, Args ++ [State]);
                        false ->
                            {error, unsupported_feature}
                    end
            end
    end.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case mnesia:transaction(Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.