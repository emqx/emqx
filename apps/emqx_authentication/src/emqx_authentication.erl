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

-module(emqx_authentication).

-include("emqx_authentication.hrl").

-export([ enable/0
        , disable/0
        ]).

-export([authenticate/1]).

-export([register_service_types/0]).

-export([ create_chain/1
        , delete_chain/1
        , lookup_chain/1
        , list_chains/0
        , add_services/2
        , delete_services/2
        , update_service/3
        , lookup_service/2
        , list_services/1
        , move_service_to_the_front/2
        , move_service_to_the_end/2
        , move_service_to_the_nth/3
        ]).

-export([ import_users/3
        , add_user/3
        , delete_user/3
        , update_user/4
        , lookup_user/3
        , list_users/2
        ]).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(CHAIN_TAB, emqx_authentication_chain).
-define(SERVICE_TYPE_TAB, emqx_authentication_service_type).

-rlog_shard({?AUTH_SHARD, ?CHAIN_TAB}).
-rlog_shard({?AUTH_SHARD, ?SERVICE_TYPE_TAB}).

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
    case emqx:hook('client.authenticate', {emqx_authentication, authenticate, []}) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

disable() ->
    emqx:unhook('client.authenticate', {emqx_authentication, authenticate}),
    ok.

authenticate(#{chain_id := ChainID} = ClientInfo) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [#chain{services = []}] ->
            {error, no_services};
        [#chain{services = Services}] ->
            do_authenticate(Services, ClientInfo);
        [] ->
            {error, todo}
    end.

do_authenticate([], _) ->
    {error, user_not_found};
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

create_chain(#{id := ID}) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ID, write) of
                [] ->
                    Chain = #chain{id = ID,
                                   services = [],
                                   created_at = erlang:system_time(millisecond)},
                    mnesia:write(?CHAIN_TAB, Chain, write),
                    {ok, serialize_chain(Chain)};
                [_ | _] ->
                    {error, {already_exists, {chain, ID}}}
            end
        end).

delete_chain(ID) ->
    trans(
        fun() ->
            case mnesia:read(?CHAIN_TAB, ID, write) of
                [] ->
                    {error, {not_found, {chain, ID}}};
                [#chain{services = Services}] ->
                    ok = delete_services_(Services),
                    mnesia:delete(?CHAIN_TAB, ID, write)
            end
        end).

lookup_chain(ID) ->
    case mnesia:dirty_read(?CHAIN_TAB, ID) of
        [] ->
            {error, {not_found, {chain, ID}}};
        [Chain] ->
            {ok, serialize_chain(Chain)}
    end.

list_chains() ->
    Chains = ets:tab2list(?CHAIN_TAB),
    {ok, [serialize_chain(Chain) || Chain <- Chains]}.

add_services(ChainID, ServiceParams) ->
    case validate_service_params(ServiceParams) of
        {ok, NServiceParams} ->
            UpdateFun = fun(Chain = #chain{services = Services}) ->
                            Names = [Name || {Name, _} <- Services] ++ [Name || #{name := Name} <- NServiceParams],
                            case no_duplicate_names(Names) of
                                ok ->
                                    case create_services(ChainID, NServiceParams) of
                                        {ok, NServices} ->
                                            NChain = Chain#chain{services = Services ++ NServices},
                                            ok = mnesia:write(?CHAIN_TAB, NChain, write),
                                            {ok, serialize_services(NServices)};
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

delete_services(ChainID, ServiceNames) ->
    case no_duplicate_names(ServiceNames) of
        ok ->
            UpdateFun = fun(Chain = #chain{services = Services}) ->
                            case extract_services(ServiceNames, Services) of
                                {ok, Extracted, Rest} ->
                                    ok = delete_services_(Extracted),
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

update_service(ChainID, ServiceName, NewParams) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case proplists:get_value(ServiceName, Services, undefined) of
                        undefined ->
                            {error, {not_found, {service, ServiceName}}};
                        #service{type     = Type,
                                 provider = Provider,
                                 params   = OriginalParams,
                                 state    = State} = Service ->
                            Params = maps:merge(OriginalParams, NewParams),
                            {ok, #service_type{params_spec = ParamsSpec}} = find_service_type(Type),
                            NParams = emqx_rule_validator:validate_params(Params, ParamsSpec),
                            case Provider:update(ChainID, ServiceName, NParams, State) of
                                {ok, NState} ->
                                    NService = Service#service{params = Params,
                                                               state = NState},
                                    NServices = lists:keyreplace(ServiceName, 1, Services, {ServiceName, NService}),
                                    ok = mnesia:write(?CHAIN_TAB, Chain#chain{services = NServices}, write),
                                    {ok, serialize_service({ServiceName, NService})};
                                {error, Reason} ->
                                    {error, Reason}
                            end
                    end
                 end,
    update_chain(ChainID, UpdateFun).

lookup_service(ChainID, ServiceName) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{services = Services}] ->
            case lists:keytake(ServiceName, 1, Services) of
                {value, Service, _} ->
                    {ok, serialize_service(Service)};
                false ->
                    {error, {not_found, {service, ServiceName}}}
            end
    end.

list_services(ChainID) ->
    case mnesia:dirty_read(?CHAIN_TAB, ChainID) of
        [] ->
            {error, {not_found, {chain, ChainID}}};
        [#chain{services = Services}] ->
            {ok, serialize_services(Services)}
    end.

move_service_to_the_front(ChainID, ServiceName) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_the_front_(ServiceName, Services) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_service_to_the_end(ChainID, ServiceName) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_the_end_(ServiceName, Services) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

move_service_to_the_nth(ChainID, ServiceName, N) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case move_service_to_the_nth_(ServiceName, Services, N) of
                        {ok, NServices} ->
                            NChain = Chain#chain{services = NServices},
                            mnesia:write(?CHAIN_TAB, NChain, write);
                        {error, Reason} ->
                            {error, Reason}
                    end
                 end,
    update_chain(ChainID, UpdateFun).

import_users(ChainID, ServiceName, Filename) ->
    call_service(ChainID, ServiceName, import_users, [Filename]).

add_user(ChainID, ServiceName, UserInfo) ->
    call_service(ChainID, ServiceName, add_user, [UserInfo]).

delete_user(ChainID, ServiceName, UserID) ->
    call_service(ChainID, ServiceName, delete_user, [UserID]).

update_user(ChainID, ServiceName, UserID, NewUserInfo) ->
    call_service(ChainID, ServiceName, update_user, [UserID, NewUserInfo]).

lookup_user(ChainID, ServiceName, UserID) ->
    call_service(ChainID, ServiceName, lookup_user, [UserID]).

list_users(ChainID, ServiceName) ->
    call_service(ChainID, ServiceName, list_users, []).

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
                                                          original_params => Params,
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
create_services(ChainID, [#{name := Name,
                            type := Type,
                            provider := Provider,
                            params := Params,
                            original_params := OriginalParams} | More], Acc) ->
    case Provider:create(ChainID, Name, Params) of
        {ok, State} ->
            Service = #service{name = Name,
                               type = Type,
                               provider = Provider,
                               params = OriginalParams,
                               state = State},
            create_services(ChainID, More, [{Name, Service} | Acc]);
        {error, Reason} ->
            delete_services_(Acc),
            {error, Reason}
    end.

delete_services_([]) ->
    ok;
delete_services_([{_, #service{provider = Provider, state = State}} | More]) ->
    Provider:destroy(State),
    delete_services_(More).

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

move_service_to_the_front_(ServiceName, Services) ->
    move_service_to_the_front_(ServiceName, Services, []).

move_service_to_the_front_(ServiceName, [], _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_the_front_(ServiceName, [{ServiceName, _} = Service | More], Passed) ->
    {ok, [Service | (lists:reverse(Passed) ++ More)]};
move_service_to_the_front_(ServiceName, [Service | More], Passed) ->
    move_service_to_the_front_(ServiceName, More, [Service | Passed]).

move_service_to_the_end_(ServiceName, Services) ->
    move_service_to_the_end_(ServiceName, Services, []).

move_service_to_the_end_(ServiceName, [], _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_the_end_(ServiceName, [{ServiceName, _} = Service | More], Passed) ->
    {ok, lists:reverse(Passed) ++ More ++ [Service]};
move_service_to_the_end_(ServiceName, [Service | More], Passed) ->
    move_service_to_the_end_(ServiceName, More, [Service | Passed]).

move_service_to_the_nth_(ServiceName, Services, N)
  when N =< length(Services) andalso N > 0 ->
    move_service_to_the_nth_(ServiceName, Services, N, []);
move_service_to_the_nth_(_, _, _) ->
    {error, out_of_range}.

move_service_to_the_nth_(ServiceName, [], _, _) ->
    {error, {not_found, {service, ServiceName}}};
move_service_to_the_nth_(ServiceName, [{ServiceName, _} = Service | More], N, Passed)
  when N =< length(Passed) ->
    {L1, L2} = lists:split(N - 1, lists:reverse(Passed)),
    {ok, L1 ++ [Service] ++ L2 ++ More};
move_service_to_the_nth_(ServiceName, [{ServiceName, _} = Service | More], N, Passed) ->
    {L1, L2} = lists:split(N - length(Passed) - 1, More),
    {ok, lists:reverse(Passed) ++ L1 ++ [Service] ++ L2};
move_service_to_the_nth_(ServiceName, [Service | More], N, Passed) ->
    move_service_to_the_nth_(ServiceName, More, N, [Service | Passed]).

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

serialize_chain(#chain{id = ID,
                       services = Services,
                       created_at = CreatedAt}) ->
    #{id => ID,
      services => serialize_services(Services),
      created_at => CreatedAt}.

serialize_services(Services) ->
    [serialize_service(Service) || Service <- Services].

serialize_service({_, #service{name = Name,
                               type = Type,
                               params = Params}}) ->
    #{name => Name,
      type => Type,
      params => Params}.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.
