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

-module(emqx_authn).

-include("emqx_authn.hrl").

-export([ enable/0
        , disable/0
        ]).

-export([authenticate/1]).

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

-define(CHAIN_TAB, emqx_authn_chain).

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
                {ram_copies, [node()]},
                {record_name, chain},
                {attributes, record_info(fields, chain)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    %% Copy chain table
    ok = ekka_mnesia:copy_table(?CHAIN_TAB, ram_copies).

enable() ->
    case emqx:hook('client.authenticate', fun emqx_authn:authenticate/1) of
        ok -> ok;
        {error, already_exists} -> ok
    end.

disable() ->
    emqx:unhook('client.authenticate', fun emqx_authn:authenticate/1),
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

add_services(ChainID, ServiceConfig) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    Names = [Name || {Name, _} <- Services] ++ [Name || #{name := Name} <- ServiceConfig],
                    case no_duplicate_names(Names) of
                        ok ->
                            case create_services(ChainID, ServiceConfig) of
                                {ok, NServices} ->
                                    NChain = Chain#chain{services = Services ++ NServices},
                                    ok = mnesia:write(?CHAIN_TAB, NChain, write),
                                    {ok, serialize_service(NServices)};
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        {error, {duplicate, Name}} ->
                            {error, {already_exists, {service, Name}}}
                    end
                end,
    update_chain(ChainID, UpdateFun).

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

update_service(ChainID, ServiceName, Config) ->
    UpdateFun = fun(Chain = #chain{services = Services}) ->
                    case proplists:get_value(ServiceName, Services, undefined) of
                        undefined ->
                            {error, {not_found, {service, ServiceName}}};
                        #service{provider   = Provider,
                                 config   = OriginalConfig,
                                 state    = State} = Service ->
                            NewConfig = maps:merge(OriginalConfig, Config),
                            case Provider:update(ChainID, ServiceName, NewConfig, State) of
                                {ok, NState} ->
                                    NService = Service#service{config = NewConfig,
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
            case lists:keyfind(ServiceName, 1, Services) of
                false ->
                    {error, {not_found, {service, ServiceName}}};
                Service ->
                    {ok, serialize_service({ServiceName, Service})}
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

no_duplicate_names(Names) ->
    no_duplicate_names(Names, #{}).

no_duplicate_names([], _) ->
    ok;
no_duplicate_names([Name | More], Acc) ->
    case maps:is_key(Name, Acc) of
        false -> no_duplicate_names(More, Acc#{Name => true});
        true -> {error, {duplicate, Name}}
    end.

create_services(ChainID, ServiceConfig) ->
    create_services(ChainID, ServiceConfig, []).

create_services(_ChainID, [], Acc) ->
    {ok, lists:reverse(Acc)};
create_services(ChainID, [#{name := Name,
                            type := Type,
                            config := Config,
                            original_config := OriginalConfig} | More], Acc) ->
    Provider = service_provider(Type),
    case Provider:create(ChainID, Name, Config) of
        {ok, State} ->
            Service = #service{name = Name,
                               type = Type,
                               provider = Provider,
                               config = OriginalConfig,
                               state = State},
            create_services(ChainID, More, [{Name, Service} | Acc]);
        {error, Reason} ->
            delete_services_(Acc),
            {error, Reason}
    end.

service_provider(mnesia) -> emqx_authn_mnesia;
service_provider(jwt) -> emqx_authn_jwt.

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
                #service{provider = Provider, state = State} ->
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
                               config = Config}}) ->
    #{name => Name,
      type => Type,
      config => Config}.

trans(Fun) ->
    trans(Fun, []).

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.
