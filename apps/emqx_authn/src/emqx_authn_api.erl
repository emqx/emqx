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

-export([ create_chain/2
        , delete_chain/2
        , lookup_chain/2
        , list_chains/2
        , bind/2
        , unbind/2
        , list_bindings/2
        , list_bound_chains/2
        , create_authenticator/2
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

-rest_api(#{name   => bind,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/bindings/bulk",
            func   => bind,
            descr  => "Bind"
           }).

-rest_api(#{name   => unbind,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id/bindings/bulk",
            func   => unbind,
            descr  => "Unbind"
           }).

-rest_api(#{name   => list_bindings,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/bindings",
            func   => list_bindings,
            descr  => "List bindings"
           }).

-rest_api(#{name   => list_bound_chains,
            method => 'GET',
            path   => "/authentication/listeners/:bin:listener_id/bound_chains",
            func   => list_bound_chains,
            descr  => "List bound chains"
           }).

-rest_api(#{name   => create_authenticator,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/authenticators",
            func   => create_authenticator,
            descr  => "Create authenticator to chain"
           }).

-rest_api(#{name   => delete_authenticator,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name",
            func   => delete_authenticator,
            descr  => "Delete authenticator from chain"
           }).

-rest_api(#{name   => update_authenticator,
            method => 'PUT',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name",
            func   => update_authenticator,
            descr  => "Update authenticator in chain"
           }).

-rest_api(#{name   => lookup_authenticator,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name",
            func   => lookup_authenticator,
            descr  => "Lookup authenticator in chain"
           }).

-rest_api(#{name   => list_authenticators,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/authenticators",
            func   => list_authenticators,
            descr  => "List authenticators in chain"
           }).

-rest_api(#{name   => move_authenticator,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/position",
            func   => move_authenticator,
            descr  => "Change the order of authenticators"
           }).

-rest_api(#{name   => import_users,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/import-users",
            func   => import_users,
            descr  => "Import users"
           }).

-rest_api(#{name   => add_user,
            method => 'POST',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/users",
            func   => add_user,
            descr  => "Add user"
           }).

-rest_api(#{name   => delete_user,
            method => 'DELETE',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/users/:bin:user_id",
            func   => delete_user,
            descr  => "Delete user"
           }).

-rest_api(#{name   => update_user,
            method => 'PUT',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/users/:bin:user_id",
            func   => update_user,
            descr  => "Update user"
           }).

-rest_api(#{name   => lookup_user,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/users/:bin:user_id",
            func   => lookup_user,
            descr  => "Lookup user"
           }).

%% TODO: Support pagination
-rest_api(#{name   => list_users,
            method => 'GET',
            path   => "/authentication/chains/:bin:id/authenticators/:bin:authenticator_name/users",
            func   => list_users,
            descr  => "List all users"
           }).

create_chain(Binding, Params) ->
    do_create_chain(uri_decode(Binding), maps:from_list(Params)).

do_create_chain(_Binding, Chain0) ->
    Config = #{<<"authn">> => #{<<"chains">> => [Chain0#{<<"authenticators">> => []}],
                                <<"bindings">> => []}},
    #{authn := #{chains := [Chain1]}}
                = hocon_schema:check_plain(emqx_authn_schema, Config,
                                           #{atom_key => true, nullable => true}),
    case emqx_authn:create_chain(Chain1) of
        {ok, Chain2} ->
            return({ok, Chain2});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

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

bind(Binding, Params) ->
    do_bind(uri_decode(Binding), lists_to_map(Params)).

do_bind(#{id := ChainID}, #{<<"listeners">> := Listeners}) ->
    % Config = #{<<"authn">> => #{<<"chains">> => [],
    %                             <<"bindings">> => [#{<<"chain">> := ChainID,
    %                                                  <<"listeners">> := Listeners}]}},
    % #{authn := #{bindings := [#{listeners := Listeners}]}}
    %             = hocon_schema:check_plain(emqx_authn_schema, Config,
    %                                        #{atom_key => true, nullable => true}),
    case emqx_authn:bind(ChainID, Listeners) of
        ok ->
            return(ok);
        {error, {alread_bound, Listeners}} ->
            {ok, #{code => <<"ALREADY_EXISTS">>,
                   message => <<"ALREADY_BOUND">>,
                   detail => Listeners}};
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_bind(_, _) ->
    return(serialize_error({missing_parameter, <<"listeners">>})).

unbind(Binding, Params) ->
    do_unbind(uri_decode(Binding), lists_to_map(Params)).

do_unbind(#{id := ChainID}, #{<<"listeners">> := Listeners0}) ->
    case emqx_authn:unbind(ChainID, Listeners0) of
        ok ->
            return(ok);
        {error, {not_found, Listeners1}} ->
            {ok, #{code => <<"NOT_FOUND">>,
                   detail => Listeners1}};
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_unbind(_, _) ->
    return(serialize_error({missing_parameter, <<"listeners">>})).

list_bindings(Binding, Params) ->
    do_list_bindings(uri_decode(Binding), lists_to_map(Params)).

do_list_bindings(#{id := ChainID}, _) ->
    {ok, Binding} = emqx_authn:list_bindings(ChainID),
    return({ok, Binding}).

list_bound_chains(Binding, Params) ->
    do_list_bound_chains(uri_decode(Binding), lists_to_map(Params)).

do_list_bound_chains(#{listener_id := ListenerID}, _) ->
    {ok, Chains} = emqx_authn:list_bound_chains(ListenerID),
    return({ok, Chains}).

create_authenticator(Binding, Params) ->
    do_create_authenticator(uri_decode(Binding), lists_to_map(Params)).

do_create_authenticator(#{id := ChainID}, Authenticator0) ->
    case emqx_authn:lookup_chain(ChainID) of
        {ok, #{type := Type}} ->
            Chain = #{<<"id">> => ChainID,
                      <<"type">> => Type,
                      <<"authenticators">> => [Authenticator0]},
            Config = #{<<"authn">> => #{<<"chains">> => [Chain],
                                        <<"bindings">> => []}},
            #{authn := #{chains := [#{authenticators := [Authenticator1]}]}}
                = hocon_schema:check_plain(emqx_authn_schema, Config,
                                           #{atom_key => true, nullable => true}),
            case emqx_authn:create_authenticator(ChainID, Authenticator1) of
                {ok, Authenticator2} ->
                    return({ok, Authenticator2});
                {error, Reason} ->
                    return(serialize_error(Reason))
            end;
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_authenticator(Binding, Params) ->
    do_delete_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_delete_authenticator(#{id := ChainID,
                          authenticator_name := AuthenticatorName}, _Params) ->
    case emqx_authn:delete_authenticator(ChainID, AuthenticatorName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

%% TODO: Support incremental update
update_authenticator(Binding, Params) ->
    do_update_authenticator(uri_decode(Binding), lists_to_map(Params)).

%% TOOD: PUT method supports creation and update
do_update_authenticator(#{id := ChainID,
                          authenticator_name := AuthenticatorName}, AuthenticatorConfig0) ->
    case emqx_authn:lookup_chain(ChainID) of
        {ok, #{type := ChainType}} ->
            case emqx_authn:lookup_authenticator(ChainID, AuthenticatorName) of
                {ok, #{type := Type}} ->
                    Authenticator = #{<<"name">> => AuthenticatorName,
                                      <<"type">> => Type,
                                      <<"config">> => AuthenticatorConfig0},
                    Chain = #{<<"id">> => ChainID,
                              <<"type">> => ChainType,
                              <<"authenticators">> => [Authenticator]},
                    Config = #{<<"authn">> => #{<<"chains">> => [Chain],
                                                <<"bindings">> => []}},
                    #{
                        authn := #{
                            chains := [#{
                                authenticators := [#{
                                    config := AuthenticatorConfig1
                                }]
                            }]
                        }
                    } = hocon_schema:check_plain(emqx_authn_schema, Config,
                                                 #{atom_key => true, nullable => true}),
                    case emqx_authn:update_authenticator(ChainID, AuthenticatorName, AuthenticatorConfig1) of
                        {ok, NAuthenticator} ->
                            return({ok, NAuthenticator});
                        {error, Reason} ->
                            return(serialize_error(Reason))
                    end;
                {error, Reason} ->
                    return(serialize_error(Reason))
            end;
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_authenticator(Binding, Params) ->
    do_lookup_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_lookup_authenticator(#{id := ChainID,
                    authenticator_name := AuthenticatorName}, _Params) ->
    case emqx_authn:lookup_authenticator(ChainID, AuthenticatorName) of
        {ok, Authenticator} ->
            return({ok, Authenticator});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_authenticators(Binding, Params) ->
    do_list_authenticators(uri_decode(Binding), maps:from_list(Params)).

do_list_authenticators(#{id := ChainID}, _Params) ->
    case emqx_authn:list_authenticators(ChainID) of
        {ok, Authenticators} ->
            return({ok, Authenticators});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

move_authenticator(Binding, Params) ->
    do_move_authenticator(uri_decode(Binding), maps:from_list(Params)).

do_move_authenticator(#{id := ChainID,
                  authenticator_name := AuthenticatorName}, #{<<"position">> := <<"the front">>}) ->
    case emqx_authn:move_authenticator_to_the_front(ChainID, AuthenticatorName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(#{id := ChainID,
                  authenticator_name := AuthenticatorName}, #{<<"position">> := <<"the end">>}) ->
    case emqx_authn:move_authenticator_to_the_end(ChainID, AuthenticatorName) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(#{id := ChainID,
                  authenticator_name := AuthenticatorName}, #{<<"position">> := N}) when is_number(N) ->
    case emqx_authn:move_authenticator_to_the_nth(ChainID, AuthenticatorName, N) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end;
do_move_authenticator(_Binding, _Params) ->
    return(serialize_error({missing_parameter, <<"position">>})).

import_users(Binding, Params) ->
    do_import_users(uri_decode(Binding), maps:from_list(Params)).

do_import_users(#{id := ChainID, authenticator_name := AuthenticatorName},
                #{<<"filename">> := Filename}) ->
    case emqx_authn:import_users(ChainID, AuthenticatorName, Filename) of
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
              authenticator_name := AuthenticatorName}, UserInfo) ->
    case emqx_authn:add_user(ChainID, AuthenticatorName, UserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

delete_user(Binding, Params) ->
    do_delete_user(uri_decode(Binding), maps:from_list(Params)).

do_delete_user(#{id := ChainID,
                 authenticator_name := AuthenticatorName,
                 user_id := UserID}, _Params) ->
    case emqx_authn:delete_user(ChainID, AuthenticatorName, UserID) of
        ok ->
            return(ok);
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

update_user(Binding, Params) ->
    do_update_user(uri_decode(Binding), maps:from_list(Params)).

do_update_user(#{id := ChainID,
                 authenticator_name := AuthenticatorName,
                 user_id := UserID}, NewUserInfo) ->
    case emqx_authn:update_user(ChainID, AuthenticatorName, UserID, NewUserInfo) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

lookup_user(Binding, Params) ->
    do_lookup_user(uri_decode(Binding), maps:from_list(Params)).

do_lookup_user(#{id := ChainID,
                 authenticator_name := AuthenticatorName,
                 user_id := UserID}, _Params) ->
    case emqx_authn:lookup_user(ChainID, AuthenticatorName, UserID) of
        {ok, User} ->
            return({ok, User});
        {error, Reason} ->
            return(serialize_error(Reason))
    end.

list_users(Binding, Params) ->
    do_list_users(uri_decode(Binding), maps:from_list(Params)).

do_list_users(#{id := ChainID,
                authenticator_name := AuthenticatorName}, _Params) ->
    case emqx_authn:list_users(ChainID, AuthenticatorName) of
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
    "Authenticator";
serialize_type(chain) ->
    "Chain";
serialize_type(authenticator_type) ->
    "Authenticator type".

get_missed_params(Actual, Expected) ->
    Keys = lists:foldl(fun(Key, Acc) ->
                           case maps:is_key(Key, Actual) of
                               true -> Acc;
                               false -> [Key | Acc]
                           end
                       end, [], Expected),
    lists:reverse(Keys).
