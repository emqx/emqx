%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The gateway configuration management module
-module(emqx_gateway_conf).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

%% Load/Unload
-export([
    load/0,
    unload/0
]).

%% APIs
-export([
    gateway/1,
    load_gateway/2,
    update_gateway/2,
    unload_gateway/1
]).

-export([
    listeners/1,
    listener/1,
    add_listener/3,
    update_listener/3,
    remove_listener/2
]).

-export([
    add_authn/2,
    add_authn/3,
    update_authn/2,
    update_authn/3,
    remove_authn/1,
    remove_authn/2
]).

-export([get_bind/1]).

%% internal exports
-export([
    unconvert_listeners/1,
    convert_listeners/2
]).

%% callbacks for emqx_config_handler
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% Data backup
-export([
    import_config/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authn_chains.hrl").
-define(AUTHN_BIN, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY).

-type atom_or_bin() :: atom() | binary().
-type ok_or_err() :: ok | {error, term()}.
-type map_or_err() :: {ok, map()} | {error, term()}.
-type listener_ref() :: {ListenerType :: atom_or_bin(), ListenerName :: atom_or_bin()}.

-define(IS_SSL(T), (T == <<"ssl_options">> orelse T == <<"dtls_options">>)).
-define(IGNORE_KEYS, [<<"listeners">>, ?AUTHN_BIN]).

%%----------------------------------------------------------------------------------------
%%  Load/Unload
%%----------------------------------------------------------------------------------------
-define(GATEWAY, [gateway]).

-spec load() -> ok.
load() ->
    emqx_conf:add_handler(?GATEWAY, ?MODULE).

-spec unload() -> ok.
unload() ->
    emqx_conf:remove_handler(?GATEWAY).

%%----------------------------------------------------------------------------------------
%% APIs

-spec load_gateway(atom_or_bin(), map()) -> map_or_err().
load_gateway(GwName, Conf) ->
    NConf =
        case maps:take(<<"listeners">>, Conf) of
            error -> Conf;
            {Ls, Conf1} -> Conf1#{<<"listeners">> => unconvert_listeners(Ls)}
        end,
    ret_gw(GwName, update({?FUNCTION_NAME, bin(GwName), NConf})).

%% @doc convert listener array to map
unconvert_listeners(Ls) when is_list(Ls) ->
    lists:foldl(
        fun(Lis, Acc) ->
            {[Type, Name], Lis1} = maps_key_take([<<"type">>, <<"name">>], Lis),
            _ = validate_listener_name(Name),
            NLis1 = maps:without([<<"id">>, <<"running">>], Lis1),
            emqx_utils_maps:deep_merge(Acc, #{Type => #{Name => NLis1}})
        end,
        #{},
        Ls
    ).

maps_key_take(Ks, M) ->
    maps_key_take(Ks, M, []).
maps_key_take([], M, Acc) ->
    {lists:reverse(Acc), M};
maps_key_take([K | Ks], M, Acc) ->
    case maps:take(K, M) of
        error -> error(bad_key);
        {V, M1} -> maps_key_take(Ks, M1, [V | Acc])
    end.

validate_listener_name(Name) ->
    try
        {match, _} = re:run(Name, "^[a-zA-Z][0-9a-zA-Z_-]*$"),
        ok
    catch
        _:_ ->
            error(
                {badconf, #{
                    key => name,
                    value => Name,
                    reason => bad_listener_name
                }}
            )
    end.

-spec update_gateway(atom_or_bin(), map()) -> map_or_err().
update_gateway(GwName, Conf0) ->
    Exclude0 = [listeners, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM],
    Exclude1 = [atom_to_binary(K, utf8) || K <- Exclude0],
    Conf = maps:without(Exclude0 ++ Exclude1, Conf0),
    ret_gw(GwName, update({?FUNCTION_NAME, bin(GwName), Conf})).

%% FIXME: delete cert files ??

-spec unload_gateway(atom_or_bin()) -> ok_or_err().
unload_gateway(GwName) ->
    ret_ok_err(update({?FUNCTION_NAME, bin(GwName)})).

%% @doc Get the gateway configurations.
%% Missing fields are filled with default values. This function is typically
%% used to show the user what configuration value is currently in effect.
-spec gateway(atom_or_bin()) -> map().
gateway(GwName0) ->
    GwName = bin(GwName0),
    Path = [<<"gateway">>, GwName],
    RawConf = emqx_config:fill_defaults(
        emqx_config:get_root_raw(Path)
    ),
    Confs = emqx_utils_maps:jsonable_map(
        emqx_utils_maps:deep_get(Path, RawConf)
    ),
    LsConf = maps:get(<<"listeners">>, Confs, #{}),
    Confs#{<<"listeners">> => convert_listeners(GwName, LsConf)}.

%% @doc convert listeners map to array
convert_listeners(GwName, Ls) when is_map(Ls) ->
    lists:append([
        do_convert_listener(GwName, Type, maps:to_list(Conf))
     || {Type, Conf} <- maps:to_list(Ls)
    ]).

do_convert_listener(GwName, LType, Conf) ->
    [
        do_convert_listener2(GwName, LType, LName, LConf)
     || {LName, LConf} <- Conf, is_map(LConf)
    ].

do_convert_listener2(GwName, LType, LName, LConf) ->
    ListenerId = emqx_gateway_utils:listener_id(GwName, LType, LName),
    LConf#{
        id => ListenerId,
        type => LType,
        name => LName
    }.

get_bind(#{bind := Bind}) ->
    emqx_gateway_utils:parse_listenon(Bind);
get_bind(#{<<"bind">> := Bind}) ->
    emqx_gateway_utils:parse_listenon(Bind).

-spec listeners(atom_or_bin()) -> [map()].
listeners(GwName0) ->
    GwName = bin(GwName0),
    RawConf = emqx_config:fill_defaults(
        emqx_config:get_root_raw([<<"gateway">>])
    ),
    Listeners = emqx_utils_maps:jsonable_map(
        emqx_utils_maps:deep_get(
            [<<"gateway">>, GwName, <<"listeners">>], RawConf
        )
    ),
    convert_listeners(GwName, Listeners).

-spec listener(binary()) -> {ok, map()} | {error, not_found} | {error, any()}.
listener(ListenerId) ->
    {GwName, Type, LName} = emqx_gateway_utils:parse_listener_id(ListenerId),
    RootConf = emqx_config:fill_defaults(
        emqx_config:get_root_raw([<<"gateway">>])
    ),
    try
        Path = [<<"gateway">>, GwName, <<"listeners">>, Type, LName],
        LConf = emqx_utils_maps:deep_get(Path, RootConf),
        Running = emqx_gateway_utils:is_running(
            binary_to_existing_atom(ListenerId), LConf
        ),
        {ok,
            emqx_utils_maps:jsonable_map(
                LConf#{
                    id => ListenerId,
                    type => Type,
                    name => LName,
                    running => Running
                }
            )}
    catch
        error:{config_not_found, _} ->
            {error, not_found};
        _Class:Reason ->
            {error, Reason}
    end.

-spec add_listener(atom_or_bin(), listener_ref(), map()) -> map_or_err().
add_listener(GwName, ListenerRef, Conf) ->
    ret_listener_or_err(
        GwName,
        ListenerRef,
        update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf})
    ).

-spec update_listener(atom_or_bin(), listener_ref(), map()) -> map_or_err().
update_listener(GwName, ListenerRef, Conf) ->
    ret_listener_or_err(
        GwName,
        ListenerRef,
        update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf})
    ).

-spec remove_listener(atom_or_bin(), listener_ref()) -> ok_or_err().
remove_listener(GwName, ListenerRef) ->
    ret_ok_err(update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef)})).

-spec add_authn(atom_or_bin(), map()) -> map_or_err().
add_authn(GwName, Conf) ->
    ret_authn(GwName, update({?FUNCTION_NAME, bin(GwName), Conf})).

-spec add_authn(atom_or_bin(), listener_ref(), map()) -> map_or_err().
add_authn(GwName, ListenerRef, Conf) ->
    ret_authn(
        GwName,
        ListenerRef,
        update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf})
    ).

-spec update_authn(atom_or_bin(), map()) -> map_or_err().
update_authn(GwName, Conf) ->
    ret_authn(GwName, update({?FUNCTION_NAME, bin(GwName), Conf})).

-spec update_authn(atom_or_bin(), listener_ref(), map()) -> map_or_err().
update_authn(GwName, ListenerRef, Conf) ->
    ret_authn(
        GwName,
        ListenerRef,
        update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf})
    ).

-spec remove_authn(atom_or_bin()) -> ok_or_err().
remove_authn(GwName) ->
    ret_ok_err(update({?FUNCTION_NAME, bin(GwName)})).

-spec remove_authn(atom_or_bin(), listener_ref()) -> ok_or_err().
remove_authn(GwName, ListenerRef) ->
    ret_ok_err(update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef)})).

%% @private
update(Req) ->
    res(emqx_conf:update([gateway], Req, #{override_to => cluster})).

res({ok, Result}) -> {ok, Result};
res({error, {pre_config_update, ?MODULE, Reason}}) -> {error, Reason};
res({error, {post_config_update, ?MODULE, Reason}}) -> {error, Reason};
res({error, Reason}) -> {error, Reason}.

bin({LType, LName}) ->
    {bin(LType), bin(LName)};
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.

ret_ok_err({ok, _}) -> ok;
ret_ok_err(Err) -> Err.

ret_gw(GwName, {ok, #{raw_config := GwConf}}) ->
    GwConf1 = emqx_utils_maps:deep_get([bin(GwName)], GwConf),
    LsConf = emqx_utils_maps:deep_get(
        [bin(GwName), <<"listeners">>],
        GwConf,
        #{}
    ),
    NLsConf =
        lists:foldl(
            fun({LType, SubConf}, Acc) ->
                NLConfs =
                    lists:map(
                        fun({LName, LConf}) ->
                            do_convert_listener2(GwName, LType, LName, LConf)
                        end,
                        maps:to_list(SubConf)
                    ),
                [NLConfs | Acc]
            end,
            [],
            maps:to_list(LsConf)
        ),
    {ok, maps:merge(GwConf1, #{<<"listeners">> => lists:append(NLsConf)})};
ret_gw(_GwName, Err) ->
    Err.

ret_authn(GwName, {ok, #{raw_config := GwConf}}) ->
    Authn = emqx_utils_maps:deep_get(
        [bin(GwName), <<"authentication">>],
        GwConf
    ),
    {ok, Authn};
ret_authn(_GwName, Err) ->
    Err.

ret_authn(GwName, {LType, LName}, {ok, #{raw_config := GwConf}}) ->
    Authn = emqx_utils_maps:deep_get(
        [
            bin(GwName),
            <<"listeners">>,
            bin(LType),
            bin(LName),
            <<"authentication">>
        ],
        GwConf
    ),
    {ok, Authn};
ret_authn(_, _, Err) ->
    Err.

ret_listener_or_err(GwName, {LType, LName}, {ok, #{raw_config := GwConf}}) ->
    LConf = emqx_utils_maps:deep_get(
        [bin(GwName), <<"listeners">>, bin(LType), bin(LName)],
        GwConf
    ),
    {ok, do_convert_listener2(GwName, LType, LName, LConf)};
ret_listener_or_err(_, _, Err) ->
    Err.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

import_config(RawConf) ->
    GatewayConf = maps:get(<<"gateway">>, RawConf, #{}),
    OldGatewayConf = emqx:get_raw_config([<<"gateway">>], #{}),
    MergedConf = maps:merge(OldGatewayConf, GatewayConf),
    case emqx_conf:update([gateway], MergedConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawConf, OldGatewayConf)),
            ChangedPaths = [[gateway, GwName] || GwName <- maps:keys(Changed)],
            {ok, #{root_key => gateway, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => gateway, reason => Error}}
    end.

%%----------------------------------------------------------------------------------------
%% Config Handler
%%----------------------------------------------------------------------------------------

-spec pre_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:raw_config()
) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update(?GATEWAY, {load_gateway, GwName, Conf}, RawConf) ->
    case maps:get(GwName, RawConf, undefined) of
        undefined ->
            NConf = tune_gw_certs(fun convert_certs/2, GwName, Conf),
            {ok, emqx_utils_maps:deep_put([GwName], RawConf, NConf)};
        _ ->
            badres_gateway(already_exist, GwName)
    end;
pre_config_update(?GATEWAY, {update_gateway, GwName, Conf}, RawConf) ->
    case maps:get(GwName, RawConf, undefined) of
        undefined ->
            badres_gateway(not_found, GwName);
        GwRawConf ->
            Conf1 = maps:without(?IGNORE_KEYS, Conf),
            NConf = tune_gw_certs(fun convert_certs/2, GwName, Conf1),
            NConf1 = maps:merge(GwRawConf, NConf),
            {ok, emqx_utils_maps:deep_put([GwName], RawConf, NConf1)}
    end;
pre_config_update(?GATEWAY, {unload_gateway, GwName}, RawConf) ->
    {ok, maps:remove(GwName, RawConf)};
pre_config_update(?GATEWAY, {add_listener, GwName, {LType, LName}, Conf}, RawConf) ->
    case get_listener(GwName, LType, LName, RawConf) of
        undefined ->
            NConf = convert_certs(certs_dir(GwName), Conf),
            NListener = #{LType => #{LName => NConf}},
            {ok,
                emqx_utils_maps:deep_merge(
                    RawConf,
                    #{GwName => #{<<"listeners">> => NListener}}
                )};
        _ ->
            badres_listener(already_exist, GwName, LType, LName)
    end;
pre_config_update(?GATEWAY, {update_listener, GwName, {LType, LName}, Conf}, RawConf) ->
    case get_listener(GwName, LType, LName, RawConf) of
        undefined ->
            badres_listener(not_found, GwName, LType, LName);
        _OldConf ->
            NConf = convert_certs(certs_dir(GwName), Conf),
            NRawConf = emqx_utils_maps:deep_put(
                [GwName, <<"listeners">>, LType, LName],
                RawConf,
                NConf
            ),
            {ok, NRawConf}
    end;
pre_config_update(?GATEWAY, {remove_listener, GwName, {LType, LName}}, RawConf) ->
    case get_listener(GwName, LType, LName, RawConf) of
        undefined ->
            {ok, RawConf};
        _OldConf ->
            Path = [GwName, <<"listeners">>, LType, LName],
            {ok, emqx_utils_maps:deep_remove(Path, RawConf)}
    end;
pre_config_update(?GATEWAY, {add_authn, GwName, Conf}, RawConf) ->
    case get_authn(GwName, RawConf) of
        undefined ->
            CertsDir = authn_certs_dir(GwName, Conf),
            Conf1 = emqx_authn_config:convert_certs(CertsDir, Conf),
            {ok,
                emqx_utils_maps:deep_merge(
                    RawConf,
                    #{GwName => #{?AUTHN_BIN => Conf1}}
                )};
        _ ->
            badres_authn(already_exist, GwName)
    end;
pre_config_update(?GATEWAY, {add_authn, GwName, {LType, LName}, Conf}, RawConf) ->
    case get_listener(GwName, LType, LName, RawConf) of
        undefined ->
            badres_listener(not_found, GwName, LType, LName);
        Listener ->
            case maps:get(?AUTHN_BIN, Listener, undefined) of
                undefined ->
                    CertsDir = authn_certs_dir(GwName, LType, LName, Conf),
                    Conf1 = emqx_authn_config:convert_certs(CertsDir, Conf),
                    NListener = maps:put(?AUTHN_BIN, Conf1, Listener),
                    NGateway = #{
                        GwName =>
                            #{
                                <<"listeners">> =>
                                    #{LType => #{LName => NListener}}
                            }
                    },
                    {ok, emqx_utils_maps:deep_merge(RawConf, NGateway)};
                _ ->
                    badres_listener_authn(already_exist, GwName, LType, LName)
            end
    end;
pre_config_update(?GATEWAY, {update_authn, GwName, Conf}, RawConf) ->
    Path = [GwName, ?AUTHN_BIN],
    case get_authn(GwName, RawConf) of
        undefined ->
            badres_authn(not_found, GwName);
        _OldConf ->
            CertsDir = authn_certs_dir(GwName, Conf),
            Conf1 = emqx_authn_config:convert_certs(CertsDir, Conf),
            {ok, emqx_utils_maps:deep_put(Path, RawConf, Conf1)}
    end;
pre_config_update(?GATEWAY, {update_authn, GwName, {LType, LName}, Conf}, RawConf) ->
    Path = [GwName, <<"listeners">>, LType, LName],
    case get_listener(GwName, LType, LName, RawConf) of
        undefined ->
            badres_listener(not_found, GwName, LType, LName);
        Listener ->
            case maps:get(?AUTHN_BIN, Listener, undefined) of
                undefined ->
                    badres_listener_authn(not_found, GwName, LType, LName);
                OldAuthnConf ->
                    CertsDir = authn_certs_dir(GwName, LType, LName, OldAuthnConf),
                    Conf1 = emqx_authn_config:convert_certs(CertsDir, Conf),
                    NListener = maps:put(
                        ?AUTHN_BIN,
                        Conf1,
                        Listener
                    ),
                    {ok, emqx_utils_maps:deep_put(Path, RawConf, NListener)}
            end
    end;
pre_config_update(?GATEWAY, {remove_authn, GwName}, RawConf) ->
    Path = [GwName, ?AUTHN_BIN],
    {ok, emqx_utils_maps:deep_remove(Path, RawConf)};
pre_config_update(?GATEWAY, {remove_authn, GwName, {LType, LName}}, RawConf) ->
    Path = [GwName, <<"listeners">>, LType, LName, ?AUTHN_BIN],
    {ok, emqx_utils_maps:deep_remove(Path, RawConf)};
pre_config_update(?GATEWAY, NewRawConf0 = #{}, OldRawConf = #{}) ->
    %% FIXME don't support gateway's listener's authn update.
    %% load all authentications
    NewRawConf1 = pre_load_authentications(NewRawConf0, OldRawConf),
    %% load all listeners
    NewRawConf2 = pre_load_listeners(NewRawConf1, OldRawConf),
    %% load all gateway
    NewRawConf3 = pre_load_gateways(NewRawConf2, OldRawConf),
    {ok, NewRawConf3};
pre_config_update(Path, UnknownReq, _RawConf) ->
    ?SLOG(error, #{
        msg => "unknown_gateway_update_request",
        request => UnknownReq,
        path => Path
    }),
    {error, badreq}.

pre_load_gateways(NewConf, OldConf) ->
    %% unload old gateways
    maps:foreach(
        fun(GwName, _OldGwConf) ->
            case maps:find(GwName, NewConf) of
                error -> pre_config_update(?GATEWAY, {unload_gateway, GwName}, OldConf);
                _ -> ok
            end
        end,
        OldConf
    ),
    %% load/update gateways
    maps:map(
        fun(GwName, NewGwConf) ->
            case maps:find(GwName, OldConf) of
                {ok, NewGwConf} ->
                    NewGwConf;
                {ok, _OldGwConf} ->
                    {ok, #{GwName := NewGwConf1}} = pre_config_update(
                        ?GATEWAY, {update_gateway, GwName, NewGwConf}, OldConf
                    ),
                    %% update gateway should pass through ignore keys(listener/authn)
                    PassThroughConf = maps:with(?IGNORE_KEYS, NewGwConf),
                    NewGwConf2 = maps:without(?IGNORE_KEYS, NewGwConf1),
                    maps:merge(NewGwConf2, PassThroughConf);
                error ->
                    {ok, #{GwName := NewGwConf1}} = pre_config_update(
                        ?GATEWAY, {load_gateway, GwName, NewGwConf}, OldConf
                    ),
                    NewGwConf1
            end
        end,
        NewConf
    ).

pre_load_listeners(NewConf, OldConf) ->
    %% remove listeners
    maps:foreach(
        fun(GwName, GwConf) ->
            Listeners = maps:get(<<"listeners">>, GwConf, #{}),
            remove_listeners(GwName, NewConf, OldConf, Listeners)
        end,
        OldConf
    ),
    %% add/update listeners
    maps:map(
        fun(GwName, GwConf) ->
            Listeners = maps:get(<<"listeners">>, GwConf, #{}),
            NewListeners = create_or_update_listeners(GwName, OldConf, Listeners),
            maps:put(<<"listeners">>, NewListeners, GwConf)
        end,
        NewConf
    ).

create_or_update_listeners(GwName, OldConf, Listeners) ->
    maps:map(
        fun(LType, LConf) ->
            maps:map(
                fun(LName, LConf1) ->
                    NConf =
                        case get_listener(GwName, LType, LName, OldConf) of
                            undefined ->
                                {ok, NConf0} =
                                    pre_config_update(
                                        ?GATEWAY,
                                        {add_listener, GwName, {LType, LName}, LConf1},
                                        OldConf
                                    ),
                                NConf0;
                            _ ->
                                {ok, NConf0} =
                                    pre_config_update(
                                        ?GATEWAY,
                                        {update_listener, GwName, {LType, LName}, LConf1},
                                        OldConf
                                    ),
                                NConf0
                        end,
                    get_listener(GwName, LType, LName, NConf)
                end,
                LConf
            )
        end,
        Listeners
    ).

remove_listeners(GwName, NewConf, OldConf, Listeners) ->
    maps:foreach(
        fun(LType, LConf) ->
            maps:foreach(
                fun(LName, _LConf1) ->
                    case get_listener(GwName, LType, LName, NewConf) of
                        undefined ->
                            pre_config_update(
                                ?GATEWAY, {remove_listener, GwName, {LType, LName}}, OldConf
                            );
                        _ ->
                            ok
                    end
                end,
                LConf
            )
        end,
        Listeners
    ).

get_listener(GwName, LType, LName, NewConf) ->
    emqx_utils_maps:deep_get(
        [GwName, <<"listeners">>, LType, LName], NewConf, undefined
    ).

get_authn(GwName, Conf) ->
    emqx_utils_maps:deep_get([GwName, ?AUTHN_BIN], Conf, undefined).

pre_load_authentications(NewConf, OldConf) ->
    %% remove authentications when not in new config
    maps:foreach(
        fun(GwName, OldGwConf) ->
            case
                maps:get(?AUTHN_BIN, OldGwConf, undefined) =/= undefined andalso
                    get_authn(GwName, NewConf) =:= undefined
            of
                true ->
                    pre_config_update(?GATEWAY, {remove_authn, GwName}, OldConf);
                false ->
                    ok
            end
        end,
        OldConf
    ),
    %% add/update authentications
    maps:map(
        fun(GwName, NewGwConf) ->
            case get_authn(GwName, OldConf) of
                undefined ->
                    case maps:get(?AUTHN_BIN, NewGwConf, undefined) of
                        undefined ->
                            NewGwConf;
                        AuthN ->
                            {ok, #{GwName := #{?AUTHN_BIN := NAuthN}}} =
                                pre_config_update(?GATEWAY, {add_authn, GwName, AuthN}, OldConf),
                            maps:put(?AUTHN_BIN, NAuthN, NewGwConf)
                    end;
                OldAuthN ->
                    case maps:get(?AUTHN_BIN, NewGwConf, undefined) of
                        undefined ->
                            NewGwConf;
                        OldAuthN ->
                            NewGwConf;
                        NewAuthN ->
                            {ok, #{GwName := #{?AUTHN_BIN := NAuthN}}} =
                                pre_config_update(
                                    ?GATEWAY, {update_authn, GwName, NewAuthN}, OldConf
                                ),
                            maps:put(?AUTHN_BIN, NAuthN, NewGwConf)
                    end
            end
        end,
        NewConf
    ).

badres_gateway(not_found, GwName) ->
    {error,
        {badres, #{
            resource => gateway,
            gateway => GwName,
            reason => not_found
        }}};
badres_gateway(already_exist, GwName) ->
    {error,
        {badres, #{
            resource => gateway,
            gateway => GwName,
            reason => already_exist
        }}}.

badres_listener(not_found, GwName, LType, LName) ->
    {error,
        {badres, #{
            resource => listener,
            gateway => GwName,
            listener => {GwName, LType, LName},
            reason => not_found
        }}};
badres_listener(already_exist, GwName, LType, LName) ->
    {error,
        {badres, #{
            resource => listener,
            gateway => GwName,
            listener => {GwName, LType, LName},
            reason => already_exist
        }}}.

badres_authn(not_found, GwName) ->
    {error,
        {badres, #{
            resource => authn,
            gateway => GwName,
            reason => not_found
        }}};
badres_authn(already_exist, GwName) ->
    {error,
        {badres, #{
            resource => authn,
            gateway => GwName,
            reason => already_exist
        }}}.

badres_listener_authn(not_found, GwName, LType, LName) ->
    {error,
        {badres, #{
            resource => listener_authn,
            gateway => GwName,
            listener => {GwName, LType, LName},
            reason => not_found
        }}};
badres_listener_authn(already_exist, GwName, LType, LName) ->
    {error,
        {badres, #{
            resource => listener_authn,
            gateway => GwName,
            listener => {GwName, LType, LName},
            reason => already_exist
        }}}.

-spec post_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

post_config_update(?GATEWAY, Req, NewConfig, OldConfig, _AppEnvs) when is_tuple(Req) ->
    [_Tag, GwName0 | _] = tuple_to_list(Req),
    GwName = binary_to_existing_atom(GwName0),

    case {maps:get(GwName, NewConfig, undefined), maps:get(GwName, OldConfig, undefined)} of
        {undefined, undefined} ->
            %% nothing to change
            ok;
        {undefined, Old} when is_map(Old) ->
            emqx_gateway:unload(GwName);
        {New, undefined} when is_map(New) ->
            emqx_gateway:load(GwName, New);
        {New, Old} when is_map(New), is_map(Old) ->
            emqx_gateway:update(GwName, New)
    end;
post_config_update(?GATEWAY, _Req = #{}, NewConfig, OldConfig, _AppEnvs) ->
    %% unload gateways
    maps:foreach(
        fun(GwName, _OldGwConf) ->
            case maps:get(GwName, NewConfig, undefined) of
                undefined ->
                    emqx_gateway:unload(GwName);
                _ ->
                    ok
            end
        end,
        OldConfig
    ),
    %% load/update gateways
    maps:foreach(
        fun(GwName, NewGwConf) ->
            case maps:get(GwName, OldConfig, undefined) of
                undefined ->
                    emqx_gateway:load(GwName, NewGwConf);
                _ ->
                    emqx_gateway:update(GwName, NewGwConf)
            end
        end,
        NewConfig
    ),
    ok.

%%----------------------------------------------------------------------------------------
%% Internal funcs
%%----------------------------------------------------------------------------------------

tune_gw_certs(Fun, GwName, Conf) ->
    apply_to_gateway_basic_confs(
        Fun,
        GwName,
        apply_to_listeners(Fun, GwName, Conf)
    ).

apply_to_listeners(Fun, GwName, Conf) ->
    SubDir = certs_dir(GwName),
    case maps:get(<<"listeners">>, Conf, undefined) of
        undefined ->
            Conf;
        Liss ->
            maps:put(
                <<"listeners">>,
                maps:map(
                    fun(_, Lis) ->
                        maps:map(
                            fun(_, LisConf) ->
                                erlang:apply(Fun, [SubDir, LisConf])
                            end,
                            Lis
                        )
                    end,
                    Liss
                ),
                Conf
            )
    end.

apply_to_gateway_basic_confs(Fun, <<"exproto">>, Conf) ->
    SvrDir = filename:join(["exproto", "server"]),
    HdrDir = filename:join(["exproto", "handler"]),
    Conf1 =
        case maps:get(<<"server">>, Conf, undefined) of
            undefined ->
                Conf;
            ServerConf ->
                maps:put(<<"server">>, erlang:apply(Fun, [SvrDir, ServerConf]), Conf)
        end,
    case maps:get(<<"handler">>, Conf1, undefined) of
        undefined -> Conf1;
        HandlerConf -> maps:put(<<"handler">>, erlang:apply(Fun, [HdrDir, HandlerConf]), Conf1)
    end;
apply_to_gateway_basic_confs(_Fun, _GwName, Conf) ->
    Conf.

certs_dir(GwName) when is_binary(GwName) ->
    GwName.

authn_certs_dir(GwName, ListenerType, ListenerName, AuthnConf) ->
    ChainName = emqx_gateway_utils:listener_chain(GwName, ListenerType, ListenerName),
    emqx_authn_config:certs_dir(ChainName, AuthnConf).

authn_certs_dir(GwName, AuthnConf) when is_binary(GwName) ->
    authn_certs_dir(binary_to_existing_atom(GwName), AuthnConf);
authn_certs_dir(GwName, AuthnConf) ->
    emqx_authn_config:certs_dir(
        emqx_gateway_utils:global_chain(GwName),
        AuthnConf
    ).

convert_certs(SubDir, Conf) ->
    convert_certs(<<"dtls_options">>, SubDir, convert_certs(<<"ssl_options">>, SubDir, Conf)).

convert_certs(Type, SubDir, Conf) ->
    SSL = maps:get(Type, Conf, undefined),
    case is_map(SSL) andalso emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(SubDir, SSL) of
        false ->
            Conf;
        {ok, NSSL = #{}} ->
            Conf#{Type => NSSL};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config", reason => Reason}),
            throw({bad_ssl_config, Reason})
    end.
