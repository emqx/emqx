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

%% @doc The gateway configuration management module
-module(emqx_gateway_conf).

%% Load/Unload
-export([ load/0
        , unload/0
        ]).

%% APIs
-export([ load_gateway/2
        , update_gateway/2
        , unload_gateway/1
        , add_listener/3
        , update_listener/3
        , remove_listener/2
        , add_authn/2
        , add_authn/3
        , update_authn/2
        , update_authn/3
        , remove_authn/1
        , remove_authn/2
        ]).

%% callbacks for emqx_config_handler
-export([ pre_config_update/2
        , post_config_update/4
        ]).

-type atom_or_bin() :: atom() | binary().
-type ok_or_err() :: ok_or_err().
-type listener_ref() :: {ListenerType :: atom_or_bin(),
                         ListenerName :: atom_or_bin()}.

%%--------------------------------------------------------------------
%%  Load/Unload
%%--------------------------------------------------------------------

-spec load() -> ok.
load() ->
    emqx_config_handler:add_handler([gateway], ?MODULE).

-spec unload() -> ok.
unload() ->
    emqx_config_handler:remove_handler([gateway]).

%%--------------------------------------------------------------------
%% APIs

-spec load_gateway(atom_or_bin(), map()) -> ok_or_err().
load_gateway(GwName, Conf) ->
    NConf = case maps:take(<<"listeners">>, Conf) of
                error -> Conf;
                {Ls, Conf1} ->
                    Conf1#{<<"listeners">> => mapping(Ls)}
            end,
    update({?FUNCTION_NAME, bin(GwName), NConf}).

mapping(Ls) when is_list(Ls) ->
    convert_to_map(Ls);
mapping(Ls) when is_map(Ls) ->
    Ls.

convert_to_map(Listeners) when is_list(Listeners) ->
    lists:foldl(fun(Lis, Acc) ->
        {[Type, Name], Lis1} = maps_key_take([<<"type">>, <<"name">>], Lis),
        emqx_map_lib:deep_merge(Acc, #{Type => #{Name => Lis1}})
    end, #{}, Listeners).

maps_key_take(Ks, M) ->
    maps_key_take(Ks, M, []).
maps_key_take([], M, Acc) ->
    {lists:reverse(Acc), M};
maps_key_take([K|Ks], M, Acc) ->
    case maps:take(K, M) of
        error -> throw(bad_key);
        {V, M1} ->
            maps_key_take(Ks, M1, [V|Acc])
    end.

-spec update_gateway(atom_or_bin(), map()) -> ok_or_err().
update_gateway(GwName, Conf0) ->
    Conf = maps:without([listeners, authentication,
                         <<"listeners">>, <<"authentication">>], Conf0),
    update({?FUNCTION_NAME, bin(GwName), Conf}).

-spec unload_gateway(atom_or_bin()) -> ok_or_err().
unload_gateway(GwName) ->
    update({?FUNCTION_NAME, bin(GwName)}).

-spec add_listener(atom_or_bin(), listener_ref(), map()) -> ok_or_err().
add_listener(GwName, ListenerRef, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf}).

-spec update_listener(atom_or_bin(), listener_ref(), map()) -> ok_or_err().
update_listener(GwName, ListenerRef, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf}).

-spec remove_listener(atom_or_bin(), listener_ref()) -> ok_or_err().
remove_listener(GwName, ListenerRef) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef)}).

-spec add_authn(atom_or_bin(), map()) -> ok_or_err().
add_authn(GwName, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), Conf}).

-spec add_authn(atom_or_bin(), listener_ref(), map()) -> ok_or_err().
add_authn(GwName, ListenerRef, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf}).

-spec update_authn(atom_or_bin(), map()) -> ok_or_err().
update_authn(GwName, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), Conf}).

-spec update_authn(atom_or_bin(), listener_ref(), map()) -> ok_or_err().
update_authn(GwName, ListenerRef, Conf) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef), Conf}).

-spec remove_authn(atom_or_bin()) -> ok_or_err().
remove_authn(GwName) ->
    update({?FUNCTION_NAME, bin(GwName)}).

-spec remove_authn(atom_or_bin(), listener_ref()) -> ok_or_err().
remove_authn(GwName, ListenerRef) ->
    update({?FUNCTION_NAME, bin(GwName), bin(ListenerRef)}).

%% @private
update(Req) ->
    res(emqx:update_config([gateway], Req)).

res({ok, _Result}) -> ok;
res({error, {pre_config_update,emqx_gateway_conf,Reason}}) -> {error, Reason};
res({error, Reason}) -> {error, Reason}.

bin({LType, LName}) ->
    {bin(LType), bin(LName)};
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.

%%--------------------------------------------------------------------
%% Config Handler
%%--------------------------------------------------------------------

-spec pre_config_update(emqx_config:update_request(),
                        emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update({load_gateway, GwName, Conf}, RawConf) ->
    case maps:get(GwName, RawConf, undefined) of
        undefined ->
            {ok, emqx_map_lib:deep_merge(RawConf, #{GwName => Conf})};
        _ ->
            {error, already_exist}
    end;
pre_config_update({update_gateway, GwName, Conf}, RawConf) ->
    case maps:get(GwName, RawConf, undefined) of
        undefined ->
            {error, not_found};
        _ ->
            NConf = maps:without([<<"listeners">>,
                                  <<"authentication">>], Conf),
            {ok, emqx_map_lib:deep_merge(RawConf, #{GwName => NConf})}
    end;
pre_config_update({unload_gateway, GwName}, RawConf) ->
    {ok, maps:remove(GwName, RawConf)};

pre_config_update({add_listener, GwName, {LType, LName}, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"listeners">>, LType, LName], RawConf, undefined) of
        undefined ->
            NListener = #{LType => #{LName => Conf}},
            {ok, emqx_map_lib:deep_merge(
                   RawConf,
                   #{GwName => #{<<"listeners">> => NListener}})};
        _ ->
            {error, already_exist}
    end;
pre_config_update({update_listener, GwName, {LType, LName}, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"listeners">>, LType, LName], RawConf, undefined) of
        undefined ->
            {error, not_found};
        _OldConf ->
            NListener = #{LType => #{LName => Conf}},
            {ok, emqx_map_lib:deep_merge(
                   RawConf,
                   #{GwName => #{<<"listeners">> => NListener}})}

    end;
pre_config_update({remove_listener, GwName, {LType, LName}}, RawConf) ->
    {ok, emqx_map_lib:deep_remove(
           [GwName, <<"listeners">>, LType, LName], RawConf)};

pre_config_update({add_authn, GwName, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"authentication">>], RawConf, undefined) of
        undefined ->
            {ok, emqx_map_lib:deep_merge(
                   RawConf,
                   #{GwName => #{<<"authentication">> => Conf}})};
        _ ->
            {error, already_exist}
    end;
pre_config_update({add_authn, GwName, {LType, LName}, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"listeners">>, LType, LName],
           RawConf, undefined) of
        undefined ->
            {error, not_found};
        Listener ->
            case maps:get(<<"authentication">>, Listener, undefined) of
                undefined ->
                    NListener = maps:put(<<"authentication">>, Conf, Listener),
                    NGateway = #{GwName =>
                                 #{<<"listeners">> =>
                                   #{LType => #{LName => NListener}}}},
                    {ok, emqx_map_lib:deep_merge(RawConf, NGateway)};
                _ ->
                    {error, already_exist}
            end
    end;
pre_config_update({update_authn, GwName, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"authentication">>], RawConf, undefined) of
        undefined ->
            {error, not_found};
        _ ->
            {ok, emqx_map_lib:deep_merge(
                   RawConf,
                   #{GwName => #{<<"authentication">> => Conf}})}
    end;
pre_config_update({update_authn, GwName, {LType, LName}, Conf}, RawConf) ->
    case emqx_map_lib:deep_get(
           [GwName, <<"listeners">>, LType, LName],
           RawConf, undefined) of
        undefined ->
            {error, not_found};
        Listener ->
            case maps:get(<<"authentication">>, Listener, undefined) of
                undefined ->
                    {error, not_found};
                Auth ->
                    NListener = maps:put(
                                  <<"authentication">>,
                                  emqx_map_lib:deep_merge(Auth, Conf),
                                  Listener
                                 ),
                    NGateway = #{GwName =>
                                 #{<<"listeners">> =>
                                   #{LType => #{LName => NListener}}}},
                    {ok, emqx_map_lib:deep_merge(RawConf, NGateway)}
            end
    end;
pre_config_update({remove_authn, GwName}, RawConf) ->
    {ok, emqx_map_lib:deep_remove(
           [GwName, <<"authentication">>], RawConf)};
pre_config_update({remove_authn, GwName, {LType, LName}}, RawConf) ->
    Path = [GwName, <<"listeners">>, LType, LName, <<"authentication">>],
    {ok, emqx_map_lib:deep_remove(Path, RawConf)};

pre_config_update(UnknownReq, _RawConf) ->
    logger:error("Unknown configuration update request: ~0p", [UnknownReq]),
    {error, badreq}.

-spec post_config_update(emqx_config:update_request(), emqx_config:config(),
                         emqx_config:config(), emqx_config:app_envs())
    -> ok | {ok, Result::any()} | {error, Reason::term()}.

post_config_update(Req, NewConfig, OldConfig, _AppEnvs) ->
    [_Tag, GwName0|_] = tuple_to_list(Req),
    GwName = binary_to_existing_atom(GwName0),

    case {maps:get(GwName, NewConfig, undefined),
          maps:get(GwName, OldConfig, undefined)} of
        {undefined, undefined} ->
            ok; %% nothing to change
        {undefined, Old} when is_map(Old) ->
            emqx_gateway:unload(GwName);
        {New, undefined} when is_map(New)  ->
            emqx_gateway:load(GwName, New);
        {New, Old} when is_map(New), is_map(Old) ->
            emqx_gateway:update(GwName, New)
    end.
