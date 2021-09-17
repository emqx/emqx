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
        , remove_gateway/1
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

-spec load_gateway(atom_or_bin(), map()) -> ok | {error, any()}.
load_gateway(GwName, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), Conf})).

-spec update_gateway(atom_or_bin(), map()) -> ok | {error, any()}.
update_gateway(GwName, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), Conf})).

-spec remove_gateway(atom_or_bin()) -> ok | {error, any()}.
remove_gateway(GwName) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName)})).

-spec add_listener(atom_or_bin(), listener_ref(), map()) -> ok | {error, any()}.
add_listener(GwName, ListenerRef, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef, Conf})).

-spec update_listener(atom_or_bin(), listener_ref(), map()) -> ok | {error, any()}.
update_listener(GwName, ListenerRef, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef, Conf})).

-spec remove_listener(atom_or_bin(), listener_ref()) -> ok | {error, any()}.
remove_listener(GwName, ListenerRef) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef})).

add_authn(GwName, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), Conf})).
add_authn(GwName, ListenerRef, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef, Conf})).

update_authn(GwName, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), Conf})).
update_authn(GwName, ListenerRef, Conf) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef, Conf})).

remove_authn(GwName) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName)})).
remove_authn(GwName, ListenerRef) ->
    res(emqx:update_config([gateway],
                           {?FUNCTION_NAME, bin(GwName), ListenerRef})).

res({ok, _Result}) -> ok;
res({error, Reason}) -> {error, Reason}.

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
            {error, alredy_exist}
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
pre_config_update({remove_gateway, GwName}, RawConf) ->
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
            {error, alredy_exist}
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
            {error, alredy_exist}
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
                    {error, alredy_exist}
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
    SubConf = maps:get(GwName, NewConfig),
    case maps:get(GwName, OldConfig, undefined) of
        undefined ->
            emqx_gateway:load(GwName, SubConf);
        _ ->
            emqx_gateway:update(GwName, SubConf)
    end.
