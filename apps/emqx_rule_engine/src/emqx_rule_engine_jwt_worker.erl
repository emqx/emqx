%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_jwt_worker).

-behaviour(gen_server).

%% API
-export([ start_link/1
        , ensure_jwt/1
        ]).

%% gen_server API
-export([ init/1
        , handle_continue/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , format_status/1
        , format_status/2
        , terminate/2
        ]).

-export([force_refresh/1]).

-include_lib("jose/include/jose_jwk.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx_rule_engine/include/rule_actions.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-type config() :: #{ private_key := binary()
                   , resource_id := resource_id()
                   , expiration := timer:time()
                   , table := ets:table()
                   , iss := binary()
                   , sub := binary()
                   , aud := binary()
                   , kid := binary()
                   , alg := binary()
                   }.
-type jwt() :: binary().
-type state() :: #{ refresh_timer := undefined | timer:tref() | reference()
                  , resource_id := resource_id()
                  , expiration := timer:time()
                  , table := ets:table()
                  , jwt := undefined | jwt()
                    %% only undefined during startup
                  , jwk := undefined | jose_jwk:key()
                  , iss := binary()
                  , sub := binary()
                  , aud := binary()
                  , kid := binary()
                  , alg := binary()
                  }.

-define(refresh_jwt, refresh_jwt).

%%-----------------------------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------------------------

-spec start_link(config()) -> gen_server:start_ret().
start_link(#{ private_key := _
            , expiration := _
            , resource_id := _
            , table := _
            , iss := _
            , sub := _
            , aud := _
            , kid := _
            , alg := _
            } = Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec ensure_jwt(pid()) -> reference().
ensure_jwt(Worker) ->
    Ref = alias([reply]),
    gen_server:cast(Worker, {ensure_jwt, Ref}),
    Ref.

-spec force_refresh(pid()) -> ok.
force_refresh(Worker) ->
    _ = erlang:send(Worker, {timeout, force_refresh, ?refresh_jwt}),
    ok.

%%-----------------------------------------------------------------------------------------
%% gen_server API
%%-----------------------------------------------------------------------------------------

-spec init(config()) -> {ok, state(), {continue, {make_key, binary()}}}
                        | {stop, {error, term()}}.
init(#{private_key := PrivateKeyPEM} = Config) ->
    process_flag(trap_exit, true),
    State0 = maps:without([private_key], Config),
    State = State0#{ jwk => undefined
                   , jwt => undefined
                   , refresh_timer => undefined
                   },
    {ok, State, {continue, {make_key, PrivateKeyPEM}}}.

handle_continue({make_key, PrivateKeyPEM}, State0) ->
    case jose_jwk:from_pem(PrivateKeyPEM) of
        JWK = #jose_jwk{} ->
            State = State0#{jwk := JWK},
            {noreply, State, {continue, create_token}};
        [] ->
            ?tp(rule_engine_jwt_worker_startup_error, #{error => empty_key}),
            {stop, {shutdown, {error, empty_key}}, State0};
        {error, Reason} ->
            Error = {invalid_private_key, Reason},
            ?tp(rule_engine_jwt_worker_startup_error, #{error => Error}),
            {stop, {shutdown, {error, Error}}, State0};
        Error0 ->
            Error = {invalid_private_key, Error0},
            ?tp(rule_engine_jwt_worker_startup_error, #{error => Error}),
            {stop, {shutdown, {error, Error}}, State0}
    end;
handle_continue(create_token, State0) ->
    State = generate_and_store_jwt(State0),
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast({ensure_jwt, From}, State0 = #{jwt := JWT}) ->
    State =
        case JWT of
            undefined ->
                generate_and_store_jwt(State0);
            _ ->
                State0
        end,
    From ! {From, token_created},
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, ?refresh_jwt}, State0) ->
    State = generate_and_store_jwt(State0),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

format_status(State) ->
    censor_secrets(State).

format_status(_Opt, [_PDict, State0]) ->
    State = censor_secrets(State0),
    [{data, [{"State", State}]}].

terminate(_Reason, State) ->
    #{resource_id := ResourceId, table := TId} = State,
    emqx_rule_engine_jwt:delete_jwt(TId, ResourceId),
    ok.

%%-----------------------------------------------------------------------------------------
%% Helper fns
%%-----------------------------------------------------------------------------------------

-spec do_generate_jwt(state()) -> jwt().
do_generate_jwt(#{ expiration := ExpirationMS
                 , iss := Iss
                 , sub := Sub
                 , aud := Aud
                 , kid := KId
                 , alg := Alg
                 , jwk := JWK
                 } = _State) ->
    Headers = #{ <<"alg">> => Alg
               , <<"kid">> => KId
               },
    Now = erlang:system_time(seconds),
    ExpirationS = erlang:convert_time_unit(ExpirationMS, millisecond, second),
    Claims = #{ <<"iss">> => Iss
              , <<"sub">> => Sub
              , <<"aud">> => Aud
              , <<"iat">> => Now
              , <<"exp">> => Now + ExpirationS
              },
    JWT0 = jose_jwt:sign(JWK, Headers, Claims),
    {_, JWT} = jose_jws:compact(JWT0),
    JWT.

-spec generate_and_store_jwt(state()) -> state().
generate_and_store_jwt(State0) ->
    JWT = do_generate_jwt(State0),
    store_jwt(State0, JWT),
    ?tp(rule_engine_jwt_worker_refresh, #{jwt => JWT}),
    State1 = State0#{jwt := JWT},
    ensure_timer(State1).

-spec store_jwt(state(), jwt()) -> ok.
store_jwt(#{resource_id := ResourceId, table := TId}, JWT) ->
    true = ets:insert(TId, {{ResourceId, jwt}, JWT}),
    ?tp(rule_engine_jwt_worker_token_stored, #{resource_id => ResourceId}),
    ok.

-spec ensure_timer(state()) -> state().
ensure_timer(State = #{ refresh_timer := OldTimer
                      , expiration := ExpirationMS0
                      }) ->
    ok = cancel_timer(OldTimer),
    ExpirationMS = max(5_000, ExpirationMS0 - 60_000),
    TRef = erlang:start_timer(ExpirationMS, self(), ?refresh_jwt),
    State#{refresh_timer => TRef}.

-spec censor_secrets(state()) -> map().
censor_secrets(State) ->
    maps:map(
     fun(Key, _Value) when Key =:= jwt;
                           Key =:= jwk ->
             "******";
        (_Key, Value) ->
             Value
     end,
     State).

-spec cancel_timer(undefined | timer:tref() | reference()) -> ok.
cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.
