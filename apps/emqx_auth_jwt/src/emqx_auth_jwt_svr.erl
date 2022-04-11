%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_jwt_svr).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("jose/include/jose_jwk.hrl").

-logger_header("[JWT-SVR]").

%% APIs
-export([start_link/1]).

-export([verify/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-type options() :: [option()].
-type option() :: {secret, list()}
                | {pubkey, list()}
                | {jwks_addr, list()}
                | {interval, pos_integer()}.

-define(INTERVAL, 300000).
-define(TAB, ?MODULE).

-record(state, {addr, tref, intv}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(options()) -> gen_server:start_ret().
start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

-spec verify(binary())
    -> {error, term()}
     | {ok, Payload :: map()}.
verify(JwsCompacted) when is_binary(JwsCompacted) ->
    case catch jose_jws:peek(JwsCompacted) of
        {'EXIT', _} -> {error, not_token};
        _ -> do_verify(JwsCompacted)
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Options]) ->
    ok = jose:json_module(jiffy),
    _ = ets:new(?TAB, [set, protected, named_table]),
    {Static, Remote} = do_init_jwks(Options),
    true = ets:insert(?TAB, [{static, Static}, {remote, Remote}]),
    Intv = proplists:get_value(interval, Options, ?INTERVAL),
    {ok, reset_timer(
           #state{
              addr = proplists:get_value(jwks_addr, Options),
              intv = Intv})}.

%% @private
do_init_jwks(Options) ->
    K2J = fun(K, F) ->
              case proplists:get_value(K, Options) of
                  undefined -> undefined;
                  V ->
                     try F(V) of
                         {error, Reason} ->
                             ?LOG(warning, "Build ~p JWK ~p failed: {error, ~p}~n",
                                  [K, V, Reason]),
                             undefined;
                         J -> J
                     catch T:R ->
                         ?LOG(warning, "Build ~p JWK ~p failed: {~p, ~p}~n",
                              [K, V, T, R]),
                         undefined
                     end
              end
          end,
    OctJwk = K2J(secret, fun(V) ->
                             jose_jwk:from_oct(list_to_binary(V))
                         end),
    PemJwk = K2J(pubkey, fun jose_jwk:from_pem_file/1),
    Remote = K2J(jwks_addr, fun request_jwks/1),
    {[J ||J <- [OctJwk, PemJwk], J /= undefined], Remote}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, refresh}, State = #state{addr = Addr}) ->
    NState = try
                 true = ets:insert(?TAB, {remote, request_jwks(Addr)})
             catch _:_ ->
                 State
             end,
    {noreply, reset_timer(NState)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

keys(Type) ->
    case ets:lookup(?TAB, Type) of
        [{_, Keys}] -> Keys;
        [] -> []
    end.

request_jwks(Addr) ->
    case httpc:request(get, {Addr, []}, [], [{body_format, binary}]) of
        {error, Reason} ->
            error(Reason);
        {ok, {_Code, _Headers, Body}} ->
            try
                JwkSet = jose_jwk:from(emqx_json:decode(Body, [return_maps])),
                {_, Jwks} = JwkSet#jose_jwk.keys, Jwks
            catch _:_ ->
                ?LOG(error, "Invalid jwks server response: ~p~n", [Body]),
                error(badarg)
            end
    end.

reset_timer(State = #state{addr = undefined}) ->
    State;
reset_timer(State = #state{intv = Intv}) ->
    State#state{tref = erlang:start_timer(Intv, self(), refresh)}.

cancel_timer(State = #state{tref = undefined}) ->
    State;
cancel_timer(State = #state{tref = TRef}) ->
    _ = erlang:cancel_timer(TRef),
    State#state{tref = undefined}.

do_verify(JwsCompacted) ->
    try
        Remote = keys(remote),
        Jwks = case emqx_json:decode(jose_jws:peek_protected(JwsCompacted), [return_maps]) of
                   #{<<"kid">> := Kid} when Remote /= undefined ->
                       [J || J <- Remote, maps:get(<<"kid">>, J#jose_jwk.fields, undefined) =:= Kid];
                   _ -> keys(static)
               end,
        case Jwks of
            [] -> {error, not_found};
            _ ->
                do_verify(JwsCompacted, Jwks)
        end
    catch
        Class : Reason : Stk ->
            ?LOG(error, "verify JWK crashed: ~p, ~p, stacktrace: ~p~n",
                        [Class, Reason, Stk]),
            {error, invalid_signature}
    end.

do_verify(_JwsCompated, []) ->
    {error, invalid_signature};
do_verify(JwsCompacted, [Jwk|More]) ->
    case jose_jws:verify(Jwk, JwsCompacted) of
        {true, Payload, _Jws} ->
            Claims = emqx_json:decode(Payload, [return_maps]),
            case check_claims(Claims) of
                {false, <<"exp">>} ->
                    {error, {invalid_signature, expired}};
                NClaims ->
                    {ok, NClaims}
            end;
        {false, _, _} ->
            do_verify(JwsCompacted, More)
    end.

check_claims(Claims) ->
    Now = os:system_time(seconds),
    Checker = [{<<"exp">>, fun(ExpireTime) ->
                               Now < ExpireTime
                           end},
               {<<"iat">>, fun(IssueAt) ->
                               IssueAt =< Now
                           end},
               {<<"nbf">>, fun(NotBefore) ->
                               NotBefore =< Now
                           end}
              ],
    do_check_claim(Checker, Claims).

do_check_claim([], Claims) ->
    Claims;
do_check_claim([{K, F}|More], Claims) ->
    case Claims of
        #{K := V} ->
            case F(V) of
                true -> do_check_claim(More, Claims);
                _ -> {false, K}
            end;
        _ ->
            do_check_claim(More, Claims)
    end.
