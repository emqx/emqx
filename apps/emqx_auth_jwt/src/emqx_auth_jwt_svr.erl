%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[JWT-SVR]").

%% APIs
-export([start_link/1]).

-export([verify/1, trace/2]).

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
    Static = do_init_jwks(Options),
    to_request_jwks(Options),
    true = ets:insert(?TAB, [{static, Static}, {remote, undefined}]),
    Intv = proplists:get_value(interval, Options, ?INTERVAL),
    {ok, reset_timer(
           #state{
              addr = proplists:get_value(jwks_addr, Options),
              intv = Intv})}.

%% @private
do_init_jwks(Options) ->
    OctJwk = key2jwt_value(secret,
                           fun(V) ->
                                   jose_jwk:from_oct(list_to_binary(V))
                           end,
                           Options),
    PemJwk = key2jwt_value(pubkey, fun jose_jwk:from_pem_file/1, Options),
    [J ||J <- [OctJwk, PemJwk], J /= undefined].

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, refresh}, State = #state{addr = Addr}) ->
    try
        true = ets:insert(?TAB, {remote, request_jwks(Addr)})
    catch Err:Reason ->
        ?LOG_SENSITIVE(warning, "Request JWKS failed, jwks_addr: ~p, reason: ~p",
            [Addr, {Err, Reason}])
    end,
    {noreply, reset_timer(State)};

handle_info({request_jwks, Options}, State) ->
    Remote = key2jwt_value(jwks_addr, fun request_jwks/1, Options),
    true = ets:insert(?TAB, {remote, Remote}),
    {noreply, State};

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
                {_, Jwks} = JwkSet#jose_jwk.keys,
                ?tp(debug, emqx_auth_jwt_svr_jwks_updated, #{jwks => Jwks, pid => self()}),
                Jwks
            catch _:_ ->
                ?MODULE:trace(jwks_server_reesponse, Body),
                ?LOG(error, "Invalid jwks server response, body is not logged for security reasons, trace it if inspection is required", []),
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
            ?LOG_SENSITIVE(error, "verify JWK crashed: ~p, ~p, stacktrace: ~p~n",
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
                {false, <<"iat">>} ->
                    {error, {invalid_signature, issued_in_future}};
                {false, <<"nbf">>} ->
                    {error, {invalid_signature, not_valid_yet}};
                {true, NClaims} ->
                    {ok, NClaims}
            end;
        {false, _, _} ->
            do_verify(JwsCompacted, More)
    end.

check_claims(Claims) ->
    Now = erlang:system_time(seconds),
    Checker = [{<<"exp">>, with_num_value(
                             fun(ExpireTime) -> Now < ExpireTime end)},
               {<<"iat">>, with_num_value(
                             fun(IssueAt) -> IssueAt =< Now end)},
               {<<"nbf">>, with_num_value(
                             fun(NotBefore) -> NotBefore =< Now end)}
              ],
    do_check_claim(Checker, Claims).

with_num_value(Fun) ->
    fun(Value) ->
            case Value of
                Num when is_number(Num) -> Fun(Num);
                Bin when is_binary(Bin) ->
                    case emqx_auth_jwt:string_to_number(Bin) of
                        {ok, Num} -> Fun(Num);
                        _ -> false
                    end;
                Str when is_list(Str) ->
                    case emqx_auth_jwt:string_to_number(Str) of
                        {ok, Num} -> Fun(Num);
                        _ -> false
                    end
            end
    end.

do_check_claim([], Claims) ->
    {true, Claims};
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

to_request_jwks(Options) ->
    erlang:send(self(), {request_jwks, Options}).

key2jwt_value(Key, Func, Options) ->
    case proplists:get_value(Key, Options) of
        undefined -> undefined;
        V ->
            try Func(V) of
                {error, Reason} ->
                    ?LOG_SENSITIVE(warning, "Build ~p JWK ~p failed: {error, ~p}~n",
                         [Key, V, Reason]),
                    undefined;
                J -> J
            catch T:R ->
                    ?LOG_SENSITIVE(warning, "Build ~p JWK ~p failed: {~p, ~p}~n",
                         [Key, V, T, R]),
                    undefined
            end
    end.

trace(_Tag, _Data) -> ok.
