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

-module(emqx_authentication_jwt).

-export([ create/3
        , update/4
        , authenticate/2
        , destroy/1
        ]).

-service_type(#{
    name => jwt,
    params_spec => #{
        use_jwks => #{
            order => 1,
            type => boolean
        },
        jwks_endpoint => #{
            order => 2,
            type => string
        },
        refresh_interval => #{
            order => 3,
            type => number
        },
        algorithm => #{
            order => 3,
            type => string,
            enum => [<<"hmac-based">>, <<"public-key">>]
        },
        secret => #{
            order => 4,
            type => string
        },
        jwt_certfile => #{
            order => 5,
            type => file
        },
        cacertfile => #{
            order => 6,
            type => file
        },
        keyfile => #{
            order => 7,
            type => file
        },
        certfile => #{
            order => 8,
            type => file
        },
        verify => #{
            order => 9,
            type => boolean
        },
        server_name_indication => #{
            order => 10,
            type => string
        }
    }
}).

-define(RULES,
        #{
            use_jwks               => [],
            jwks_endpoint          => [use_jwks],
            refresh_interval       => [use_jwks],
            algorithm              => [use_jwks],
            secret                 => [algorithm],
            jwt_certfile           => [algorithm],
            cacertfile             => [jwks_endpoint],
            keyfile                => [jwks_endpoint],
            certfile               => [jwks_endpoint],
            verify                 => [jwks_endpoint],
            server_name_indication => [jwks_endpoint],
            verify_claims          => []
         }).

create(_ChainID, _ServiceName, Params) ->
    try handle_options(Params) of
        Opts ->
            do_create(Opts)
    catch
        {error, Reason} ->
            {error, Reason}
    end.

update(_ChainID, _ServiceName, Params, State) ->
    try handle_options(Params) of
        Opts ->
            do_update(Opts, State)
    catch
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(ClientInfo = #{password := JWT}, #{jwk := JWK,
                                                jwks_connector := Connector,
                                                verify_claims := VerifyClaims0}) ->
    JWKs = case Connector of
               undefined ->
                   [JWK];
               _ ->
                   {ok, JWKs0} = emqx_authentication_jwks_connector:get_jwks(Connector),
                   JWKs0
           end,
    VerifyClaims = replace_placeholder(VerifyClaims0, ClientInfo),
    verify(JWT, JWKs, VerifyClaims).

destroy(#{jwks_connector := undefined}) ->
    ok;
destroy(#{jwks_connector := Connector}) ->
    emqx_authentication_jwks_connector:stop(Connector),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_create(#{use_jwks := false,
            algorithm := 'hmac-based',
            secret := Secret,
            verify_claims := VerifyClaims}) ->
    JWK = jose_jwk:from_oct(Secret),
    {ok, #{jwk => JWK,
           jwks_connector => undefined,
           verify_claims => VerifyClaims}};
do_create(#{use_jwks := false,
            algorithm := 'public-key',
            jwt_certfile := Certfile,
            verify_claims := VerifyClaims}) ->
    JWK = jose_jwk:from_pem_file(Certfile),
    {ok, #{jwk => JWK,
           jwks_connector => undefined,
           verify_claims => VerifyClaims}};
do_create(#{use_jwks := true,
            verify_claims := VerifyClaims} = Opts) ->
    case emqx_authentication_jwks_connector:start_link(Opts) of
        {ok, Connector} ->
            {ok, #{jwk => undefined,
                   jwks_connector => Connector,
                   verify_claims => VerifyClaims}};
        {error, Reason} ->
            {error, Reason}
    end.

do_update(Opts, #{jwk_connector := undefined}) ->
    do_create(Opts);
do_update(#{use_jwks := false} = Opts, #{jwk_connector := Connector}) ->
    emqx_authentication_jwks_connector:stop(Connector),
    do_create(Opts);
do_update(#{use_jwks := true} = Opts, #{jwk_connector := Connector} = State) ->
    ok = emqx_authentication_jwks_connector:update(Connector, Opts),
    {ok, State}.

replace_placeholder(L, Variables) ->
    replace_placeholder(L, Variables, []).

replace_placeholder([], _Variables, Acc) ->
    Acc;
replace_placeholder([{Name, {placeholder, PL}} | More], Variables, Acc) ->
    Value = maps:get(PL, Variables),
    replace_placeholder(More, Variables, [{Name, Value} | Acc]);
replace_placeholder([{Name, Value} | More], Variables, Acc) ->
    replace_placeholder(More, Variables, [{Name, Value} | Acc]).

verify(_JWS, [], _VerifyClaims) ->
    {error, invalid_signature};
verify(JWS, [JWK | More], VerifyClaims) ->
    case jose_jws:verify(JWK, JWS) of
        {true, Payload, _JWS} ->
            Claims = emqx_json:decode(Payload, [return_maps]),
            verify_claims(Claims, VerifyClaims);
        {false, _, _} ->
            verify(JWS, More, VerifyClaims)
    end.

verify_claims(Claims, VerifyClaims0) ->
    Now = os:system_time(seconds),
    VerifyClaims = [{<<"exp">>, fun(ExpireTime) ->
                                    Now < ExpireTime
                                end},
                    {<<"iat">>, fun(IssueAt) ->
                                    IssueAt =< Now
                                end},
                    {<<"nbf">>, fun(NotBefore) ->
                                    NotBefore =< Now
                                end}] ++ VerifyClaims0,
    do_verify_claims(Claims, VerifyClaims).

do_verify_claims(_Claims, []) ->
    ok;
do_verify_claims(Claims, [{Name, Fun} | More]) when is_function(Fun) ->
    case maps:take(Name, Claims) of
        error ->
            do_verify_claims(Claims, More);
        {Value, NClaims} ->
            case Fun(Value) of
                true ->
                    do_verify_claims(NClaims, More);
                _ ->
                    {error, {claims, {Name, Value}}}
            end
    end;
do_verify_claims(Claims, [{Name, Value} | More]) ->
    case maps:take(Name, Claims) of
        error ->
             do_verify_claims(Claims, More);
        {Value, NClaims} ->
            do_verify_claims(NClaims, More);
        {Value0, _} ->
            {error, {claims, {Name, Value0}}}
    end.

handle_options(Opts0) when is_map(Opts0) ->
    Ks = maps:fold(fun(K, _, Acc) ->
                       [atom_to_binary(K, utf8) | Acc]
                   end, [], ?RULES),
    Opts1 = maps:to_list(maps:with(Ks, Opts0)),
    handle_options([{binary_to_existing_atom(K, utf8), V} || {K, V} <- Opts1]);

handle_options(Opts0) when is_list(Opts0) ->
    Opts1 = add_missing_options(Opts0),
    process_options({Opts1, [], length(Opts1)}, #{}).

add_missing_options(Opts) ->
    AllOpts = maps:keys(?RULES),
    Fun = fun(K, Acc) ->
               case proplists:is_defined(K, Acc) of
                   true ->
                       Acc;
                   false ->
                       [{K, unbound} | Acc]
                  end
          end,
    lists:foldl(Fun, Opts, AllOpts).

process_options({[], [], _}, OptsMap) ->
    OptsMap;
process_options({[], Skipped, Counter}, OptsMap)
  when length(Skipped) < Counter ->
    process_options({Skipped, [], length(Skipped)}, OptsMap);
process_options({[], _Skipped, _Counter}, _OptsMap) ->
    throw({error, faulty_configuration});
process_options({[{K, V} = Opt | More], Skipped, Counter}, OptsMap0) ->
    case check_dependencies(K, OptsMap0) of
        true ->
            OptsMap1 = handle_option(K, V, OptsMap0),
            process_options({More, Skipped, Counter}, OptsMap1);
        false ->
            process_options({More, [Opt | Skipped], Counter}, OptsMap0)
    end.

%% TODO: This is not a particularly good implementation(K => needless), it needs to be improved
handle_option(use_jwks, true, OptsMap) ->
    OptsMap#{use_jwks => true,
             algorithm => needless};
handle_option(use_jwks, false, OptsMap) ->
    OptsMap#{use_jwks => false,
             jwks_endpoint => needless};      
handle_option(jwks_endpoint = Opt, unbound, #{use_jwks := true}) ->
    throw({error, {options, {Opt, unbound}}});
handle_option(jwks_endpoint, Value, #{use_jwks := true} = OptsMap)
  when Value =/= unbound ->
    case emqx_http_lib:uri_parse(Value) of
        {ok, #{scheme := http}} ->
            OptsMap#{enable_ssl => false,
                     jwks_endpoint => Value};
        {ok, #{scheme := https}} ->
            OptsMap#{enable_ssl => true,
                     jwks_endpoint => Value};
        {error, _Reason} ->
            throw({error, {options, {jwks_endpoint, Value}}})
    end;
handle_option(refresh_interval = Opt, Value0, #{use_jwks := true} = OptsMap) ->
    Value = validate_option(Opt, Value0),
    OptsMap#{Opt => Value};
handle_option(algorithm = Opt, Value0, #{use_jwks := false} = OptsMap) ->
    Value = validate_option(Opt, Value0),
    OptsMap#{Opt => Value};
handle_option(secret = Opt, unbound, #{algorithm := 'hmac-based'}) ->
    throw({error, {options, {Opt, unbound}}});
handle_option(secret = Opt, Value, #{algorithm := 'hmac-based'} = OptsMap) ->
    OptsMap#{Opt => Value};
handle_option(jwt_certfile = Opt, unbound, #{algorithm := 'public-key'}) ->
    throw({error, {options, {Opt, unbound}}});
handle_option(jwt_certfile = Opt, Value, #{algorithm := 'public-key'} = OptsMap) ->
    OptsMap#{Opt => Value};
handle_option(verify = Opt, Value0, #{enable_ssl := true} = OptsMap) ->
    Value = validate_option(Opt, Value0),
    OptsMap#{Opt => Value};
handle_option(cacertfile = Opt, Value, #{enable_ssl := true} = OptsMap)
  when Value =/= unbound ->
    OptsMap#{Opt => Value};
handle_option(certfile, unbound, #{enable_ssl := true} = OptsMap) ->
    OptsMap;
handle_option(certfile = Opt, Value, #{enable_ssl := true} = OptsMap) ->
    OptsMap#{Opt => Value};
handle_option(keyfile, unbound, #{enable_ssl := true} = OptsMap) ->
    OptsMap;
handle_option(keyfile = Opt, Value, #{enable_ssl := true} = OptsMap) ->
    OptsMap#{Opt => Value};
handle_option(server_name_indication = Opt, Value0, #{enable_ssl := true} = OptsMap) ->
    Value = validate_option(Opt, Value0),
    OptsMap#{Opt => Value};
handle_option(verify_claims = Opt, Value0, OptsMap) ->
    Value = handle_verify_claims(Value0),
    OptsMap#{Opt => Value};
handle_option(_Opt, _Value, OptsMap) ->
    OptsMap.

validate_option(refresh_interval, unbound) ->
    300;
validate_option(refresh_interval, Value) when is_integer(Value) ->
    Value;
validate_option(algorithm, <<"hmac-based">>) ->
    'hmac-based';
validate_option(algorithm, <<"public-key">>) ->
    'public-key';
validate_option(verify, unbound) ->
    verify_none;
validate_option(verify, true) ->
    verify_peer;
validate_option(verify, false) ->
    verify_none;
validate_option(server_name_indication, unbound) ->
    disable;
validate_option(server_name_indication, <<"disable">>) ->
    disable;
validate_option(server_name_indication, Value) when is_list(Value) ->
    Value;
validate_option(Opt, Value) ->
    throw({error, {options, {Opt, Value}}}).

handle_verify_claims(Opts0) ->
    try handle_verify_claims(Opts0, [])
    catch
        error:_ ->
            throw({error, {options, {verify_claims, Opts0}}})
    end.

handle_verify_claims([], Acc) ->
    Acc;
handle_verify_claims([{Name, Expected0} | More], Acc)
  when is_binary(Name) andalso is_binary(Expected0) ->
    Expected = handle_placeholder(Expected0),
    handle_verify_claims(More, [{Name, Expected} | Acc]).

handle_placeholder(Placeholder0) ->
    case re:run(Placeholder0, "^\\$\\{[a-z0-9\\_]+\\}$", [{capture, all}]) of
        {match, [{Offset, Length}]} ->
            Placeholder1 = binary:part(Placeholder0, Offset + 2, Length - 3),
            Placeholder2 = validate_placeholder(Placeholder1),
            {placeholder, Placeholder2};
        nomatch ->
            Placeholder0
    end.

validate_placeholder(<<"clientid">>) ->
    clientid;
validate_placeholder(<<"username">>) ->
    username.

check_dependencies(Opt, OptsMap) ->
    case maps:get(Opt, ?RULES) of
        [] ->
            true;
        Deps ->
            option_already_defined(Opt, OptsMap) orelse
                dependecies_already_defined(Deps, OptsMap)
    end.

option_already_defined(Opt, OptsMap) ->
    maps:get(Opt, OptsMap, unbound) =/= unbound.

dependecies_already_defined(Deps, OptsMap) ->
    Fun = fun(Opt) -> option_already_defined(Opt, OptsMap) end,
    lists:all(Fun, Deps).
