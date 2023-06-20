%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tls_lib).

-export([ default_versions/0
        , integral_versions/1
        , default_ciphers/0
        , default_ciphers/1
        , integral_ciphers/2
        , drop_tls13_for_old_otp/1
        , inject_root_fun/1
        , inject_verify_fun/1
        , opt_partial_chain/1
        , opt_verify_fun/1
        , maybe_drop_incompatible_options/1
        ]).

-include("logger.hrl").
-include_lib("public_key/include/public_key.hrl").

%% non-empty string
-define(IS_STRING(L), (is_list(L) andalso L =/= [] andalso is_integer(hd(L)))).
%% non-empty list of strings
-define(IS_STRING_LIST(L), (is_list(L) andalso L =/= [] andalso ?IS_STRING(hd(L)))).

%% @doc Returns the default supported tls versions.
-spec default_versions() -> [atom()].
default_versions() ->
    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    integral_versions(default_versions(OtpRelease)).

%% @doc Validate a given list of desired tls versions.
%% raise an error exception if non of them are available.
%% The input list can be a string/binary of comma separated versions.
-spec integral_versions(undefined | string() | binary() | [ssl:tls_version()]) ->
        [ssl:tls_version()].
integral_versions(undefined) ->
    integral_versions(default_versions());
integral_versions([]) ->
    integral_versions(default_versions());
integral_versions(<<>>) ->
    integral_versions(default_versions());
integral_versions(Desired) when ?IS_STRING(Desired) ->
    integral_versions(iolist_to_binary(Desired));
integral_versions(Desired) when is_binary(Desired) ->
    integral_versions(parse_versions(Desired));
integral_versions(Desired) ->
    {_, Available} = lists:keyfind(available, 1, ssl:versions()),
    case lists:filter(fun(V) -> lists:member(V, Available) end, Desired) of
        [] -> erlang:error(#{ reason => no_available_tls_version
                            , desired => Desired
                            , available => Available
                            });
        Filtered ->
            Filtered
    end.

%% @doc Return a list of default (openssl string format) cipher suites.
-spec default_ciphers() -> [string()].
default_ciphers() -> default_ciphers(default_versions()).

%% @doc Return a list of (openssl string format) cipher suites.
-spec default_ciphers([ssl:tls_version()]) -> [string()].
default_ciphers(['tlsv1.3']) ->
    %% When it's only tlsv1.3 wanted, use 'exclusive' here
    %% because 'all' returns legacy cipher suites too,
    %% which does not make sense since tlsv1.3 can not use
    %% legacy cipher suites.
    ssl:cipher_suites(exclusive, 'tlsv1.3', openssl);
default_ciphers(Versions) ->
    %% assert non-empty
    [_ | _] = dedup(lists:append([ssl:cipher_suites(all, V, openssl) || V <- Versions])).

%% @doc Ensure version & cipher-suites integrity.
-spec integral_ciphers([ssl:tls_version()], binary() | string() | [string()]) -> [string()].
integral_ciphers(Versions, Ciphers) when Ciphers =:= [] orelse Ciphers =:= undefined ->
    %% not configured
    integral_ciphers(Versions, default_ciphers(Versions));
integral_ciphers(Versions, Ciphers) when ?IS_STRING_LIST(Ciphers) ->
    %% ensure tlsv1.3 ciphers if none of them is found in Ciphers
    dedup(ensure_tls13_cipher(lists:member('tlsv1.3', Versions), Ciphers));
integral_ciphers(Versions, Ciphers) when is_binary(Ciphers) ->
    %% parse binary
    integral_ciphers(Versions, binary_to_list(Ciphers));
integral_ciphers(Versions, Ciphers) ->
    %% parse comma separated cipher suite names
    integral_ciphers(Versions, string:tokens(Ciphers, ", ")).

%% In case tlsv1.3 is present, ensure tlsv1.3 cipher is added if user
%% did not provide it from config --- which is a common mistake
ensure_tls13_cipher(true, Ciphers) ->
    Tls13Ciphers = default_ciphers(['tlsv1.3']),
    case lists:any(fun(C) -> lists:member(C, Tls13Ciphers) end, Ciphers) of
        true  -> Ciphers;
        false -> Tls13Ciphers ++ Ciphers
    end;
ensure_tls13_cipher(false, Ciphers) ->
    Ciphers.

%% tlsv1.3 is available from OTP-22 but we do not want to use until 23.
default_versions(OtpRelease) when OtpRelease >= 23 ->
    ['tlsv1.3' | default_versions(22)];
default_versions(_) ->
    ['tlsv1.2', 'tlsv1.1', tlsv1].

%% Deduplicate a list without re-ordering the elements.
dedup([]) -> [];
dedup([H | T]) -> [H | dedup([I || I <- T, I =/= H])].

%% parse comma separated tls version strings
parse_versions(Versions) ->
    do_parse_versions(split_by_comma(Versions), []).

do_parse_versions([], Acc) -> lists:reverse(Acc);
do_parse_versions([V | More], Acc) ->
    case parse_version(V) of
        unknown ->
            emqx_logger:warning("unknown_tls_version_discarded: ~p", [V]),
            do_parse_versions(More, Acc);
        Parsed ->
            do_parse_versions(More, [Parsed | Acc])
    end.

parse_version(<<"tlsv", Vsn/binary>>) -> parse_version(Vsn);
parse_version(<<"v", Vsn/binary>>) -> parse_version(Vsn);
parse_version(<<"1.3">>) -> 'tlsv1.3';
parse_version(<<"1.2">>) -> 'tlsv1.2';
parse_version(<<"1.1">>) -> 'tlsv1.1';
parse_version(<<"1">>) -> 'tlsv1';
parse_version(_) -> unknown.

split_by_comma(Bin) ->
    [trim_space(I) || I <- binary:split(Bin, <<",">>, [global])].

%% trim spaces
trim_space(Bin) ->
    hd([I || I <- binary:split(Bin, <<" ">>), I =/= <<>>]).

%% @doc Drop tlsv1.3 version and ciphers from ssl options
%% if running on otp 22 or earlier.
drop_tls13_for_old_otp(SslOpts) ->
    case list_to_integer(erlang:system_info(otp_release)) < 23 of
        true -> drop_tls13(SslOpts);
        false -> SslOpts
    end.

%% The ciphers that ssl:cipher_suites(exclusive, 'tlsv1.3', openssl)
%% should return when running on otp 23.
%% But we still have to hard-code them because tlsv1.3 on otp 22 is
%% not trustworthy.
-define(TLSV13_EXCLUSIVE_CIPHERS, [ "TLS_AES_256_GCM_SHA384"
                                  , "TLS_AES_128_GCM_SHA256"
                                  , "TLS_CHACHA20_POLY1305_SHA256"
                                  , "TLS_AES_128_CCM_SHA256"
                                  , "TLS_AES_128_CCM_8_SHA256"
                                  ]).
drop_tls13(SslOpts0) ->
    SslOpts1 = case proplists:get_value(versions, SslOpts0) of
                   undefined -> SslOpts0;
                   Vsns -> replace(SslOpts0, versions, Vsns -- ['tlsv1.3'])
               end,
    case proplists:get_value(ciphers, SslOpts1) of
        undefined -> SslOpts1;
        Ciphers -> replace(SslOpts1, ciphers, Ciphers -- ?TLSV13_EXCLUSIVE_CIPHERS)
    end.

inject_root_fun(Options) ->
    case proplists:get_value(ssl_options, Options) of
        undefined ->
            Options;
        SslOpts ->
            replace(Options, ssl_options, opt_partial_chain(SslOpts))
    end.

inject_verify_fun(Options) ->
    case proplists:get_value(ssl_options, Options) of
        undefined ->
            Options;
        SslOpts ->
            replace(Options, ssl_options, emqx_tls_lib:opt_verify_fun(SslOpts))
    end.

%% @doc enable TLS partial_chain validation if set.
-spec opt_partial_chain(SslOpts :: proplists:proplist()) -> NewSslOpts :: proplists:proplist().
opt_partial_chain(SslOpts) ->
    case proplists:get_value(partial_chain, SslOpts, undefined) of
        undefined ->
            SslOpts;
        false ->
            proplists:delete(partial_chain, SslOpts);
        V when V =:= cacert_from_cacertfile orelse V == true ->
            replace(SslOpts, partial_chain, rootfun_trusted_ca_from_cacertfile(1, SslOpts));
        V when V =:= two_cacerts_from_cacertfile -> %% for certificate rotations
            replace(SslOpts, partial_chain, rootfun_trusted_ca_from_cacertfile(2, SslOpts))
    end.


-spec opt_verify_fun(SslOpts :: proplists:proplist()) -> NewSslOpts :: proplists:proplist().
opt_verify_fun(SslOpts) ->
    case proplists:get_value(verify_peer_ext_key_usage, SslOpts, undefined) of
        undefined ->
            SslOpts;
        V ->
            VerifyFun = emqx_const_v2:make_tls_verify_fun(verify_cert_extKeyUsage, V),
            replace(SslOpts, verify_fun, VerifyFun)
    end.

replace(Opts, Key, Value) -> [{Key, Value} | proplists:delete(Key, Opts)].

%% @doc Helper, make TLS root_fun
rootfun_trusted_ca_from_cacertfile(NumOfCerts, SslOpts) ->
    Cacertfile = proplists:get_value(cacertfile, SslOpts, undefined),
    case file:read_file(Cacertfile) of
        {ok, PemBin} ->
            try do_rootfun_trusted_ca_from_cacertfile(NumOfCerts, PemBin)
            catch _Error:_Info:ST ->
                %% The cacertfile will be checked by OTP SSL as well and OTP choice to be silent on this.
                %% We are touching security sutffs, don't leak extra info..
                ?LOG(error, "Failed to look for trusted cacert from cacertfile. Stacktrace: ~p", [ST]),
                throw({error, ?FUNCTION_NAME})
            end;
        {error, Reason} ->
            throw({error, {read_cacertfile_error, Cacertfile, Reason}})
    end.

do_rootfun_trusted_ca_from_cacertfile(NumOfCerts, PemBin) ->
    %% The last one or two should be the top parent in the chain if it is a chain
    Certs = public_key:pem_decode(PemBin),
    Pos = length(Certs) - NumOfCerts + 1,
    Trusted = [ CADer || {'Certificate', CADer, _} <-
                             lists:sublist(public_key:pem_decode(PemBin), Pos, NumOfCerts)],
    emqx_const_v2:make_tls_root_fun(cacert_from_cacertfile, Trusted).

maybe_drop_incompatible_options(Options) ->
    case proplists:get_value(ssl_options, Options) of
        undefined ->
            Options;
        SslOpts ->
            maybe_drop_incompatible_options(Options, SslOpts, lists:keyfind(versions, 1, SslOpts))
    end.

maybe_drop_incompatible_options(Options, _SslOpts, false) ->
    Options;
maybe_drop_incompatible_options(Options, SslOpts0, {versions, ['tlsv1.3']}) ->
    Incompatible = [reuse_sessions, secure_renegotiate, user_lookup_fun, client_renegotiation],
    SslOpts = lists:filter(fun({K, _V}) -> not lists:member(K, Incompatible) end, SslOpts0),
    lists:keyreplace(ssl_options, 1, Options, {ssl_options, SslOpts});
maybe_drop_incompatible_options(Options, _SslOpts, {versions, [_ | _]}) ->
    Options.

-if(?OTP_RELEASE > 22).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

drop_tls13_test() ->
    Versions = default_versions(),
    ?assert(lists:member('tlsv1.3', Versions)),
    Ciphers = default_ciphers(),
    ?assert(has_tlsv13_cipher(Ciphers)),
    Opts0 = [{versions, Versions}, {ciphers, Ciphers}, other, {bool, true}],
    Opts = drop_tls13(Opts0),
    ?assertNot(lists:member('tlsv1.3', proplists:get_value(versions, Opts))),
    ?assertNot(has_tlsv13_cipher(proplists:get_value(ciphers, Opts))).

drop_tls13_no_versions_cipers_test() ->
    Opts0 = [other, {bool, true}],
    Opts = drop_tls13(Opts0),
    ?_assertEqual(Opts0, Opts).

has_tlsv13_cipher(Ciphers) ->
    lists:any(fun(C) -> lists:member(C, Ciphers) end, ?TLSV13_EXCLUSIVE_CIPHERS).

maybe_drop_incompatible_options_test() ->
    Opts0 = [{ssl_options, [{versions, ['tlsv1.3']}, {ciphers, ?TLSV13_EXCLUSIVE_CIPHERS},
                            {reuse_sessions, true}, {secure_renegotiate, true},
                            {user_lookup_fun, fun maybe_drop_incompatible_options/1},
                            {client_renegotiation, true}]}],
    Opts = maybe_drop_incompatible_options(Opts0),
    ?assertNot(lists:member(reuse_sessions, proplists:get_value(ssl_options, Opts))),
    ?assertNot(lists:member(secure_renegotiate, proplists:get_value(ssl_options, Opts))),
    ?assertNot(lists:member(user_lookup_fun, proplists:get_value(ssl_options, Opts))),
    ?assertNot(lists:member(client_renegotiation, proplists:get_value(ssl_options, Opts))),
    ?assertEqual([{versions, ['tlsv1.3']}, {ciphers, ?TLSV13_EXCLUSIVE_CIPHERS}],
                 proplists:get_value(ssl_options, Opts)).

-endif. %% TEST
-endif. %% OTP_RELEASE > 22
