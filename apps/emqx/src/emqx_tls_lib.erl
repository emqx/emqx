%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% version & cipher suites
-export([
    available_versions/1,
    integral_versions/2,
    default_ciphers/0,
    selected_ciphers/1,
    integral_ciphers/2,
    all_ciphers_set_cached/0
]).

%% SSL files
-export([
    ensure_ssl_files/2,
    ensure_ssl_files/3,
    delete_ssl_files/3,
    drop_invalid_certs/1,
    is_valid_pem_file/1,
    is_pem/1
]).

-export([
    to_server_opts/2,
    to_client_opts/1,
    to_client_opts/2
]).

-include("logger.hrl").

-define(IS_TRUE(Val), ((Val =:= true) orelse (Val =:= <<"true">>))).
-define(IS_FALSE(Val), ((Val =:= false) orelse (Val =:= <<"false">>))).

-define(SSL_FILE_OPT_NAMES, [<<"keyfile">>, <<"certfile">>, <<"cacertfile">>]).
-define(SSL_FILE_OPT_NAMES_A, [keyfile, certfile, cacertfile]).

%% non-empty string
-define(IS_STRING(L), (is_list(L) andalso L =/= [] andalso is_integer(hd(L)))).
%% non-empty list of strings
-define(IS_STRING_LIST(L), (is_list(L) andalso L =/= [] andalso ?IS_STRING(hd(L)))).

%% The ciphers that ssl:cipher_suites(exclusive, 'tlsv1.3', openssl)
%% should return when running on otp 23.
%% But we still have to hard-code them because tlsv1.3 on otp 22 is
%% not trustworthy.
-define(TLSV13_EXCLUSIVE_CIPHERS, [
    "TLS_AES_256_GCM_SHA384",
    "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_CCM_SHA256",
    "TLS_AES_128_CCM_8_SHA256"
]).

-define(SELECTED_CIPHERS, [
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-ECDSA-AES256-SHA384",
    "ECDHE-RSA-AES256-SHA384",
    "ECDH-ECDSA-AES256-GCM-SHA384",
    "ECDH-RSA-AES256-GCM-SHA384",
    "ECDH-ECDSA-AES256-SHA384",
    "ECDH-RSA-AES256-SHA384",
    "DHE-DSS-AES256-GCM-SHA384",
    "DHE-DSS-AES256-SHA256",
    "AES256-GCM-SHA384",
    "AES256-SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES128-SHA256",
    "ECDHE-RSA-AES128-SHA256",
    "ECDH-ECDSA-AES128-GCM-SHA256",
    "ECDH-RSA-AES128-GCM-SHA256",
    "ECDH-ECDSA-AES128-SHA256",
    "ECDH-RSA-AES128-SHA256",
    "DHE-DSS-AES128-GCM-SHA256",
    "DHE-DSS-AES128-SHA256",
    "AES128-GCM-SHA256",
    "AES128-SHA256",
    "ECDHE-ECDSA-AES256-SHA",
    "ECDHE-RSA-AES256-SHA",
    "DHE-DSS-AES256-SHA",
    "ECDH-ECDSA-AES256-SHA",
    "ECDH-RSA-AES256-SHA",
    "ECDHE-ECDSA-AES128-SHA",
    "ECDHE-RSA-AES128-SHA",
    "DHE-DSS-AES128-SHA",
    "ECDH-ECDSA-AES128-SHA",
    "ECDH-RSA-AES128-SHA",

    %% psk
    "RSA-PSK-AES256-GCM-SHA384",
    "RSA-PSK-AES256-CBC-SHA384",
    "RSA-PSK-AES128-GCM-SHA256",
    "RSA-PSK-AES128-CBC-SHA256",
    "RSA-PSK-AES256-CBC-SHA",
    "RSA-PSK-AES128-CBC-SHA"
]).

%% @doc Validate a given list of desired tls versions.
%% raise an error exception if non of them are available.
%% The input list can be a string/binary of comma separated versions.
-spec integral_versions(tls | dtls, undefined | string() | binary() | [ssl:tls_version()]) ->
    [ssl:tls_version()].
integral_versions(Type, undefined) ->
    available_versions(Type);
integral_versions(Type, []) ->
    available_versions(Type);
integral_versions(Type, <<>>) ->
    available_versions(Type);
integral_versions(Type, Desired) when ?IS_STRING(Desired) ->
    integral_versions(Type, iolist_to_binary(Desired));
integral_versions(Type, Desired) when is_binary(Desired) ->
    integral_versions(Type, parse_versions(Desired));
integral_versions(Type, Desired) ->
    Available = available_versions(Type),
    case lists:filter(fun(V) -> lists:member(V, Available) end, Desired) of
        [] ->
            erlang:error(#{
                reason => no_available_tls_version,
                desired => Desired,
                available => Available
            });
        Filtered ->
            Filtered
    end.

%% @doc Return a set of all ciphers
all_ciphers_set_cached() ->
    case persistent_term:get(?FUNCTION_NAME, false) of
        false ->
            S = sets:from_list(all_ciphers()),
            persistent_term:put(?FUNCTION_NAME, S),
            S;
        Set ->
            Set
    end.

%% @hidden Return a list of all supported ciphers.
all_ciphers() ->
    all_ciphers(available_versions(all)).

%% @hidden Return a list of (openssl string format) cipher suites.
-spec all_ciphers([ssl:tls_version()]) -> [string()].
all_ciphers(['tlsv1.3']) ->
    %% When it's only tlsv1.3 wanted, use 'exclusive' here
    %% because 'all' returns legacy cipher suites too,
    %% which does not make sense since tlsv1.3 can not use
    %% legacy cipher suites.
    ?TLSV13_EXCLUSIVE_CIPHERS;
all_ciphers(Versions) ->
    %% assert non-empty
    List = lists:append([ssl:cipher_suites(all, V, openssl) || V <- Versions]),

    %% Some PSK ciphers are both supported by OpenSSL and Erlang, but they need manual add here.
    %% Found by this cmd
    %% openssl ciphers -v|grep ^PSK| awk '{print $1}'| sed  "s/^/\"/;s/$/\"/" | tr "\n" ","
    %% Then remove the ciphers that aren't supported by Erlang
    PSK = [
        "PSK-AES256-GCM-SHA384",
        "PSK-AES128-GCM-SHA256",
        "PSK-AES256-CBC-SHA384",
        "PSK-AES256-CBC-SHA",
        "PSK-AES128-CBC-SHA256",
        "PSK-AES128-CBC-SHA"
    ],
    [_ | _] = dedup(List ++ PSK).

%% @doc All Pre-selected TLS ciphers.
default_ciphers() ->
    selected_ciphers(available_versions(all)).

%% @doc Pre-selected TLS ciphers for given versions..
selected_ciphers(Vsns) ->
    All = all_ciphers(Vsns),
    dedup(
        lists:filter(
            fun(Cipher) -> lists:member(Cipher, All) end,
            lists:flatmap(fun do_selected_ciphers/1, Vsns)
        )
    ).

do_selected_ciphers('tlsv1.3') ->
    case lists:member('tlsv1.3', proplists:get_value(available, ssl:versions())) of
        true -> ?TLSV13_EXCLUSIVE_CIPHERS;
        false -> []
    end ++ do_selected_ciphers('tlsv1.2');
do_selected_ciphers(_) ->
    ?SELECTED_CIPHERS.

%% @doc Ensure version & cipher-suites integrity.
-spec integral_ciphers([ssl:tls_version()], binary() | string() | [string()]) -> [string()].
integral_ciphers(Versions, Ciphers) when Ciphers =:= [] orelse Ciphers =:= undefined ->
    %% not configured
    integral_ciphers(Versions, selected_ciphers(Versions));
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
    Tls13Ciphers = selected_ciphers(['tlsv1.3']),
    case lists:any(fun(C) -> lists:member(C, Tls13Ciphers) end, Ciphers) of
        true -> Ciphers;
        false -> Tls13Ciphers ++ Ciphers
    end;
ensure_tls13_cipher(false, Ciphers) ->
    Ciphers.

%% @doc Returns the default available tls/dtls versions.
available_versions(Type) ->
    All = ssl:versions(),
    available_versions(Type, All).

available_versions(tls, All) ->
    proplists:get_value(available, All);
available_versions(dtls, All) ->
    proplists:get_value(available_dtls, All);
available_versions(all, All) ->
    available_versions(tls, All) ++ available_versions(dtls, All).

%% Deduplicate a list without re-ordering the elements.
dedup([]) ->
    [];
dedup(List0) ->
    List = lists:foldl(
        fun(L, Acc) ->
            case lists:member(L, Acc) of
                false -> [L | Acc];
                true -> Acc
            end
        end,
        [],
        List0
    ),
    lists:reverse(List).

%% parse comma separated tls version strings
parse_versions(Versions) ->
    do_parse_versions(split_by_comma(Versions), []).

do_parse_versions([], Acc) ->
    lists:reverse(Acc);
do_parse_versions([V | More], Acc) ->
    case parse_version(V) of
        unknown ->
            ?SLOG(warning, #{msg => "unknown_tls_version_discarded", version => V}),
            do_parse_versions(More, Acc);
        Parsed ->
            do_parse_versions(More, [Parsed | Acc])
    end.

parse_version(<<"dtlsv1.2">>) -> 'dtlsv1.2';
parse_version(<<"dtlsv1">>) -> dtlsv1;
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

%% @doc The input map is a HOCON decoded result of a struct defined as
%% emqx_schema:server_ssl_opts_schema. (NOTE: before schema-checked).
%% `keyfile', `certfile' and `cacertfile' can be either pem format key or certificates,
%% or file path.
%% When PEM format key or certificate is given, it tries to to save them in the given
%% sub-dir in emqx's data_dir, and replace saved file paths for SSL options.
-spec ensure_ssl_files(file:name_all(), undefined | map()) ->
    {ok, undefined | map()} | {error, map()}.
ensure_ssl_files(Dir, SSL) ->
    ensure_ssl_files(Dir, SSL, #{dry_run => false, required_keys => []}).

ensure_ssl_files(_Dir, undefined, _Opts) ->
    {ok, undefined};
ensure_ssl_files(_Dir, #{<<"enable">> := False} = SSL, _Opts) when ?IS_FALSE(False) ->
    {ok, SSL};
ensure_ssl_files(_Dir, #{enable := False} = SSL, _Opts) when ?IS_FALSE(False) ->
    {ok, SSL};
ensure_ssl_files(Dir, SSL, Opts) ->
    RequiredKeys = maps:get(required_keys, Opts, []),
    case ensure_ssl_file_key(SSL, RequiredKeys) of
        ok ->
            Keys = ?SSL_FILE_OPT_NAMES ++ ?SSL_FILE_OPT_NAMES_A,
            ensure_ssl_files(Dir, SSL, Keys, Opts);
        {error, _} = Error ->
            Error
    end.

ensure_ssl_files(_Dir, SSL, [], _Opts) ->
    {ok, SSL};
ensure_ssl_files(Dir, SSL, [Key | Keys], Opts) ->
    case ensure_ssl_file(Dir, Key, SSL, maps:get(Key, SSL, undefined), Opts) of
        {ok, NewSSL} ->
            ensure_ssl_files(Dir, NewSSL, Keys, Opts);
        {error, Reason} ->
            {error, Reason#{which_options => [Key]}}
    end.

%% @doc Compare old and new config, delete the ones in old but not in new.
-spec delete_ssl_files(file:name_all(), undefined | map(), undefined | map()) -> ok.
delete_ssl_files(Dir, NewOpts0, OldOpts0) ->
    DryRun = true,
    {ok, NewOpts} = ensure_ssl_files(Dir, NewOpts0, #{dry_run => DryRun}),
    {ok, OldOpts} = ensure_ssl_files(Dir, OldOpts0, #{dry_run => DryRun}),
    Get = fun
        (_K, undefined) -> undefined;
        (K, Opts) -> maps:get(K, Opts, undefined)
    end,
    lists:foreach(
        fun(Key) -> delete_old_file(Get(Key, NewOpts), Get(Key, OldOpts)) end,
        ?SSL_FILE_OPT_NAMES ++ ?SSL_FILE_OPT_NAMES_A
    ),
    %% try to delete the dir if it is empty
    _ = file:del_dir(pem_dir(Dir)),
    ok.

delete_old_file(New, Old) when New =:= Old -> ok;
delete_old_file(_New, _Old = undefined) ->
    ok;
delete_old_file(_New, Old) ->
    case is_generated_file(Old) andalso filelib:is_regular(Old) andalso file:delete(Old) of
        ok ->
            ok;
        %% the file is not generated by us, or it is already deleted
        false ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_delete_ssl_file", file_path => Old, reason => Reason})
    end.

ensure_ssl_file(_Dir, _Key, SSL, undefined, _Opts) ->
    {ok, SSL};
ensure_ssl_file(Dir, Key, SSL, MaybePem, Opts) ->
    case is_valid_string(MaybePem) of
        true ->
            DryRun = maps:get(dry_run, Opts, false),
            do_ensure_ssl_file(Dir, Key, SSL, MaybePem, DryRun);
        false ->
            {error, #{reason => invalid_file_path_or_pem_string}}
    end.

do_ensure_ssl_file(Dir, Key, SSL, MaybePem, DryRun) ->
    case is_pem(MaybePem) of
        true ->
            case save_pem_file(Dir, Key, MaybePem, DryRun) of
                {ok, Path} -> {ok, SSL#{Key => Path}};
                {error, Reason} -> {error, Reason}
            end;
        false ->
            case is_valid_pem_file(MaybePem) of
                true ->
                    {ok, SSL};
                {error, enoent} when DryRun -> {ok, SSL};
                {error, Reason} ->
                    {error, #{
                        pem_check => invalid_pem,
                        file_read => Reason
                    }}
            end
    end.

is_valid_string(Empty) when Empty == <<>>; Empty == "" -> false;
is_valid_string(String) when is_list(String) ->
    io_lib:printable_unicode_list(String);
is_valid_string(Binary) when is_binary(Binary) ->
    case unicode:characters_to_list(Binary, utf8) of
        String when is_list(String) -> is_valid_string(String);
        _Otherwise -> false
    end.

%% Check if it is a valid PEM formatted key.
is_pem(MaybePem) ->
    try
        public_key:pem_decode(MaybePem) =/= []
    catch
        _:_ -> false
    end.

%% Write the pem file to the given dir.
%% To make it simple, the file is always overwritten.
%% Also a potentially half-written PEM file (e.g. due to power outage)
%% can be corrected with an overwrite.
save_pem_file(Dir, Key, Pem, DryRun) ->
    Path = pem_file_name(Dir, Key, Pem),
    case filelib:ensure_dir(Path) of
        ok when DryRun ->
            {ok, Path};
        ok ->
            case file:write_file(Path, Pem) of
                ok -> {ok, Path};
                {error, Reason} -> {error, #{failed_to_write_file => Reason, file_path => Path}}
            end;
        {error, Reason} ->
            {error, #{failed_to_create_dir_for => Path, reason => Reason}}
    end.

%% compute the filename for a PEM format key/certificate
%% the filename is prefixed by the option name without the 'file' part
%% and suffixed with the first 8 byets the PEM content's md5 checksum.
%% e.g. key-1234567890abcdef, cert-1234567890abcdef, and cacert-1234567890abcdef
is_generated_file(Filename) ->
    case string:split(filename:basename(Filename), "-") of
        [_Name, Suffix] -> is_hex_str(Suffix);
        _ -> false
    end.

pem_file_name(Dir, Key, Pem) ->
    <<CK:8/binary, _/binary>> = crypto:hash(md5, Pem),
    Suffix = hex_str(CK),
    FileName = binary:replace(ensure_bin(Key), <<"file">>, <<"-", Suffix/binary>>),
    filename:join([pem_dir(Dir), FileName]).

pem_dir(Dir) ->
    filename:join([emqx:mutable_certs_dir(), Dir]).

is_hex_str(HexStr) ->
    try
        is_hex_str2(ensure_str(HexStr))
    catch
        throw:not_hex -> false
    end.

is_hex_str2(HexStr) ->
    _ = [
        case S of
            S when S >= $0, S =< $9 -> S;
            S when S >= $a, S =< $f -> S;
            _ -> throw(not_hex)
        end
     || S <- HexStr
    ],
    true.

hex_str(Bin) ->
    iolist_to_binary([io_lib:format("~2.16.0b", [X]) || <<X:8>> <= Bin]).

%% @doc Returns 'true' when the file is a valid pem, otherwise {error, Reason}.
is_valid_pem_file(Path) ->
    case file:read_file(Path) of
        {ok, Pem} -> is_pem(Pem) orelse {error, not_pem};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Input and output are both HOCON-checked maps, with invalid SSL
%% file options dropped.
%% This is to give a feedback to the front-end or management API caller
%% so they are forced to upload a cert file, or use an existing file path.
-spec drop_invalid_certs(map()) -> map().
drop_invalid_certs(#{enable := False} = SSL) when ?IS_FALSE(False) ->
    maps:without(?SSL_FILE_OPT_NAMES_A, SSL);
drop_invalid_certs(#{<<"enable">> := False} = SSL) when ?IS_FALSE(False) ->
    maps:without(?SSL_FILE_OPT_NAMES, SSL);
drop_invalid_certs(#{enable := True} = SSL) when ?IS_TRUE(True) ->
    do_drop_invalid_certs(?SSL_FILE_OPT_NAMES_A, SSL);
drop_invalid_certs(#{<<"enable">> := True} = SSL) when ?IS_TRUE(True) ->
    do_drop_invalid_certs(?SSL_FILE_OPT_NAMES, SSL).

do_drop_invalid_certs([], SSL) ->
    SSL;
do_drop_invalid_certs([Key | Keys], SSL) ->
    case maps:get(Key, SSL, undefined) of
        undefined ->
            do_drop_invalid_certs(Keys, SSL);
        PemOrPath ->
            case is_pem(PemOrPath) orelse is_valid_pem_file(PemOrPath) of
                true -> do_drop_invalid_certs(Keys, SSL);
                {error, _} -> do_drop_invalid_certs(Keys, maps:without([Key], SSL))
            end
    end.

%% @doc Convert hocon-checked ssl server options (map()) to
%% proplist accepted by ssl library.
-spec to_server_opts(tls | dtls, map()) -> [{atom(), term()}].
to_server_opts(Type, Opts) ->
    Versions = integral_versions(Type, maps:get(versions, Opts, undefined)),
    Ciphers = integral_ciphers(Versions, maps:get(ciphers, Opts, undefined)),
    maps:to_list(Opts#{
        ciphers => Ciphers,
        versions => Versions
    }).

%% @doc Convert hocon-checked tls client options (map()) to
%% proplist accepted by ssl library.
-spec to_client_opts(map()) -> [{atom(), term()}].
to_client_opts(Opts) ->
    to_client_opts(tls, Opts).

%% @doc Convert hocon-checked tls or dtls client options (map()) to
%% proplist accepted by ssl library.
-spec to_client_opts(tls | dtls, map()) -> [{atom(), term()}].
to_client_opts(Type, Opts) ->
    GetD = fun(Key, Default) -> fuzzy_map_get(Key, Opts, Default) end,
    Get = fun(Key) -> GetD(Key, undefined) end,
    case GetD(enable, false) of
        true ->
            KeyFile = ensure_str(Get(keyfile)),
            CertFile = ensure_str(Get(certfile)),
            CAFile = ensure_str(Get(cacertfile)),
            Verify = GetD(verify, verify_none),
            SNI = ensure_sni(Get(server_name_indication)),
            Versions = integral_versions(Type, Get(versions)),
            Ciphers = integral_ciphers(Versions, Get(ciphers)),
            filter([
                {keyfile, KeyFile},
                {certfile, CertFile},
                {cacertfile, CAFile},
                {verify, Verify},
                {server_name_indication, SNI},
                {versions, Versions},
                {ciphers, Ciphers},
                {reuse_sessions, Get(reuse_sessions)},
                {depth, Get(depth)},
                {password, ensure_str(Get(password))},
                {secure_renegotiate, Get(secure_renegotiate)}
            ]);
        false ->
            []
    end.

filter([]) -> [];
filter([{_, undefined} | T]) -> filter(T);
filter([{_, ""} | T]) -> filter(T);
filter([H | T]) -> [H | filter(T)].

-spec fuzzy_map_get(atom() | binary(), map(), any()) -> any().
fuzzy_map_get(Key, Options, Default) ->
    case maps:find(Key, Options) of
        {ok, Val} ->
            Val;
        error when is_atom(Key) ->
            fuzzy_map_get(atom_to_binary(Key, utf8), Options, Default);
        error ->
            Default
    end.

ensure_sni(disable) -> disable;
ensure_sni(undefined) -> undefined;
ensure_sni(L) when is_list(L) -> L;
ensure_sni(B) when is_binary(B) -> unicode:characters_to_list(B, utf8).

ensure_str(undefined) -> undefined;
ensure_str(L) when is_list(L) -> L;
ensure_str(B) when is_binary(B) -> unicode:characters_to_list(B, utf8).

ensure_bin(B) when is_binary(B) -> B;
ensure_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).

ensure_ssl_file_key(_SSL, []) ->
    ok;
ensure_ssl_file_key(SSL, RequiredKeys) ->
    Filter = fun(Key) -> not maps:is_key(Key, SSL) end,
    case lists:filter(Filter, RequiredKeys) of
        [] -> ok;
        Miss -> {error, #{reason => ssl_file_option_not_found, which_options => Miss}}
    end.
