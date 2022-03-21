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
    default_versions/0,
    integral_versions/1,
    default_ciphers/0,
    selected_ciphers/1,
    integral_ciphers/2,
    drop_tls13_for_old_otp/1,
    all_ciphers/0
]).

%% SSL files
-export([
    ensure_ssl_files/2,
    delete_ssl_files/3,
    file_content_as_options/1
]).

-include("logger.hrl").

-define(IS_TRUE(Val), ((Val =:= true) or (Val =:= <<"true">>))).
-define(IS_FALSE(Val), ((Val =:= false) or (Val =:= <<"false">>))).

-define(SSL_FILE_OPT_NAMES, [<<"keyfile">>, <<"certfile">>, <<"cacertfile">>]).

%% non-empty string
-define(IS_STRING(L), (is_list(L) andalso L =/= [] andalso is_integer(hd(L)))).
%% non-empty list of strings
-define(IS_STRING_LIST(L), (is_list(L) andalso L =/= [] andalso ?IS_STRING(hd(L)))).

%% @doc Returns the default supported tls versions.
-spec default_versions() -> [atom()].
default_versions() -> available_versions().

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
    Available = available_versions(),
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

%% @doc Return a list of all supported ciphers.
all_ciphers() -> all_ciphers(default_versions()).

%% @doc Return a list of (openssl string format) cipher suites.
-spec all_ciphers([ssl:tls_version()]) -> [string()].
all_ciphers(['tlsv1.3']) ->
    %% When it's only tlsv1.3 wanted, use 'exclusive' here
    %% because 'all' returns legacy cipher suites too,
    %% which does not make sense since tlsv1.3 can not use
    %% legacy cipher suites.
    ssl:cipher_suites(exclusive, 'tlsv1.3', openssl);
all_ciphers(Versions) ->
    %% assert non-empty
    [_ | _] = dedup(lists:append([ssl:cipher_suites(all, V, openssl) || V <- Versions])).

%% @doc All Pre-selected TLS ciphers.
default_ciphers() ->
    selected_ciphers(available_versions()).

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
        true -> ssl:cipher_suites(exclusive, 'tlsv1.3', openssl);
        false -> []
    end ++ do_selected_ciphers('tlsv1.2');
do_selected_ciphers(_) ->
    [
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
    ].

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

%% default ssl versions based on available versions.
-spec available_versions() -> [atom()].
available_versions() ->
    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    default_versions(OtpRelease).

%% tlsv1.3 is available from OTP-22 but we do not want to use until 23.
default_versions(OtpRelease) when OtpRelease >= 23 ->
    proplists:get_value(available, ssl:versions());
default_versions(_) ->
    lists:delete('tlsv1.3', proplists:get_value(available, ssl:versions())).

%% Deduplicate a list without re-ordering the elements.
dedup([]) -> [];
dedup([H | T]) -> [H | dedup([I || I <- T, I =/= H])].

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
-define(TLSV13_EXCLUSIVE_CIPHERS, [
    "TLS_AES_256_GCM_SHA384",
    "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_CCM_SHA256",
    "TLS_AES_128_CCM_8_SHA256"
]).
drop_tls13(SslOpts0) ->
    SslOpts1 =
        case maps:find(versions, SslOpts0) of
            error -> SslOpts0;
            {ok, Vsns} -> SslOpts0#{versions => (Vsns -- ['tlsv1.3'])}
        end,
    case maps:find(ciphers, SslOpts1) of
        error -> SslOpts1;
        {ok, Ciphers} -> SslOpts1#{ciphers => Ciphers -- ?TLSV13_EXCLUSIVE_CIPHERS}
    end.

%% @doc The input map is a HOCON decoded result of a struct defined as
%% emqx_schema:server_ssl_opts_schema. (NOTE: before schema-checked).
%% `keyfile', `certfile' and `cacertfile' can be either pem format key or certificates,
%% or file path.
%% When PEM format key or certificate is given, it tries to to save them in the given
%% sub-dir in emqx's data_dir, and replace saved file paths for SSL options.
-spec ensure_ssl_files(file:name_all(), undefined | map()) ->
    {ok, undefined | map()} | {error, map()}.
ensure_ssl_files(Dir, Opts) ->
    ensure_ssl_files(Dir, Opts, _DryRun = false).

ensure_ssl_files(_Dir, undefined, _DryRun) ->
    {ok, undefined};
ensure_ssl_files(_Dir, #{<<"enable">> := False} = Opts, _DryRun) when ?IS_FALSE(False) ->
    {ok, Opts};
ensure_ssl_files(Dir, Opts, DryRun) ->
    ensure_ssl_files(Dir, Opts, ?SSL_FILE_OPT_NAMES, DryRun).

ensure_ssl_files(_Dir, Opts, [], _DryRun) ->
    {ok, Opts};
ensure_ssl_files(Dir, Opts, [Key | Keys], DryRun) ->
    case ensure_ssl_file(Dir, Key, Opts, maps:get(Key, Opts, undefined), DryRun) of
        {ok, NewOpts} ->
            ensure_ssl_files(Dir, NewOpts, Keys, DryRun);
        {error, Reason} ->
            {error, Reason#{which_option => Key}}
    end.

%% @doc Compare old and new config, delete the ones in old but not in new.
-spec delete_ssl_files(file:name_all(), undefined | map(), undefined | map()) -> ok.
delete_ssl_files(Dir, NewOpts0, OldOpts0) ->
    DryRun = true,
    {ok, NewOpts} = ensure_ssl_files(Dir, NewOpts0, DryRun),
    {ok, OldOpts} = ensure_ssl_files(Dir, OldOpts0, DryRun),
    Get = fun
        (_K, undefined) -> undefined;
        (K, Opts) -> maps:get(K, Opts, undefined)
    end,
    lists:foreach(
        fun(Key) -> delete_old_file(Get(Key, NewOpts), Get(Key, OldOpts)) end,
        ?SSL_FILE_OPT_NAMES
    ).

delete_old_file(New, Old) when New =:= Old -> ok;
delete_old_file(_New, _Old = undefined) ->
    ok;
delete_old_file(_New, Old) ->
    case filelib:is_regular(Old) andalso file:delete(Old) of
        ok ->
            ok;
        %% already deleted
        false ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_delete_ssl_file", file_path => Old, reason => Reason})
    end.

ensure_ssl_file(_Dir, _Key, Opts, undefined, _DryRun) ->
    {ok, Opts};
ensure_ssl_file(Dir, Key, Opts, MaybePem, DryRun) ->
    case is_valid_string(MaybePem) of
        true ->
            do_ensure_ssl_file(Dir, Key, Opts, MaybePem, DryRun);
        false ->
            {error, #{reason => invalid_file_path_or_pem_string}}
    end.

do_ensure_ssl_file(Dir, Key, Opts, MaybePem, DryRun) ->
    case is_pem(MaybePem) of
        true ->
            case save_pem_file(Dir, Key, MaybePem, DryRun) of
                {ok, Path} -> {ok, Opts#{Key => Path}};
                {error, Reason} -> {error, Reason}
            end;
        false ->
            case is_valid_pem_file(MaybePem) of
                true ->
                    {ok, Opts};
                {error, enoent} when DryRun -> {ok, Opts};
                {error, Reason} ->
                    {error, #{
                        pem_check => invalid_pem,
                        file_read => Reason
                    }}
            end
    end.

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
pem_file_name(Dir, Key, Pem) ->
    <<CK:8/binary, _/binary>> = crypto:hash(md5, Pem),
    Suffix = hex_str(CK),
    FileName = binary:replace(Key, <<"file">>, <<"-", Suffix/binary>>),
    filename:join([emqx:certs_dir(), Dir, FileName]).

hex_str(Bin) ->
    iolist_to_binary([io_lib:format("~2.16.0b", [X]) || <<X:8>> <= Bin]).

is_valid_pem_file(Path) ->
    case file:read_file(Path) of
        {ok, Pem} -> is_pem(Pem) orelse {error, not_pem};
        {error, Reason} -> {error, Reason}
    end.

%% @doc This is to return SSL file content in management APIs.
file_content_as_options(undefined) ->
    undefined;
file_content_as_options(#{<<"enable">> := False} = SSL) when ?IS_FALSE(False) ->
    {ok, maps:without(?SSL_FILE_OPT_NAMES, SSL)};
file_content_as_options(#{<<"enable">> := True} = SSL) when ?IS_TRUE(True) ->
    file_content_as_options(?SSL_FILE_OPT_NAMES, SSL).

file_content_as_options([], SSL) ->
    {ok, SSL};
file_content_as_options([Key | Keys], SSL) ->
    case maps:get(Key, SSL, undefined) of
        undefined ->
            file_content_as_options(Keys, SSL);
        Path ->
            case file:read_file(Path) of
                {ok, Bin} ->
                    file_content_as_options(Keys, SSL#{Key => Bin});
                {error, Reason} ->
                    {error, #{
                        file_path => Path,
                        reason => Reason
                    }}
            end
    end.

-if(?OTP_RELEASE > 22).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

drop_tls13_test() ->
    Versions = default_versions(),
    ?assert(lists:member('tlsv1.3', Versions)),
    Ciphers = all_ciphers(),
    ?assert(has_tlsv13_cipher(Ciphers)),
    Opts0 = #{versions => Versions, ciphers => Ciphers, other => true},
    Opts = drop_tls13(Opts0),
    ?assertNot(lists:member('tlsv1.3', maps:get(versions, Opts, undefined))),
    ?assertNot(has_tlsv13_cipher(maps:get(ciphers, Opts, undefined))).

drop_tls13_no_versions_cipers_test() ->
    Opts0 = #{other => 0, bool => true},
    Opts = drop_tls13(Opts0),
    ?_assertEqual(Opts0, Opts).

has_tlsv13_cipher(Ciphers) ->
    lists:any(fun(C) -> lists:member(C, Ciphers) end, ?TLSV13_EXCLUSIVE_CIPHERS).

-endif.
-endif.
