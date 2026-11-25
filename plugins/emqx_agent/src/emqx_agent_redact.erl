%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_agent_redact).

%% A vendored copy of newer emqx_utils_redact from 6.3.4. Besides header maps and
%% lists of `{Name, Value}` tuples, it masks and restores headers given as a list
%% of maps with name and value keys
%% (`[#{~"name" => ~"Authorization", ~"value" => ~"secret"}]`).
%% `emqx_utils_redact` in the supported
%% 6.3 brokers does not handle it, so the plugin keeps its own copy.

-export([redact/1, redact/2, redact_headers/1, is_sensitive_key/1, is_redacted/2, is_redacted/3]).
-export([deobfuscate/2]).
-export([redacted_value/0]).

-define(REDACT_VAL, "******").
-define(IS_KEY_HEADERS(K), (K == headers orelse K == <<"headers">> orelse K == "headers")).
-define(IS_KEY_CLIENT_JWKS(K),
    (K == client_jwks orelse K == <<"client_jwks">> orelse K == "client_jwks")
).
-define(IS_VAL_NONE(V), (V == none orelse V == <<"none">> orelse V == "none")).
-define(IS_KEY_CLIENT_JWKS_FILE(K), (K == file orelse K == <<"file">> orelse K == "file")).
-define(HEADER_NAME_BIN, <<"name">>).
-define(HEADER_VALUE_BIN, <<"value">>).
-define(HEADER_NAME_ATOM, name).
-define(HEADER_VALUE_ATOM, value).
-define(HEADER_NAME_STR, "name").
-define(HEADER_VALUE_STR, "value").

redacted_value() -> <<?REDACT_VAL>>.

%% NOTE: keep alphabetical order
is_sensitive_key(access_key_id) -> true;
is_sensitive_key("access_key_id") -> true;
is_sensitive_key(<<"access_key_id">>) -> true;
is_sensitive_key(access_key_secret) -> true;
is_sensitive_key("access_key_secret") -> true;
is_sensitive_key(<<"access_key_secret">>) -> true;
is_sensitive_key(access_token) -> true;
is_sensitive_key("access_token") -> true;
is_sensitive_key(<<"access_token">>) -> true;
is_sensitive_key(account_key) -> true;
is_sensitive_key("account_key") -> true;
is_sensitive_key(<<"account_key">>) -> true;
is_sensitive_key(api_key) -> true;
is_sensitive_key("api_key") -> true;
is_sensitive_key(<<"api_key">>) -> true;
is_sensitive_key(api_secret) -> true;
is_sensitive_key("api_secret") -> true;
is_sensitive_key(<<"api_secret">>) -> true;
is_sensitive_key(aws_secret_access_key) -> true;
is_sensitive_key("aws_secret_access_key") -> true;
is_sensitive_key(<<"aws_secret_access_key">>) -> true;
is_sensitive_key(bind_password) -> true;
is_sensitive_key("bind_password") -> true;
is_sensitive_key(<<"bind_password">>) -> true;
is_sensitive_key(client_jwks) -> true;
is_sensitive_key("client_jwks") -> true;
is_sensitive_key(<<"client_jwks">>) -> true;
is_sensitive_key(client_secret) -> true;
is_sensitive_key("client_secret") -> true;
is_sensitive_key(<<"client_secret">>) -> true;
is_sensitive_key(id_token) -> true;
is_sensitive_key("id_token") -> true;
is_sensitive_key(<<"id_token">>) -> true;
is_sensitive_key(jwk) -> true;
is_sensitive_key("jwk") -> true;
is_sensitive_key(<<"jwk">>) -> true;
is_sensitive_key(jwt) -> true;
is_sensitive_key("jwt") -> true;
is_sensitive_key(<<"jwt">>) -> true;
is_sensitive_key(mfa_token) -> true;
is_sensitive_key("mfa_token") -> true;
is_sensitive_key(<<"mfa_token">>) -> true;
is_sensitive_key(new_pwd) -> true;
is_sensitive_key("new_pwd") -> true;
is_sensitive_key(<<"new_pwd">>) -> true;
is_sensitive_key(old_pwd) -> true;
is_sensitive_key("old_pwd") -> true;
is_sensitive_key(<<"old_pwd">>) -> true;
is_sensitive_key(passcode) -> true;
is_sensitive_key("passcode") -> true;
is_sensitive_key(<<"passcode">>) -> true;
is_sensitive_key(passwd) -> true;
is_sensitive_key("passwd") -> true;
is_sensitive_key(<<"passwd">>) -> true;
is_sensitive_key(password) -> true;
is_sensitive_key("password") -> true;
is_sensitive_key(<<"password">>) -> true;
is_sensitive_key(private_key) -> true;
is_sensitive_key("private_key") -> true;
is_sensitive_key(<<"private_key">>) -> true;
is_sensitive_key(private_key_password) -> true;
is_sensitive_key(<<"private_key_password">>) -> true;
is_sensitive_key(refresh_token) -> true;
is_sensitive_key("refresh_token") -> true;
is_sensitive_key(<<"refresh_token">>) -> true;
is_sensitive_key(secret) -> true;
is_sensitive_key("secret") -> true;
is_sensitive_key(<<"secret">>) -> true;
is_sensitive_key(secret_access_key) -> true;
is_sensitive_key("secret_access_key") -> true;
is_sensitive_key(<<"secret_access_key">>) -> true;
is_sensitive_key(secret_key) -> true;
is_sensitive_key("secret_key") -> true;
is_sensitive_key(<<"secret_key">>) -> true;
is_sensitive_key(security_token) -> true;
is_sensitive_key("security_token") -> true;
is_sensitive_key(<<"security_token">>) -> true;
is_sensitive_key(sentinel_password) -> true;
is_sensitive_key("sentinel_password") -> true;
is_sensitive_key(<<"sentinel_password">>) -> true;
is_sensitive_key(service_account_json) -> true;
is_sensitive_key("service_account_json") -> true;
is_sensitive_key(<<"service_account_json">>) -> true;
is_sensitive_key(setup_token) -> true;
is_sensitive_key("setup_token") -> true;
is_sensitive_key(<<"setup_token">>) -> true;
is_sensitive_key(sp_private_key) -> true;
is_sensitive_key(<<"sp_private_key">>) -> true;
is_sensitive_key(token) -> true;
is_sensitive_key("token") -> true;
is_sensitive_key(<<"token">>) -> true;
is_sensitive_key(totp_code) -> true;
is_sensitive_key("totp_code") -> true;
is_sensitive_key(<<"totp_code">>) -> true;
is_sensitive_key(verify_token) -> true;
is_sensitive_key("verify_token") -> true;
is_sensitive_key(<<"verify_token">>) -> true;
is_sensitive_key(_) -> false.

redact(Term) ->
    do_redact(Term, fun is_sensitive_key/1).

redact(Term, Checker) ->
    do_redact(Term, fun(V) ->
        is_sensitive_key(V) orelse Checker(V)
    end).

redact_headers(Term) ->
    do_redact_headers(Term).

do_redact([], _Checker) ->
    [];
do_redact([X | Xs], Checker) ->
    %% Note: we could be dealing with an improper list
    [do_redact(X, Checker) | do_redact(Xs, Checker)];
do_redact(M, Checker) when is_map(M) ->
    maps:map(
        fun(K, V) ->
            do_redact(K, V, Checker)
        end,
        M
    );
do_redact({Headers, Value}, _Checker) when ?IS_KEY_HEADERS(Headers) ->
    {Headers, do_redact_headers(Value)};
do_redact({Key, Value}, Checker) ->
    case Checker(Key) of
        true ->
            {Key, redact_v(Key, Value)};
        false ->
            {do_redact(Key, Checker), do_redact(Value, Checker)}
    end;
do_redact({jose_jwk, _Keys, _Kty, _Fields}, _Checker) ->
    %% A jose_jwk record carries the raw key material (a kty_oct binary for HMAC,
    %% kty_rsa fields for RSA, etc.) in its inner tuples. Replace it wholesale so
    %% the generic tuple walk below never descends into the key bytes.
    {jose_jwk, '******'};
do_redact(T, Checker) when is_tuple(T) ->
    Elements = erlang:tuple_to_list(T),
    Redact = do_redact(Elements, Checker),
    erlang:list_to_tuple(Redact);
do_redact(Any, _Checker) ->
    Any.

do_redact(Headers, V, _Checker) when ?IS_KEY_HEADERS(Headers) ->
    do_redact_headers(V);
do_redact(K, V, Checker) ->
    case Checker(K) of
        true ->
            redact_v(K, V);
        false ->
            do_redact(V, Checker)
    end.

do_redact_headers(List) when is_list(List) ->
    lists:map(
        fun
            ({K, V} = Pair) ->
                case check_is_sensitive_header(K) of
                    true ->
                        {K, redact_v(V)};
                    _ ->
                        Pair
                end;
            %% A header as a map with name and value keys.
            (#{?HEADER_NAME_BIN := K, ?HEADER_VALUE_BIN := V} = Header) when
                map_size(Header) == 2
            ->
                redact_header_record(Header, ?HEADER_VALUE_BIN, K, V);
            (#{?HEADER_NAME_ATOM := K, ?HEADER_VALUE_ATOM := V} = Header) when
                map_size(Header) == 2
            ->
                redact_header_record(Header, ?HEADER_VALUE_ATOM, K, V);
            (#{?HEADER_NAME_STR := K, ?HEADER_VALUE_STR := V} = Header) when
                map_size(Header) == 2
            ->
                redact_header_record(Header, ?HEADER_VALUE_STR, K, V);
            (Any) ->
                Any
        end,
        List
    );
do_redact_headers(Map) when is_map(Map) ->
    maps:map(
        fun(K, V) ->
            case check_is_sensitive_header(K) of
                true ->
                    redact_v(V);
                _ ->
                    V
            end
        end,
        Map
    );
do_redact_headers(Value) ->
    Value.

redact_header_record(Header, ValueKey, Name, Value) ->
    case check_is_sensitive_header(Name) of
        true -> Header#{ValueKey := redact_v(Value)};
        _ -> Header
    end.

check_is_sensitive_header(Key) ->
    is_sensitive_header(normalize_header_name(Key)).

normalize_header_name(Key) ->
    %% Keep this in sync with emqx_auth_http_utils:transform_header_name/1.
    Key1 =
        try iolist_to_binary(Key) of
            Bin -> Bin
        catch
            _:_ -> Key
        end,
    string:lowercase(emqx_utils_conv:str(Key1)).

find_header(Key, Headers) ->
    case maps:find(Key, Headers) of
        {ok, _} = Found ->
            Found;
        error ->
            NormalizedKey = normalize_header_name(Key),
            MatchingValues = [
                Value
             || {OldKey, Value} <- maps:to_list(Headers),
                normalize_header_name(OldKey) =:= NormalizedKey
            ],
            find_unambiguous_header(MatchingValues)
    end.

find_unambiguous_header([]) ->
    error;
find_unambiguous_header([Value | Values]) ->
    %% Raw connector and bridge configurations may preserve multiple casing
    %% variants. Do not choose one when their stored values disagree.
    case lists:all(fun(V) -> V =:= Value end, Values) of
        true -> {ok, Value};
        false -> error
    end.

is_sensitive_header("authorization") ->
    true;
is_sensitive_header("api-key") ->
    true;
is_sensitive_header("cookie") ->
    true;
is_sensitive_header("proxy-authorization") ->
    true;
is_sensitive_header("x-api-key") ->
    true;
is_sensitive_header("x-auth-token") ->
    true;
is_sensitive_header(_Any) ->
    false.

%% Scoped to the `client_jwks' object: only `file' (the JWKS content or path)
%% holds key material, unlike the blanket `is_sensitive_key/1` check.
is_client_jwks_file_key(K) ->
    ?IS_KEY_CLIENT_JWKS_FILE(K).

%% `client_jwks' is a union of the atom `none' and a JWKS object; only the
%% object holds key material. `none' (the default) must stay readable. A
%% configured object stays a union member instead of collapsing to a bare
%% string: only `file' (the key material) is masked, so the redacted value
%% still passes schema validation on the update path and `deobfuscate/2` can
%% restore it.
redact_v(K, V) when ?IS_KEY_CLIENT_JWKS(K), ?IS_VAL_NONE(V) ->
    V;
redact_v(K, V) when ?IS_KEY_CLIENT_JWKS(K), is_map(V) ->
    maps:map(
        fun(FK, FV) ->
            case is_client_jwks_file_key(FK) of
                true -> redact_v(FV);
                false -> FV
            end
        end,
        V
    );
redact_v(_K, V) ->
    redact_v(V).

redact_v(V) when is_binary(V) ->
    case emqx_placeholder:preproc_tmpl(V) of
        [{var, _}] ->
            V;
        _ ->
            do_redact_v(V)
    end;
redact_v([{str, Bin}]) when is_binary(Bin) ->
    %% The HOCON schema system may generate sensitive values with this format
    [{str, do_redact_v(Bin)}];
redact_v(V) ->
    do_redact_v(V).

do_redact_v(<<"file://", _/binary>> = V) ->
    V;
do_redact_v("file://" ++ _ = V) ->
    V;
do_redact_v(B) when is_binary(B) ->
    <<?REDACT_VAL>>;
do_redact_v(L) when is_list(L) ->
    ?REDACT_VAL;
do_redact_v(F) ->
    try
        %% this can happen in logs
        case emqx_secret:term(F) of
            {file, File} ->
                File;
            V ->
                do_redact_v(V)
        end
    catch
        _:_ ->
            %% most of the time
            ?REDACT_VAL
    end.

deobfuscate_headers(NewHeaders, OldHeaders) ->
    %% Case-insensitive lookup applies only to direct entries of this map.
    %% Nested maps re-enter deobfuscate/3 and retain exact-key matching.
    deobfuscate(
        NewHeaders,
        OldHeaders,
        fun check_is_sensitive_header/1,
        fun find_header/2
    ).

deobfuscate(NewConf, OldConf) ->
    deobfuscate(NewConf, OldConf, fun(_) -> false end).

deobfuscate(NewConf, OldConf, IsSensitiveFun) ->
    deobfuscate(NewConf, OldConf, IsSensitiveFun, fun maps:find/2).

deobfuscate(NewConf, OldConf, IsSensitiveFun, FindOldValue) ->
    maps:fold(
        fun(K, V, Acc) ->
            case FindOldValue(K, OldConf) of
                error when is_list(V), ?IS_KEY_HEADERS(K) ->
                    Acc#{K => deobfuscate_header_list(V, [])};
                error ->
                    case is_redacted(K, V, IsSensitiveFun) of
                        %% don't put redacted value into new config
                        true -> Acc;
                        false -> Acc#{K => V}
                    end;
                {ok, OldV} when is_list(V), is_list(OldV), ?IS_KEY_HEADERS(K) ->
                    Acc#{K => deobfuscate_header_list(V, OldV)};
                {ok, OldV} when is_map(V), is_map(OldV), ?IS_KEY_HEADERS(K) ->
                    Acc#{K => deobfuscate_headers(V, OldV)};
                {ok, OldV} when is_map(V), is_map(OldV), ?IS_KEY_CLIENT_JWKS(K) ->
                    Acc#{K => deobfuscate(V, OldV, fun is_client_jwks_file_key/1)};
                {ok, OldV} when is_map(V), is_map(OldV) ->
                    Acc#{K => deobfuscate(V, OldV, IsSensitiveFun)};
                {ok, OldV} ->
                    case is_redacted(K, V, IsSensitiveFun) of
                        true ->
                            Acc#{K => OldV};
                        _ ->
                            Acc#{K => V}
                    end
            end
        end,
        #{},
        NewConf
    ).

%% Restore the masked values of a header list of maps with name and value keys.
%% A header is looked up by its name in the stored list, the same way as a key
%% of a headers map: an exact name first, then a name that differs only in case.
%% The stored value is used only when all matching stored values are equal. A
%% masked header without a stored value is dropped. Other list elements are kept.
deobfuscate_header_list(Headers, OldHeaders) ->
    lists:flatmap(
        fun
            (#{?HEADER_NAME_BIN := K, ?HEADER_VALUE_BIN := V} = Header) when
                map_size(Header) == 2
            ->
                Keys = {?HEADER_NAME_BIN, ?HEADER_VALUE_BIN},
                deobfuscate_header_record(Header, Keys, K, V, OldHeaders);
            (#{?HEADER_NAME_ATOM := K, ?HEADER_VALUE_ATOM := V} = Header) when
                map_size(Header) == 2
            ->
                Keys = {?HEADER_NAME_ATOM, ?HEADER_VALUE_ATOM},
                deobfuscate_header_record(Header, Keys, K, V, OldHeaders);
            (#{?HEADER_NAME_STR := K, ?HEADER_VALUE_STR := V} = Header) when
                map_size(Header) == 2
            ->
                Keys = {?HEADER_NAME_STR, ?HEADER_VALUE_STR},
                deobfuscate_header_record(Header, Keys, K, V, OldHeaders);
            (Any) ->
                [Any]
        end,
        Headers
    ).

deobfuscate_header_record(Header, {_, ValueKey} = Keys, Name, Value, OldHeaders) ->
    case is_redact_val(Value) andalso check_is_sensitive_header(Name) of
        true ->
            case find_header_in_list(Keys, Name, OldHeaders) of
                {ok, OldValue} -> [Header#{ValueKey := OldValue}];
                error -> []
            end;
        false ->
            [Header]
    end.

%% A stored header is matched only if its name and value keys have the same type
%% as in the submitted header: binary, atom or string.
find_header_in_list({NameKey, ValueKey}, Name, OldHeaders) ->
    Old = [{N, V} || #{NameKey := N, ValueKey := V} = H <- OldHeaders, map_size(H) == 2],
    case [V || {N, V} <- Old, N =:= Name] of
        [] ->
            NormalizedName = normalize_header_name(Name),
            find_unambiguous_header(
                [V || {N, V} <- Old, normalize_header_name(N) =:= NormalizedName]
            );
        Exact ->
            find_unambiguous_header(Exact)
    end.

is_redact_val(?REDACT_VAL) -> true;
is_redact_val(<<?REDACT_VAL>>) -> true;
is_redact_val(_) -> false.

is_redacted(K, V) ->
    do_is_redacted(K, V, fun is_sensitive_key/1).

is_redacted(K, V, Fun) ->
    do_is_redacted(K, V, fun(E) ->
        is_sensitive_key(E) orelse Fun(E)
    end).

do_is_redacted(K, ?REDACT_VAL, Fun) ->
    Fun(K);
do_is_redacted(K, <<?REDACT_VAL>>, Fun) ->
    Fun(K);
do_is_redacted(K, WrappedFun, Fun) when is_function(WrappedFun, 0) ->
    %% wrapped by `emqx_secret' or other module
    do_is_redacted(K, WrappedFun(), Fun);
do_is_redacted(_K, _V, _Fun) ->
    false.
