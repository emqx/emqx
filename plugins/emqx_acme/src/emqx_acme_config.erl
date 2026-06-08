-module(emqx_acme_config).

-include("emqx_acme.hrl").

-export([
    load/0,
    update/1,
    settings/0,
    name_vsn/0,
    parse/1,
    resolve_file_uri/1
]).

-define(SETTINGS_KEY, {?MODULE, settings}).

name_vsn() ->
    "emqx_acme-" ++ version().

version() ->
    element(2, application:get_key(emqx_acme, vsn)).

load() ->
    NameVsn = name_vsn(),
    Raw = emqx_plugins:get_config(NameVsn, #{}),
    update(Raw).

update(RawConfig) ->
    case parse(RawConfig) of
        {ok, Settings} ->
            persistent_term:put(?SETTINGS_KEY, Settings),
            ok;
        {error, _} = Error ->
            Error
    end.

settings() ->
    persistent_term:get(?SETTINGS_KEY, default_settings()).

default_settings() ->
    #{
        dir_url => ?DEFAULT_DIR_URL,
        domains => [],
        contact => [],
        cert_bundle_name => ?DEFAULT_CERT_BUNDLE_NAME,
        listener_ids => [],
        enable_dashboard_https => true,
        dashboard_https_port => ?DEFAULT_DASHBOARD_HTTPS_PORT,
        cert_type => ec,
        challenge_port => ?DEFAULT_CHALLENGE_PORT,
        renew_before_expiry_days => ?DEFAULT_RENEW_BEFORE_DAYS,
        check_interval_hours => ?DEFAULT_CHECK_INTERVAL_HOURS,
        acc_key => undefined,
        acc_key_password => undefined
    }.

parse(RawConfig) ->
    maybe
        {ok, DirUrl} ?= parse_dir_url(RawConfig),
        {ok, Domains} ?= parse_domains(RawConfig),
        {ok, Contact} ?= parse_contact(RawConfig),
        {ok, CertBundleName} ?= parse_cert_bundle_name(RawConfig),
        {ok, ListenerIds} ?= parse_listener_ids(RawConfig),
        {ok, EnableDashHttps} ?= parse_bool(<<"enable_dashboard_https">>, RawConfig, true),
        {ok, DashHttpsPort} ?= parse_dashboard_https_port(RawConfig),
        {ok, CertType} ?= parse_cert_type(RawConfig),
        {ok, ChallengePort} ?= parse_challenge_port(RawConfig),
        {ok, RenewDays} ?= parse_renew_days(RawConfig),
        {ok, CheckHours} ?= parse_check_hours(RawConfig),
        {ok, AccKey} ?= parse_file_uri(<<"acc_key">>, RawConfig, undefined),
        {ok, AccKeyPassword} ?= parse_file_uri(<<"acc_key_password">>, RawConfig, undefined),
        {ok, #{
            dir_url => DirUrl,
            domains => Domains,
            contact => Contact,
            cert_bundle_name => CertBundleName,
            listener_ids => ListenerIds,
            enable_dashboard_https => EnableDashHttps,
            dashboard_https_port => DashHttpsPort,
            cert_type => CertType,
            challenge_port => ChallengePort,
            renew_before_expiry_days => RenewDays,
            check_interval_hours => CheckHours,
            acc_key => AccKey,
            acc_key_password => AccKeyPassword
        }}
    else
        {error, Reason} -> {error, Reason}
    end.

parse_dir_url(Config) ->
    {ok, to_str(get_val(<<"dir_url">>, Config, ?DEFAULT_DIR_URL))}.

parse_domains(Config) ->
    maybe
        {ok, Domains} ?= parse_csv_list(<<"domains">>, Config, invalid_domains),
        validate_domains(Domains)
    end.

parse_contact(Config) ->
    parse_csv_list(<<"contact">>, Config, invalid_contact).

%% Quick sanity check on each domain entry. Deliberately permissive
%% (no full RFC 1035 enforcement, no IDN normalisation) — only the
%% characters clearly not part of a hostname (`@`, whitespace, `/`,
%% `\`, control codes) are rejected. Full label/IDN validation stays
%% in idna at issuance time.
validate_domains(Domains) ->
    case lists:dropwhile(fun is_plausible_domain/1, Domains) of
        [] -> {ok, Domains};
        [Bad | _] -> {error, {invalid_domain, Bad}}
    end.

is_plausible_domain(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    not has_disallowed_char(Bin);
is_plausible_domain(_) ->
    false.

has_disallowed_char(<<>>) ->
    false;
has_disallowed_char(<<C, _/binary>>) when
    C =:= $@; C =:= $/; C =:= $\\; C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r; C < 32
->
    true;
has_disallowed_char(<<_, Rest/binary>>) ->
    has_disallowed_char(Rest).

%% domains and contact are exposed in the dashboard as a single
%% comma-separated string field (input-array was hard for operators to
%% use). Internally we still keep a list of binaries because that is what
%% acme_client_issuance expects. Empty entries (e.g. from a trailing
%% comma) are dropped and surrounding whitespace is trimmed so
%% "a.example.com, b.example.com" works the same as
%% "a.example.com,b.example.com".
parse_csv_list(Key, Config, ErrorTag) ->
    case get_val(Key, Config, <<>>) of
        undefined ->
            {ok, []};
        null ->
            {ok, []};
        Bin when is_binary(Bin) -> {ok, split_csv_bin(Bin)};
        List when is_list(List) ->
            case is_charlist(List) of
                true ->
                    {ok, split_csv_bin(list_to_binary(List))};
                false ->
                    %% Already a list of binaries/strings (e.g. set from
                    %% emqx_acme_config:update/1 with the pre-CSV shape).
                    {ok, [to_bin(X) || X <- List, X =/= <<>>, X =/= ""]}
            end;
        Other ->
            {error, {ErrorTag, Other}}
    end.

split_csv_bin(Bin) ->
    Trimmed = [string:trim(P) || P <- binary:split(Bin, <<",">>, [global])],
    [P || P <- Trimmed, P =/= <<>>].

is_charlist([]) -> true;
is_charlist([H | T]) when is_integer(H), H >= 0, H =< 16#10FFFF -> is_charlist(T);
is_charlist(_) -> false.

parse_cert_bundle_name(Config) ->
    {ok, to_bin(get_val(<<"cert_bundle_name">>, Config, ?DEFAULT_CERT_BUNDLE_NAME))}.

%% Dashboard exposes listener_ids as a comma-separated string (input-array
%% was awkward to use). Each entry is "ssl:<name>" or "wss:<name>" and is
%% parsed to {Type, NameAtom}. Listeners that don't yet exist are accepted
%% here; runtime migration silently skips them. QUIC, TCP, and WS are
%% rejected at parse time. The dashboard's HTTPS listener is configured
%% separately via enable_dashboard_https. Back-compat: an older
%% list-of-binaries shape (from configs persisted before the CSV
%% migration, or set via emqx_acme_config:update/1) is still accepted.
parse_listener_ids(Config) ->
    case get_val(<<"listener_ids">>, Config, <<>>) of
        undefined ->
            {ok, []};
        null ->
            {ok, []};
        Bin when is_binary(Bin) ->
            parse_listener_ids_each(split_csv_bin(Bin), []);
        List when is_list(List) ->
            case is_charlist(List) of
                true ->
                    parse_listener_ids_each(
                        split_csv_bin(list_to_binary(List)), []
                    );
                false ->
                    parse_listener_ids_each(
                        [X || X <- List, X =/= <<>>, X =/= ""], []
                    )
            end;
        Other ->
            {error, {invalid_listener_ids, Other}}
    end.

parse_listener_ids_each([], Acc) ->
    {ok, lists:reverse(Acc)};
parse_listener_ids_each([Entry | Rest], Acc) ->
    case parse_one_listener_id(Entry) of
        {ok, Parsed} -> parse_listener_ids_each(Rest, [Parsed | Acc]);
        {error, _} = Error -> Error
    end.

parse_one_listener_id(Entry) ->
    EntryBin = to_bin(Entry),
    case binary:split(EntryBin, <<":">>) of
        [TypeBin, NameBin] when NameBin =/= <<>> ->
            case TypeBin of
                <<"ssl">> -> {ok, {ssl, binary_to_atom(NameBin, utf8)}};
                <<"wss">> -> {ok, {wss, binary_to_atom(NameBin, utf8)}};
                Other -> {error, {invalid_listener_type, Other}}
            end;
        _ ->
            {error, {invalid_listener_id, EntryBin}}
    end.

parse_bool(Key, Config, Default) ->
    case get_val(Key, Config, Default) of
        true -> {ok, true};
        false -> {ok, false};
        null -> {ok, Default};
        undefined -> {ok, Default};
        Other -> {error, {invalid_bool, Key, Other}}
    end.

parse_cert_type(Config) ->
    case to_bin(get_val(<<"cert_type">>, Config, <<"ec">>)) of
        <<"ec">> -> {ok, ec};
        <<"rsa">> -> {ok, rsa};
        Other -> {error, {invalid_cert_type, Other}}
    end.

parse_challenge_port(Config) ->
    {ok, to_pos_int(get_val(<<"challenge_port">>, Config, ?DEFAULT_CHALLENGE_PORT))}.

parse_dashboard_https_port(Config) ->
    {ok,
        to_pos_int(
            get_val(<<"dashboard_https_port">>, Config, ?DEFAULT_DASHBOARD_HTTPS_PORT)
        )}.

parse_renew_days(Config) ->
    {ok, to_pos_int(get_val(<<"renew_before_expiry_days">>, Config, ?DEFAULT_RENEW_BEFORE_DAYS))}.

parse_check_hours(Config) ->
    {ok, to_pos_int(get_val(<<"check_interval_hours">>, Config, ?DEFAULT_CHECK_INTERVAL_HOURS))}.

%% Parse a file:// URI from raw config. Returns {ok, Default} when the key
%% is absent or empty (so a cleared dashboard field falls back to the
%% plugin default); {ok, "file:///abs/path"} when set to a valid file URI;
%% {error, _} when set to a non-empty value that doesn't start with
%% "file://" (we accept only that scheme so a future addition of inline
%% contents or a different scheme is unambiguous). Env-var interpolation
%% (${EMQX_ETC_DIR} / ${EMQX_LOG_DIR} / $FOO) is deferred to use sites via
%% resolve_file_uri/1.
parse_file_uri(Key, Config, Default) ->
    case get_val(Key, Config, undefined) of
        undefined ->
            {ok, Default};
        null ->
            {ok, Default};
        <<>> ->
            {ok, Default};
        "" ->
            {ok, Default};
        Val ->
            Str = to_str(Val),
            case Str of
                "file://" ++ _ -> {ok, Str};
                _ -> {error, {invalid_file_uri, Key, to_bin(Val)}}
            end
    end.

%% Resolve env vars in the path portion of a file:// URI. Called by use
%% sites (e.g. emqx_acme_issuer:ensure_acc_key_file/2) just before opening
%% the file or handing the path to acme-erlang-client. EMQX exports
%% EMQX_ETC_DIR and EMQX_LOG_DIR at startup; emqx_utils_schema also
%% provides sensible fallbacks for those two when running under CT without
%% bin/emqx. Other ${VAR} are looked up via os:getenv/1.
-spec resolve_file_uri(undefined | string()) -> undefined | string().
resolve_file_uri(undefined) ->
    undefined;
resolve_file_uri("file://" ++ Path) ->
    "file://" ++ emqx_utils_schema:naive_env_interpolation(Path).

%% Helpers

get_val(Key, Map, Default) when is_map(Map) ->
    maps:get(Key, Map, Default);
get_val(_Key, _NotMap, Default) ->
    Default.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_integer(V) -> integer_to_binary(V).

to_str(V) when is_list(V) -> V;
to_str(V) when is_binary(V) -> binary_to_list(V);
to_str(V) when is_atom(V) -> atom_to_list(V).

to_pos_int(V) when is_integer(V), V > 0 -> V;
to_pos_int(V) when is_binary(V) -> to_pos_int(binary_to_integer(V));
to_pos_int(V) when is_list(V) -> to_pos_int(list_to_integer(V));
to_pos_int(_) -> 1.
