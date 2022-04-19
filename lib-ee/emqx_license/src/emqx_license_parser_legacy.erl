%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser_legacy).

-behaviour(emqx_license_parser).

-include_lib("public_key/include/public_key.hrl").
-include("emqx_license.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

-define(CACERT, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDVDCCAjwCCQCckt8CVupoRDANBgkqhkiG9w0BAQsFADBsMQswCQYDVQQGEwJD\n"
    "TjERMA8GA1UECAwIWmhlamlhbmcxETAPBgNVBAcMCEhhbmd6aG91MQwwCgYDVQQK\n"
    "DANFTVExDDAKBgNVBAsMA0VNUTEbMBkGA1UEAwwSRU1RWCBFbnRlcnByaXNlIHY1\n"
    "MB4XDTIyMDQwODE1MTA1M1oXDTIzMDQwODE1MTA1M1owbDELMAkGA1UEBhMCQ04x\n"
    "ETAPBgNVBAgMCFpoZWppYW5nMREwDwYDVQQHDAhIYW5nemhvdTEMMAoGA1UECgwD\n"
    "RU1RMQwwCgYDVQQLDANFTVExGzAZBgNVBAMMEkVNUVggRW50ZXJwcmlzZSB2NTCC\n"
    "ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMiYB/gbxCSErWL8sNZHkP4s\n"
    "VTyeBho5T+5Uyp2S95qmcj10FBGi50ZnEN/62vMWED3HzEXsp6pq2Jk+Of3g9rSu\n"
    "63V082HzlqFNHFzUDGkEu23tWyxeEKwBGyYRLIJI1/az99Jq82Qo0UZ5ELVpouAz\n"
    "QVOKjpehHvWgEuWmPi+w1uuOieO08nO4AAOLHWcNOChgV50sl88gbz2n/kAcjqzl\n"
    "1MQXMXoRzfzseNf3bmBV0keNFOpcqePTWCeshFFVkqeKMbK5HIKsnoDSl3VtQ/KK\n"
    "iV88WpW4f0QfGGJV/gHt++4BAZS3nzxXUhGA0Tf2o7N1CHqnXuottJVcgzyIxHEC\n"
    "AwEAATANBgkqhkiG9w0BAQsFAAOCAQEANh3ofOa9Aoqb7gUoTb6dNj883aHZ4aHi\n"
    "kQVo4fVc4IH1MLVNuH/H/aqQ+YtRbbE4YT0icApJFa8qriv8afD9reh5/6ySdsms\n"
    "RAXSogCuAPk2DwT1fyQa6A45x5EBpgwW10rYhwa5JJi6YKPpWS/Uo1Fgk9YGmeW4\n"
    "FgGWYvWQHQIXhjfTC0wJPXlsDB2AB7xMINlOSfg/Bz8mhz7iOjM4pkvnTj17JrgR\n"
    "VQLAj4NFAvdLFFjhZarFtCjPiCE4gb5YZI/Os4iMenD1ZWnYy9Sy7JSNXhWda6e2\n"
    "WGl1AsyDsVPdvAzcB5ymrLnptCzZYT29PSubmCHS9nFgT6hkWCam4g==\n"
    "-----END CERTIFICATE-----"
>>).

%% emqx_license_parser callbacks
-export([
    parse/2,
    dump/1,
    customer_type/1,
    license_type/1,
    expiry_date/1,
    max_connections/1
]).

%%--------------------------------------------------------------------
%% emqx_license_parser API
%%--------------------------------------------------------------------

%% Sample parsed data:
%% #{customer => <<"EMQ X Evaluation">>,
%%   email => "contact@emqx.io",
%%   permits =>
%%       #{customer_type => 10,
%%         enabled_plugins =>
%%             [emqx_backend_redis,emqx_backend_mysql,
%%              emqx_backend_pgsql,emqx_backend_mongo,
%%              emqx_backend_cassa,emqx_bridge_kafka,
%%              emqx_bridge_rabbit],
%%         max_connections => 10,type => 1},
%%   product => "EMQX Enterprise",
%%   validity =>
%%       {{{2020,6,20},{3,2,52}},{{2049,1,1},{3,2,52}}},
%%   vendor => "EMQ Technologies Co., Ltd.",
%%   version => "5.0.0-alpha.1-22e2ad1c"}

parse(Contents, _PublicKey) ->
    case decode_and_verify_signature(Contents) of
        {ok, DerCert} ->
            parse_payload(DerCert);
        {error, Error} ->
            {error, Error}
    end.

dump(#{
    customer := Customer,
    email := Email,
    permits :=
        #{
            customer_type := CustomerType,
            max_connections := MaxConnections,
            type := Type
        },
    validity := {{StartAtDate, _StartAtTime}, {ExpiryAtDate, _ExpiryAtTime}}
}) ->
    {DateNow, _} = calendar:universal_time(),
    Expiry = DateNow > ExpiryAtDate,
    [
        {customer, Customer},
        {email, Email},
        {deployment, "default"},
        {max_connections, MaxConnections},
        {start_at, format_date(StartAtDate)},
        {expiry_at, format_date(ExpiryAtDate)},
        {type, format_type(Type)},
        {customer_type, CustomerType},
        {expiry, Expiry}
    ].

customer_type(#{permits := Permits}) ->
    maps:get(customer_type, Permits, ?LARGE_CUSTOMER).

license_type(#{permits := Permits}) ->
    maps:get(type, Permits, ?TRIAL).

expiry_date(#{validity := {_From, {EndDate, _EndTime}}}) ->
    EndDate.

max_connections(#{permits := Permits}) ->
    maps:get(max_connections, Permits, 0).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

decode_and_verify_signature(Contents) ->
    try
        {ok, Cert, DerCert} = decode_license(Contents),
        [{'Certificate', DerCaCert, _}] = public_key:pem_decode(?CACERT),
        CaCert = public_key:pkix_decode_cert(DerCaCert, otp),
        Result = public_key:pkix_path_validation(
            CaCert,
            [DerCert],
            [{verify_fun, {fun verify_fun/3, user_state}}]
        ),
        case Result of
            {ok, _Info} ->
                {ok, Cert};
            {error, {bad_cert, Reason}} ->
                {error, Reason}
        end
    catch
        throw:bad_license_format ->
            {error, bad_license_format};
        _:_ ->
            {error, bad_certificate}
    end.

decode_license(Contents) ->
    case public_key:pem_decode(Contents) of
        [{'Certificate', DerCert, _}] ->
            Cert = public_key:pkix_decode_cert(DerCert, otp),
            {ok, Cert, DerCert};
        _ ->
            throw(bad_license_format)
    end.

parse_payload(DerCert) ->
    try
        {Start, End} = read_validity(DerCert),
        Subject = read_subject(DerCert),
        Permits = read_permits(DerCert),
        LicenseData = maps:merge(
            #{
                vendor => "EMQ Technologies Co., Ltd.",
                product => emqx_sys:sysdescr(),
                version => emqx_sys:version(),
                validity => {Start, End},
                permits => Permits
            },
            Subject
        ),
        {ok, LicenseData}
    catch
        _:_ ->
            {error, bad_license}
    end.

read_validity(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{validity = Validity}}) ->
    case Validity of
        {'Validity', {utcTime, Start0}, {utcTime, End0}} ->
            {local_time(Start0), local_time(End0)};
        {'Validity', {utcTime, Start0}, {generalTime, End0}} ->
            {local_time(Start0), local_time(End0)}
    end.

local_time([Y01, Y0, Y1, Y2, M1, M2, D1, D2, H1, H2, Min1, Min2, S1, S2, $Z]) ->
    {{b2l(<<Y01, Y0, Y1, Y2>>), b2l(<<M1, M2>>), b2l(<<D1, D2>>)}, {
        b2l(<<H1, H2>>), b2l(<<Min1, Min2>>), b2l(<<S1, S2>>)
    }};
local_time([Y1, Y2, M1, M2, D1, D2, H1, H2, Min1, Min2, S1, S2, $Z]) ->
    {{b2l(<<"20", Y1, Y2>>), b2l(<<M1, M2>>), b2l(<<D1, D2>>)}, {
        b2l(<<H1, H2>>), b2l(<<Min1, Min2>>), b2l(<<S1, S2>>)
    }}.

b2l(L) -> binary_to_integer(L).

read_subject(#'OTPCertificate'{tbsCertificate = TbsCertificate}) ->
    #'OTPTBSCertificate'{subject = {rdnSequence, RDNs}} = TbsCertificate,
    read_subject(lists:flatten(RDNs), #{}).

read_subject([], Subject) ->
    Subject;
read_subject([#'AttributeTypeAndValue'{type = {2, 5, 4, 3}, value = V0} | RDNs], Subject) ->
    V = unwrap_utf8_string(V0),
    read_subject(RDNs, maps:put(customer, V, Subject));
read_subject([#'AttributeTypeAndValue'{type = {2, 5, 4, 10}, value = V0} | RDNs], Subject) ->
    V = unwrap_utf8_string(V0),
    read_subject(RDNs, maps:put(customer, V, Subject));
read_subject(
    [#'AttributeTypeAndValue'{type = {1, 2, 840, 113549, 1, 9, 1}, value = V} | RDNs],
    Subject
) ->
    read_subject(RDNs, maps:put(email, V, Subject));
read_subject([_ | RDNs], Subject) ->
    read_subject(RDNs, Subject).

read_permits(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Extensions}}) ->
    read_permits(Extensions, #{}).

read_permits([], Permits) ->
    Permits;
read_permits(
    [#'Extension'{extnID = {1, 3, 6, 1, 4, 1, 52509, 1}, extnValue = Val} | More], Permits
) ->
    MaxConns = list_to_integer(parse_utf8_string(Val)),
    read_permits(More, maps:put(max_connections, MaxConns, Permits));
read_permits(
    [#'Extension'{extnID = {1, 3, 6, 1, 4, 1, 52509, 2}, extnValue = Val} | More], Permits
) ->
    Plugins = [list_to_atom(Plugin) || Plugin <- string:tokens(parse_utf8_string(Val), ",")],
    read_permits(More, maps:put(enabled_plugins, Plugins, Permits));
read_permits(
    [#'Extension'{extnID = {1, 3, 6, 1, 4, 1, 52509, 3}, extnValue = Val} | More], Permits
) ->
    Type = list_to_integer(parse_utf8_string(Val)),
    read_permits(More, maps:put(type, Type, Permits));
read_permits(
    [#'Extension'{extnID = {1, 3, 6, 1, 4, 1, 52509, 4}, extnValue = Val} | More], Permits
) ->
    CustomerType = list_to_integer(parse_utf8_string(Val)),
    read_permits(More, maps:put(customer_type, CustomerType, Permits));
read_permits([_ | More], Permits) ->
    read_permits(More, Permits).

unwrap_utf8_string({utf8String, Str}) -> Str;
unwrap_utf8_string(Str) -> Str.

parse_utf8_string(Val) ->
    {utf8String, Str} = public_key:der_decode('DisplayText', Val),
    binary_to_list(Str).

format_date({Year, Month, Day}) ->
    iolist_to_binary(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w",
            [Year, Month, Day]
        )
    ).

format_type(?OFFICIAL) -> <<"official">>;
format_type(?TRIAL) -> <<"trial">>.

%% We want to issue new CA certificates with different issuer and keep
%% validating old licenses.
verify_fun(_OTPCertificate, {bad_cert, invalid_issuer}, UserState) ->
    {valid, UserState};
%% We want to continue using the same CA certificate even after it
%% expires.
verify_fun(_OTPCertificate, {bad_cert, cert_expired}, UserState) ->
    {valid, UserState};
verify_fun(OTPCertificate, Event, State) ->
    DefaultVerifyFun = element(1, ?DEFAULT_VERIFYFUN),
    DefaultVerifyFun(OTPCertificate, Event, State).
