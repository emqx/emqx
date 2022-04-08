%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQX License Management.
%%--------------------------------------------------------------------

-module(emqx_license_parser).

-include_lib("emqx/include/logger.hrl").
-include("emqx_license.hrl").

-define(PUBKEY, <<
    ""
    "\n"
    "-----BEGIN PUBLIC KEY-----\n"
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEbtkdos3TZmSv+D7+X5pc0yfcjum2\n"
    "Q1DK6PCWkiQihjvjJjKFzdYzcWOgC6f4Ou3mgGAUSjdQYYnFKZ/9f5ax4g==\n"
    "-----END PUBLIC KEY-----\n"
    ""
>>).

-define(LICENSE_PARSE_MODULES, [
    emqx_license_parser_v20220101,
    emqx_license_parser_legacy
]).

-type license_data() :: term().
-type customer_type() ::
    ?SMALL_CUSTOMER
    | ?MEDIUM_CUSTOMER
    | ?LARGE_CUSTOMER
    | ?EVALUATION_CUSTOMER.

-type license_type() :: ?OFFICIAL | ?TRIAL.

-type license() :: #{module := module(), data := license_data()}.

-export_type([
    license_data/0,
    customer_type/0,
    license_type/0,
    license/0
]).

-export([
    parse/1,
    parse/2,
    dump/1,
    customer_type/1,
    license_type/1,
    expiry_date/1,
    max_connections/1
]).

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

-callback parse(string() | binary(), binary()) -> {ok, license_data()} | {error, term()}.

-callback dump(license_data()) -> list({atom(), term()}).

-callback customer_type(license_data()) -> customer_type().

-callback license_type(license_data()) -> license_type().

-callback expiry_date(license_data()) -> calendar:date().

-callback max_connections(license_data()) -> non_neg_integer().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec parse(string() | binary()) -> {ok, license()} | {error, term()}.
parse(Content) ->
    parse(Content, ?PUBKEY).

parse(Content, Pem) ->
    [PemEntry] = public_key:pem_decode(Pem),
    Key = public_key:pem_entry_decode(PemEntry),
    do_parse(iolist_to_binary(Content), Key, ?LICENSE_PARSE_MODULES, []).

-spec dump(license()) -> list({atom(), term()}).
dump(#{module := Module, data := LicenseData}) ->
    Module:dump(LicenseData).

-spec customer_type(license()) -> customer_type().
customer_type(#{module := Module, data := LicenseData}) ->
    Module:customer_type(LicenseData).

-spec license_type(license()) -> license_type().
license_type(#{module := Module, data := LicenseData}) ->
    Module:license_type(LicenseData).

-spec expiry_date(license()) -> calendar:date().
expiry_date(#{module := Module, data := LicenseData}) ->
    Module:expiry_date(LicenseData).

-spec max_connections(license()) -> non_neg_integer().
max_connections(#{module := Module, data := LicenseData}) ->
    Module:max_connections(LicenseData).

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

do_parse(_Content, _Key, [], Errors) ->
    {error, lists:reverse(Errors)};
do_parse(Content, Key, [Module | Modules], Errors) ->
    try Module:parse(Content, Key) of
        {ok, LicenseData} ->
            {ok, #{module => Module, data => LicenseData}};
        {error, Error} ->
            do_parse(Content, Key, Modules, [{Module, Error} | Errors])
    catch
        _Class:Error:Stacktrace ->
            do_parse(Content, Key, Modules, [{Module, {Error, Stacktrace}} | Errors])
    end.
