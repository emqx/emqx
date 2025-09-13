%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser).

-include_lib("emqx/include/logger.hrl").
-include("emqx_license.hrl").

-define(PUBKEY, <<
    ""
    "\n"
    "-----BEGIN PUBLIC KEY-----\n"
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGul8oM77eCk9HgOt0mOkBnQHUf0G\n"
    "M8QyDrnMYLOTpP93XcEQlf70nvodmEmHBbDzKTa7DoZMZRRIW3vGCK6zsw==\n"
    "-----END PUBLIC KEY-----\n"
    ""
>>).

-define(LICENSE_PARSE_MODULES, [
    emqx_license_parser_v20220101
]).

-type license_data() :: term().
-type customer_type() ::
    ?SMALL_CUSTOMER
    | ?MEDIUM_CUSTOMER
    | ?LARGE_CUSTOMER
    | ?BUSINESS_CRITICAL_CUSTOMER
    | ?BYOC_CUSTOMER
    | ?EDUCATION_NONPROFIT_CUSTOMER
    | ?EVALUATION_CUSTOMER
    | ?DEVELOPER_CUSTOMER.

-type license_type() :: ?OFFICIAL | ?TRIAL | ?COMMUNITY.

-type license() :: #{
    %% the parser module which parsed the license
    module := module(),
    %% the parse result
    data := license_data(),
    %% the source of the license, e.g. "file://path/to/license/file" or "******" for license key
    source := binary()
}.

-type raw_license() :: string() | binary() | default.

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
    summary/1,
    customer_type/1,
    license_type/1,
    expiry_date/1,
    max_sessions/1,
    max_uptime_seconds/1,
    is_business_critical/1,
    is_single_node/1
]).

%% for testing purpose
-export([
    default/0,
    community/0,
    evaluation/0,
    pubkey/0
]).

%%--------------------------------------------------------------------
%% Behaviour
%%--------------------------------------------------------------------

-callback parse(string() | binary(), binary()) -> {ok, license_data()} | {error, term()}.

-callback dump(license_data()) -> list({atom(), term()}).

%% provide a summary map for logging purposes
-callback summary(license_data()) -> map().

-callback customer_type(license_data()) -> customer_type().

-callback license_type(license_data()) -> license_type().

-callback expiry_date(license_data()) -> calendar:date().

-callback max_sessions(license_data()) -> non_neg_integer().

-callback max_uptime_seconds(license_data()) -> non_neg_integer() | infinity.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

pubkey() -> ?PUBKEY.
evaluation() -> ?DEFAULT_EVALUATION_LICENSE_KEY.
community() -> ?DEFAULT_COMMUNITY_LICENSE_KEY.
-ifdef(TEST).
%% Allow common tests to run without setting license key.
default() -> evaluation().
-else.
default() -> community().
-endif.

%% @doc Parse license key.
%% If the license key is prefixed with "file://path/to/license/file",
%% then the license key is read from the file.
-spec parse(raw_license()) -> {ok, license()} | {error, map()}.
parse(Content) ->
    parse(to_bin(Content), ?MODULE:pubkey()).

parse(<<"default">>, PubKey) ->
    parse(?MODULE:default(), PubKey);
parse(<<"evaluation">>, PubKey) ->
    parse(?MODULE:evaluation(), PubKey);
parse(<<"file://", Path/binary>> = FileKey, PubKey) ->
    case file:read_file(Path) of
        {ok, Content} ->
            case parse(Content, PubKey) of
                {ok, License} ->
                    {ok, License#{source => FileKey}};
                {error, Reason} ->
                    {error, Reason#{
                        license_file => Path
                    }}
            end;
        {error, Reason} ->
            {error, #{
                license_file => Path,
                read_error => Reason
            }}
    end;
parse(Content, PubKey) ->
    [PemEntry] = public_key:pem_decode(PubKey),
    Key = public_key:pem_entry_decode(PemEntry),
    do_parse(iolist_to_binary(Content), Key, ?LICENSE_PARSE_MODULES, []).

-spec dump(license()) -> list({atom(), term()}).
dump(#{module := Module, data := LicenseData}) ->
    Module:dump(LicenseData).

-spec summary(license()) -> map().
summary(#{module := Module, data := Data}) ->
    Module:summary(Data).

-spec customer_type(license()) -> customer_type().
customer_type(#{module := Module, data := LicenseData}) ->
    Module:customer_type(LicenseData).

-spec license_type(license()) -> license_type().
license_type(#{module := Module, data := LicenseData}) ->
    Module:license_type(LicenseData).

-spec expiry_date(license()) -> calendar:date().
expiry_date(#{module := Module, data := LicenseData}) ->
    Module:expiry_date(LicenseData).

-spec max_uptime_seconds(license()) -> non_neg_integer() | infinity.
max_uptime_seconds(#{module := Module, data := LicenseData}) ->
    Module:max_uptime_seconds(LicenseData).

-spec max_sessions(license()) -> non_neg_integer().
max_sessions(#{module := Module, data := LicenseData}) ->
    Module:max_sessions(LicenseData).

-spec is_business_critical(license() | raw_license()) -> boolean().
is_business_critical(#{module := Module, data := LicenseData}) ->
    Module:customer_type(LicenseData) =:= ?BUSINESS_CRITICAL_CUSTOMER;
is_business_critical(Key) when is_binary(Key) ->
    {ok, License} = parse(Key),
    is_business_critical(License).

%% @doc Check if the license is a single node license.
%% currently, community license = single node license.
-spec is_single_node(license()) -> boolean().
is_single_node(License) ->
    license_type(License) =:= ?COMMUNITY.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

do_parse(_Content, _Key, [], Errors) ->
    {error, #{parse_results => lists:reverse(Errors)}};
do_parse(Content, Key, [Module | Modules], Errors) ->
    try Module:parse(Content, Key) of
        {ok, LicenseData} ->
            {ok, #{module => Module, data => LicenseData, source => <<"******">>}};
        {error, Error} ->
            do_parse(Content, Key, Modules, [#{module => Module, error => Error} | Errors])
    catch
        _Class:Error:Stacktrace ->
            do_parse(Content, Key, Modules, [
                #{module => Module, error => Error, stacktrace => Stacktrace} | Errors
            ])
    end.

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) ->
    iolist_to_binary(L).
