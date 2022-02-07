%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser_v20220101).

-behaviour(emqx_license_parser).

-include_lib("emqx/include/logger.hrl").
-include("emqx_license.hrl").

-define(DIGEST_TYPE, sha256).
-define(LICENSE_VERSION, <<"220111">>).

-export([parse/2,
         dump/1,
         customer_type/1,
         license_type/1,
         expiry_date/1,
         max_connections/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

parse(Content, Key) ->
    [EncodedPayload, EncodedSignature] = binary:split(Content, <<".">>),
    Payload = base64:decode(EncodedPayload),
    Signature = base64:decode(EncodedSignature),
    case verify_signature(Payload, Signature, Key) of
        true -> parse_payload(Payload);
        false -> {error, invalid_signature}
    end.

dump(#{type := Type,
       customer_type := CType,
       customer := Customer,
       email := Email,
       date_start := DateStart,
       max_connections := MaxConns} = License) ->

    DateExpiry = expiry_date(License),
    {DateNow, _} = calendar:universal_time(),
    Expiry = DateNow > DateExpiry,

    [{customer, Customer},
     {email, Email},
     {max_connections, MaxConns},
     {start_at, format_date(DateStart)},
     {expiry_at, format_date(DateExpiry)},
     {type, format_type(Type)},
     {customer_type, CType},
     {expiry, Expiry}].

customer_type(#{customer_type := CType}) -> CType.

license_type(#{type := Type}) -> Type.

expiry_date(#{date_start := DateStart, days := Days}) ->
    calendar:gregorian_days_to_date(
     calendar:date_to_gregorian_days(DateStart) + Days).

max_connections(#{max_connections := MaxConns}) ->
    MaxConns.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

verify_signature(Payload, Signature, Key) ->
    RSAPublicKey = public_key:der_decode('RSAPublicKey', Key),
    public_key:verify(Payload, ?DIGEST_TYPE, Signature, RSAPublicKey).

parse_payload(Payload) ->
    Lines = lists:map(
              fun string:trim/1,
              string:split(string:trim(Payload), <<"\n">>, all)),
    case Lines of
        [?LICENSE_VERSION, Type, CType, Customer, Email, DateStart, Days, MaxConns] ->
            collect_fields([{type, parse_type(Type)},
                            {customer_type, parse_customer_type(CType)},
                            {customer, {ok, Customer}},
                            {email, {ok, Email}},
                            {date_start, parse_date_start(DateStart)},
                            {days, parse_days(Days)},
                            {max_connections, parse_max_connections(MaxConns)}]);
        [_Version, _Type, _CType, _Customer, _Email, _DateStart, _Days, _MaxConns] ->
            {error, invalid_version};
        _ ->
            {error, invalid_field_number}
    end.

parse_type(TypeStr) ->
    case string:to_integer(TypeStr) of
        {Type, <<"">>} -> {ok, Type};
        _ -> {error, invalid_license_type}
    end.

parse_customer_type(CTypeStr) ->
    case string:to_integer(CTypeStr) of
        {CType, <<"">>} -> {ok, CType};
        _ -> {error, invalid_customer_type}
    end.

parse_date_start(<<Y:4/binary, M:2/binary, D:2/binary>>) ->
    Date = list_to_tuple([N || {N, <<>>} <- [string:to_integer(S) || S <- [Y, M, D]]]),
    case calendar:valid_date(Date) of
        true -> {ok, Date};
        false -> {error, invalid_date}
    end;
parse_date_start(_) -> {error, invalid_date}.

parse_days(DaysStr) ->
    case string:to_integer(DaysStr) of
        {Days, <<"">>} when Days > 0 -> {ok, Days};
        _ -> {error, invalid_int_value}
    end.

parse_max_connections(MaxConnStr) ->
    case string:to_integer(MaxConnStr) of
        {MaxConns, <<"">>} when MaxConns > 0 -> {ok, MaxConns};
        _ -> {error, invalid_int_value}
    end.

collect_fields(Fields) ->
    Collected = lists:foldl(
                  fun({Name, {ok, Value}}, {FieldValues, Errors}) ->
                          {[{Name, Value} | FieldValues], Errors};
                     ({Name, {error, Reason}}, {FieldValues, Errors}) ->
                          {FieldValues, [{Name, Reason} | Errors]}
                  end,
                  {[], []},
                  Fields),
    case Collected of
        {FieldValues, []} ->
            {ok, maps:from_list(FieldValues)};
        {_, Errors} ->
            {error, lists:reverse(Errors)}
    end.

format_date({Year, Month, Day}) ->
    iolist_to_binary(
      io_lib:format(
        "~4..0w-~2..0w-~2..0w",
        [Year, Month, Day])).

format_type(?OFFICIAL) -> <<"official">>;
format_type(?TRIAL) -> <<"trial">>.
