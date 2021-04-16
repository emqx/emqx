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

-module(emqx_http_lib).

-export([ uri_encode/1
        , uri_decode/1
        , uri_parse/1
        ]).

-export_type([uri_map/0]).

-type uri_map() :: #{scheme := http | https,
                     host := unicode:chardata(),
                     port := non_neg_integer(),
                     path => unicode:chardata(),
                     query => unicode:chardata(),
                     fragment => unicode:chardata(),
                     userinfo => unicode:chardata()}.

-type hex_uri() :: string() | binary().
-type maybe_hex_uri() :: string() | binary(). %% A possibly hexadecimal encoded URI.
-type uri() :: string() | binary().

%% @doc Decode percent-encoded URI.
%% This is copied from http_uri.erl which has been deprecated since OTP-23
%% The recommended replacement uri_string function is not quite equivalent
%% and not backward compatible.
-spec uri_decode(maybe_hex_uri()) -> uri().
uri_decode(String) when is_list(String) ->
    do_uri_decode(String);
uri_decode(String) when is_binary(String) ->
    do_uri_decode_binary(String).

do_uri_decode([$%,Hex1,Hex2|Rest]) ->
    [hex2dec(Hex1)*16+hex2dec(Hex2)|do_uri_decode(Rest)];
do_uri_decode([First|Rest]) ->
    [First|do_uri_decode(Rest)];
do_uri_decode([]) ->
    [].

do_uri_decode_binary(<<$%, Hex:2/binary, Rest/bits>>) ->
    <<(binary_to_integer(Hex, 16)), (do_uri_decode_binary(Rest))/binary>>;
do_uri_decode_binary(<<First:1/binary, Rest/bits>>) ->
    <<First/binary, (do_uri_decode_binary(Rest))/binary>>;
do_uri_decode_binary(<<>>) ->
    <<>>.

%% @doc Encode URI.
-spec uri_encode(uri()) -> hex_uri().
uri_encode(URI) when is_list(URI) ->
    lists:append([do_uri_encode(Char) || Char <- URI]);
uri_encode(URI) when is_binary(URI) ->
    << <<(do_uri_encode_binary(Char))/binary>> || <<Char>> <= URI >>.

%% @doc Parse URI into a map as uri_string:uri_map(), but with two fields
%% normalised: (1): port number is never 'undefined', default ports are used
%% if missing. (2): scheme is always atom.
-spec uri_parse(string() | binary()) -> {ok, uri_map()} | {error, any()}.
uri_parse(URI) ->
    try
        {ok, do_parse(uri_string:normalize(URI))}
    catch
        throw : Reason ->
            {error, Reason}
    end.

do_parse({error, Reason, Which}) -> throw({Reason, Which});
do_parse(URI) ->
    %% ensure we return string() instead of binary() in uri_map() values.
    Map = uri_string:parse(unicode:characters_to_list(URI)),
    case maps:is_key(scheme, Map) of
        true ->
            normalise_parse_result(Map);
        false ->
            %% missing scheme, add "http://" and try again
            Map2 = uri_string:parse(unicode:characters_to_list(["http://", URI])),
            normalise_parse_result(Map2)
    end.

normalise_parse_result(#{host := _, scheme := Scheme0} = Map) ->
    Scheme = atom_scheme(Scheme0),
    DefaultPort = case https =:= Scheme of
                      true  -> 443;
                      false -> 80
                  end,
    Port = case maps:get(port, Map, undefined) of
               N when is_number(N) -> N;
               _ -> DefaultPort
           end,
    Map#{ scheme => Scheme
        , port => Port
        }.

%% NOTE: so far we only support http schemes.
atom_scheme(Scheme) when is_list(Scheme) -> atom_scheme(list_to_binary(Scheme));
atom_scheme(<<"https">>) -> https;
atom_scheme(<<"http">>) -> http;
atom_scheme(Other) -> throw({unsupported_scheme, Other}).

do_uri_encode(Char) ->
    case reserved(Char) of
	    true ->
	        [ $% | http_util:integer_to_hexlist(Char)];
	    false ->
	        [Char]
    end.

do_uri_encode_binary(Char) ->
    case reserved(Char)  of
        true ->
            << $%, (integer_to_binary(Char, 16))/binary >>;
        false ->
            <<Char>>
    end.

reserved($;) -> true;
reserved($:) -> true;
reserved($@) -> true;
reserved($&) -> true;
reserved($=) -> true;
reserved($+) -> true;
reserved($,) -> true;
reserved($/) -> true;
reserved($?) -> true;
reserved($#) -> true;
reserved($[) -> true;
reserved($]) -> true;
reserved($<) -> true;
reserved($>) -> true;
reserved($\") -> true;
reserved(${) -> true;
reserved($}) -> true;
reserved($|) -> true;
reserved($\\) -> true;
reserved($') -> true;
reserved($^) -> true;
reserved($%) -> true;
reserved($\s) -> true;
reserved(_) -> false.
