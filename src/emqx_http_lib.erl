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

-export([uri_encode/1, uri_decode/1]).

%% @doc Decode percent-encoded URI.
%% This is copied from http_uri.erl which has been deprecated since OTP-23
%% The recommended replacement uri_string function is not quite equivalent
%% and not backward compatible.
-spec uri_decode(binary()) -> binary().
uri_decode(<<$%, Hex:2/binary, Rest/bits>>) ->
    <<(binary_to_integer(Hex, 16)), (uri_decode(Rest))/binary>>;
uri_decode(<<First:1/binary, Rest/bits>>) ->
    <<First/binary, (uri_decode(Rest))/binary>>;
uri_decode(<<>>) ->
    <<>>.

%% @doc Encode URI.
-spec uri_encode(binary()) -> binary().
uri_encode(URI) when is_binary(URI) ->
    << <<(uri_encode_binary(Char))/binary>> || <<Char>> <= URI >>.

uri_encode_binary(Char) ->
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
