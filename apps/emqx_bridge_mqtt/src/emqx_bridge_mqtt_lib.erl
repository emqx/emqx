%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_lib).

-export([clientid_base/1, bytes23/2, bytes23_with_prefix/3]).

%% @doc Make the base ID of client IDs.
%% A base ID is used to concatenate with pool worker ID to build a
%% full ID.
%% In order to avoid client ID clashing when EMQX is clustered,
%% the base ID is the resource name concatenated with
%% broker node name SHA-hash and truncated to 8 hex characters.
clientid_base(Name) ->
    bin([Name, shortener(atom_to_list(node()), 8)]).

%% @doc Limit the number of bytes for client ID under 23 bytes.
%% If ClientID base and suffix concatenated is longer than 23 bytes
%% it hashes the concatenation and replace the non-random suffix.
bytes23(ClientId, SeqNo) ->
    bytes_n(ClientId, SeqNo, 23).

bytes_n(ClientId, SeqNo, N) ->
    Suffix = integer_to_binary(SeqNo),
    Concat = bin([ClientId, $:, Suffix]),
    case size(Concat) =< N of
        true ->
            Concat;
        false ->
            shortener(Concat, N)
    end.

%% @doc Limit the number of bytes for client ID under 23 bytes.
%% If Prefix, ClientID base and suffix concatenated is longer than 23 bytes
%% it hashes the ClientID and SeqNo before appended to the Prefix
bytes23_with_prefix(Prefix, ClientId, SeqNo) when Prefix =/= <<>> ->
    SuffixLen = 23 - size(Prefix),
    true = (SuffixLen > 0),
    bin([Prefix, bytes_n(ClientId, SeqNo, SuffixLen)]).

%% @private SHA hash a string and return the prefix of
%% the given length as hex string in binary format.
shortener(Str, Length) when is_list(Str) ->
    shortener(bin(Str), Length);
shortener(Str, Length) when is_binary(Str) ->
    true = size(Str) > 0,
    true = (Length > 0 andalso Length =< 40),
    Sha = crypto:hash(sha, Str),
    %% TODO: change to binary:encode_hex(X, lowercase) when OTP version is always > 25
    Hex = string:lowercase(binary:encode_hex(Sha)),
    <<UniqueEnough:Length/binary, _/binary>> = Hex,
    UniqueEnough.

bin(IoList) ->
    iolist_to_binary(IoList).
