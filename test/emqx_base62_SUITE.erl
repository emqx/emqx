%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_base62_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_proper_base62(_) ->
    Opts = [{numtests, 100}, {to_file, user}],
    ?assert(proper:quickcheck(prop_symmetric(), Opts)),
    ?assert(proper:quickcheck(prop_size(), Opts)).

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%
prop_symmetric() ->
    ?FORALL(Data, raw_data(),
        begin
            Encoded = emqx_base62:encode(Data),
            to_binary(Data) =:= emqx_base62:decode(Encoded)
        end).

prop_size() ->
    ?FORALL(Data, binary(),
         begin
             Encoded = emqx_base62:encode(Data),
             base62_size(Data, Encoded)
         end).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%
to_binary(Data) when is_list(Data) ->
    unicode:characters_to_binary(Data);
to_binary(Data) when is_integer(Data) ->
    integer_to_binary(Data);
to_binary(Data) when is_binary(Data) ->
    Data.

base62_size(Data, Encoded) ->
    DataSize = erlang:size(Data),
    EncodedSize = erlang:size(Encoded),
    case (DataSize * 8 rem 6) of
        0 ->
            %% Due to the particularity of base 62, 3 bytes data maybe encoded
            %% as 4 bytes data or 5 bytes data, the encode size maybe in the
            %% range between DataSize*4/3 and DataSize*8/3
            RangeStart = DataSize div 3 * 4,
            RangeEnd = DataSize div 3 * 8,
            EncodedSize >= RangeStart andalso EncodedSize =< RangeEnd;
        _Rem ->
            RangeStart = DataSize * 8 div 6  + 1,
            RangeEnd = DataSize * 8 div 6 * 2 + 1,
            EncodedSize >= RangeStart andalso EncodedSize =< RangeEnd
    end.

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%
raw_data() -> oneof([integer(), string(), binary()]).
