%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_emqx_base62).

-include_lib("proper/include/proper.hrl").

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_symmetric() ->
    ?FORALL(
        Data,
        raw_data(),
        begin
            Encoded = emqx_base62:encode(Data),
            to_binary(Data) =:= emqx_base62:decode(Encoded)
        end
    ).

prop_size() ->
    ?FORALL(
        Data,
        binary(),
        begin
            Encoded = emqx_base62:encode(Data),
            base62_size(Data, Encoded)
        end
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

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
            RangeStart = DataSize * 8 div 6 + 1,
            RangeEnd = DataSize * 8 div 6 * 2 + 1,
            EncodedSize >= RangeStart andalso EncodedSize =< RangeEnd
    end.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

raw_data() ->
    oneof([string(), binary()]).
