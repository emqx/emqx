%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_bif).

%% API
-export([
    json_decode/1,
    json_encode/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

json_encode(X) ->
    case emqx_utils_json:safe_encode(X) of
        {ok, JSON} ->
            JSON;
        {error, Reason} ->
            throw(#{reason => json_encode_failure, detail => Reason})
    end.

json_decode(JSON) ->
    case emqx_utils_json:safe_decode(JSON, [return_maps]) of
        {ok, X} ->
            X;
        {error, Reason} ->
            throw(#{reason => json_decode_failure, detail => Reason})
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
