%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_cursor).
-moduledoc """
Encode and decode the opaque pagination cursors that the REST API
returns to the client and receives back from it.

The cursor is a JSON object in URL-safe base64 without padding. Use it when
the cursor holds only plain data (integers, strings). The caller validates
the decoded map.
""".

-export([
    encode_json/1,
    decode_json/1
]).

-type cursor() :: binary().

-export_type([cursor/0]).

%%--------------------------------------------------------------------
%% JSON cursor
%%--------------------------------------------------------------------

-doc "Encode a JSON object as a cursor.".
-spec encode_json(map()) -> cursor().
encode_json(Map) when is_map(Map) ->
    b64_encode(emqx_utils_json:encode(Map)).

-doc """
Decode a cursor that `encode_json/1` returned.
Return `{error, bad_cursor}` when the input is not a base64-encoded JSON object.
Object keys are binaries. The caller must validate the values.
""".
-spec decode_json(cursor() | term()) -> {ok, map()} | {error, bad_cursor}.
decode_json(Cursor) when is_binary(Cursor) ->
    try emqx_utils_json:decode(b64_decode(Cursor)) of
        Map when is_map(Map) ->
            {ok, Map};
        _ ->
            {error, bad_cursor}
    catch
        _:_ ->
            {error, bad_cursor}
    end;
decode_json(_) ->
    {error, bad_cursor}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

b64_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

b64_decode(Bin) ->
    base64:decode(Bin, #{mode => urlsafe, padding => false}).
