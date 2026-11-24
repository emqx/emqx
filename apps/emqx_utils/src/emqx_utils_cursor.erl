%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_utils_cursor).
%% Encode and decode the opaque pagination cursors that the REST API
%% returns to the client and receives back from it.
%%
%% Two encodings are available:
%%
%% - `encode_json/1` and `decode_json/1`: the cursor is a JSON object in
%%   URL-safe base64 without padding. Use it when the cursor holds only
%%   plain data (integers, strings). The caller validates the decoded map.
%%
%% - `encode_term/2` and `decode_term/2`: the cursor is an Erlang term
%%   with an HMAC-SHA256 tag in URL-safe base64 without padding. Use it
%%   when the cursor must hold an opaque term, for example an ETS
%%   continuation. The HMAC key is derived from the cluster cookie, so a
%%   cursor that one node issues is accepted by every node in the cluster.
%%   `decode_term/2` checks the tag before it decodes the term, and it
%%   decodes only bytes that a node of this cluster encoded.
%%   The `Domain` argument separates the cursors of different APIs:
%%   a cursor encoded for one domain does not decode for another.

-export([
    encode_json/1,
    decode_json/1,
    encode_term/2,
    decode_term/2
]).

-define(MAC_ALG, sha256).
-define(MAC_SIZE, 32).
-define(KEY_LABEL, <<"emqx_utils_cursor">>).

-type cursor() :: binary().
-type domain() :: atom().

-export_type([cursor/0, domain/0]).

%%--------------------------------------------------------------------
%% JSON cursor
%%--------------------------------------------------------------------

%% Encode a JSON object as a cursor.
-spec encode_json(map()) -> cursor().
encode_json(Map) when is_map(Map) ->
    b64_encode(emqx_utils_json:encode(Map)).

%% Decode a cursor that `encode_json/1` returned.
%% Return `{error, bad_cursor}` when the input is not a base64-encoded JSON object.
%% Object keys are binaries. The caller must validate the values.
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
%% Signed term cursor
%%--------------------------------------------------------------------

%% Encode a term as a cursor signed for `Domain`.
-spec encode_term(domain(), term()) -> cursor().
encode_term(Domain, Term) when is_atom(Domain) ->
    Payload = term_to_binary(Term, [{compressed, 9}]),
    b64_encode(<<(mac(Domain, Payload))/binary, Payload/binary>>).

%% Decode a cursor that `encode_term/2` returned for the same `Domain`.
%% Return `{error, bad_cursor}` when the input is not a cursor that a node of
%% this cluster signed for `Domain`.
-spec decode_term(domain(), cursor() | term()) -> {ok, term()} | {error, bad_cursor}.
decode_term(Domain, Cursor) when is_atom(Domain), is_binary(Cursor) ->
    try b64_decode(Cursor) of
        <<Mac:?MAC_SIZE/binary, Payload/binary>> ->
            verify_and_decode(Domain, Mac, Payload);
        _ ->
            {error, bad_cursor}
    catch
        _:_ ->
            {error, bad_cursor}
    end;
decode_term(_Domain, _) ->
    {error, bad_cursor}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

verify_and_decode(Domain, Mac, Payload) ->
    case crypto:hash_equals(Mac, mac(Domain, Payload)) of
        true ->
            try
                {ok, binary_to_term(Payload, [safe])}
            catch
                error:badarg ->
                    {error, bad_cursor}
            end;
        false ->
            {error, bad_cursor}
    end.

mac(Domain, Payload) ->
    crypto:mac(hmac, ?MAC_ALG, key(), [atom_to_binary(Domain), 0, Payload]).

key() ->
    Cookie = atom_to_binary(erlang:get_cookie()),
    crypto:mac(hmac, ?MAC_ALG, Cookie, ?KEY_LABEL).

b64_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

b64_decode(Bin) ->
    base64:decode(Bin, #{mode => urlsafe, padding => false}).
