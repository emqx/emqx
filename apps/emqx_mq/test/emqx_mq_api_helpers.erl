%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api_helpers).

-export([
    api_get/1,
    api_post/2,
    api_put/2,
    api_delete/1,
    urlencode/1
]).

api_get(Path) ->
    R = emqx_mgmt_api_test_util:request(get, emqx_mgmt_api_test_util:uri(Path)),
    decode_body(R).

api_post(Path, Data) ->
    decode_body(emqx_mgmt_api_test_util:request(post, emqx_mgmt_api_test_util:uri(Path), Data)).

api_put(Path, Data) ->
    decode_body(emqx_mgmt_api_test_util:request(put, emqx_mgmt_api_test_util:uri(Path), Data)).

api_delete(Path) ->
    decode_body(emqx_mgmt_api_test_util:request(delete, emqx_mgmt_api_test_util:uri(Path))).

urlencode(X) when is_list(X) ->
    uri_string:quote(X);
urlencode(X) when is_binary(X) ->
    urlencode(binary_to_list(X)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

decode_body({ok, Code, <<>>}) ->
    {ok, Code};
decode_body({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} ->
            {ok, Code, Decoded};
        {error, _} = Error ->
            ct:pal("Invalid body: ~p", [Body]),
            Error
    end;
decode_body(Error) ->
    Error.
