-define(ERR_NO_SERVER_AVAILABLE, no_server_available).
-define(ERR_TIMEOUT, timeout).
-define(ERR_INVALID_JSON, invalid_json).
-define(ERR_SEND_TO_MCP_SERVER_FAILED, send_to_mcp_server_failed).
-define(ERR_INVALID_SERVER_RESPONSE, invalid_server_response).
-define(ERR_WRONG_SERVER_RESPONSE_ID, wrong_server_response_id).
-define(ERR_MALFORMED_JSON_RPC, malformed_json_rpc).
-define(ERR_UNEXPECTED_METHOD, unexpected_method).

-define(ERR_CODE(ERR),
    case ERR of
        ?ERR_NO_SERVER_AVAILABLE -> -32000;
        ?ERR_TIMEOUT -> -32001;
        ?ERR_INVALID_JSON -> -32002;
        ?ERR_SEND_TO_MCP_SERVER_FAILED -> -32003;
        ?ERR_INVALID_SERVER_RESPONSE -> -32004;
        ?ERR_WRONG_SERVER_RESPONSE_ID -> -32005;
        ?ERR_MALFORMED_JSON_RPC -> -32006;
        ?ERR_UNEXPECTED_METHOD -> -32007
    end
).
