-module(emqx_mcp_authorization).

-export([
    get_roles/1
]).

get_roles(_ServerName) ->
    {error, rbac_not_implemented}.
