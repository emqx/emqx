%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_mfa_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% Hocon
-export([namespace/0, fields/1, desc/1]).

-export([
    common_mfa_schema/1,
    method_schema/1,
    token_schema/0
]).

-import(hoconsc, [ref/2, mk/2, enum/1]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------
namespace() -> dashboard.

fields(mfa) ->
    lists:map(
        fun({Type, Module}) ->
            {Type,
                mk(
                    emqx_dashboard_mfa:config_ref(Module),
                    #{required => {false, recursively}}
                )}
        end,
        maps:to_list(emqx_dashboard_mfa:methods())
    ).

desc(mfa) ->
    "Dashboard Multi-factor Authentication";
desc(_) ->
    undefined.

-spec common_mfa_schema(list(atom())) -> proplists:proplist().
common_mfa_schema(Method) ->
    [
        {enable,
            mk(
                boolean(), #{
                    desc => ?DESC(enable),
                    required => false,
                    default => false
                }
            )},
        method_schema(Method)
    ].

method_schema(Method) ->
    {method,
        mk(enum([Method]), #{
            required => true,
            desc => ?DESC(method)
        })}.

token_schema() ->
    {token, mk(binary(), #{desc => ?DESC(token)})}.
