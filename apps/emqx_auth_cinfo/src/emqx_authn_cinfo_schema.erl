%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_cinfo_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_auth_cinfo.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

namespace() -> "authn".

refs() ->
    [
        ?R_REF("cinfo")
    ].

select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_BIN}) ->
    [?R_REF("cinfo")];
select_union_member(_Value) ->
    undefined.

fields("cinfo") ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM)},
        {checks,
            hoconsc:mk(
                hoconsc:array(?R_REF("cinfo_check")),
                #{
                    required => true,
                    desc => ?DESC(checks),
                    validator => fun validate_checks/1,
                    importance => ?IMPORTANCE_HIGH
                }
            )}
    ] ++ emqx_authn_schema:common_fields();
fields("cinfo_check") ->
    [
        {is_match,
            hoconsc:mk(
                hoconsc:union([binary(), hoconsc:array(binary())]),
                #{
                    required => true,
                    desc => ?DESC(is_match),
                    importance => ?IMPORTANCE_HIGH,
                    validator => fun validate_expressions/1
                }
            )},
        {result,
            hoconsc:mk(
                hoconsc:enum([allow, deny, ignore]),
                #{
                    required => true,
                    desc => ?DESC(result),
                    importance => ?IMPORTANCE_HIGH
                }
            )}
    ].

desc("cinfo") ->
    ?DESC("cinfo");
desc("cinfo_check") ->
    ?DESC("check").

validate_checks([]) ->
    throw("require_at_least_one_check");
validate_checks(List) when is_list(List) ->
    ok.

validate_expressions(Expr) when is_binary(Expr) ->
    validate_expression(Expr);
validate_expressions(Exprs) when is_list(Exprs) ->
    lists:foreach(fun validate_expression/1, Exprs).

validate_expression(<<>>) ->
    throw("should not be empty string");
validate_expression(Expr) ->
    case emqx_variform:compile(Expr) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            throw(Reason)
    end.
