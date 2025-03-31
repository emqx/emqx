%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mysql_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_auth_mysql.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() ->
    [?R_REF(mysql)].

select_union_member(
    #{
        <<"mechanism">> := ?AUTHN_MECHANISM_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
    }
) ->
    refs();
select_union_member(#{<<"backend">> := ?AUTHN_BACKEND_BIN}) ->
    throw(#{
        reason => "unknown_mechanism",
        expected => ?AUTHN_MECHANISM
    });
select_union_member(_) ->
    undefined.

fields(mysql) ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1},
        {query, fun query/1},
        {query_timeout, fun query_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++ emqx_mysql:fields(config).

desc(mysql) ->
    ?DESC(mysql);
desc(_) ->
    undefined.

query(type) -> string();
query(desc) -> ?DESC(?FUNCTION_NAME);
query(required) -> true;
query(_) -> undefined.

query_timeout(type) -> emqx_schema:duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.
