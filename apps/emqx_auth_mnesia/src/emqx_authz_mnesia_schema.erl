%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia_schema).

-include("emqx_auth_mnesia.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authz_schema).

-export([
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    select_union_member/2,
    namespace/0
]).

-define(MAX_RULES, 100).

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

fields(builtin_db) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        [
            {max_rules,
                ?HOCON(
                    pos_integer(),
                    #{
                        default => ?MAX_RULES,
                        desc => ?DESC(max_rules)
                    }
                )}
        ].

source_refs() ->
    [?R_REF(builtin_db)].

desc(builtin_db) ->
    ?DESC(builtin_db);
desc(_) ->
    undefined.

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, _) ->
    ?R_REF(builtin_db);
select_union_member(_Value, _) ->
    undefined.
