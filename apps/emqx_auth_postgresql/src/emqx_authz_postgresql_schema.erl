%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_postgresql_schema).

-include("emqx_auth_postgresql.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authz_schema).

-export([
    namespace/0,
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    select_union_member/2
]).

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

fields(postgresql) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        emqx_postgresql:fields(config) ++
        [{query, query()}].

desc(postgresql) ->
    ?DESC(postgresql);
desc(_) ->
    undefined.

source_refs() ->
    [?R_REF(postgresql)].

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, _) ->
    ?R_REF(postgresql);
select_union_member(_Value, _) ->
    undefined.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

query() ->
    ?HOCON(binary(), #{
        desc => ?DESC(query),
        required => true,
        validator => fun(S) ->
            case size(S) > 0 of
                true -> ok;
                _ -> {error, "Request query"}
            end
        end
    }).
