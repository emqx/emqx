%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_file_schema).

-include("emqx_auth_file.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authz_schema).

-export([
    namespace/0,
    type/0,
    fields/1,
    desc/1,
    source_refs/0,
    api_source_refs/0,
    select_union_member/2
]).

namespace() -> "authz".

type() -> ?AUTHZ_TYPE.

fields(file) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        [
            {path,
                ?HOCON(
                    string(),
                    #{
                        required => true,
                        validator => fun(Path) -> element(1, emqx_authz_file:validate(Path)) end,
                        desc => ?DESC(path)
                    }
                )}
        ];
fields(api_file) ->
    emqx_authz_schema:authz_common_fields(?AUTHZ_TYPE) ++
        [
            {rules,
                ?HOCON(
                    binary(),
                    #{
                        required => true,
                        example =>
                            <<
                                "{allow,{username,{re,\"^dashboard$\"}},subscribe,[\"$SYS/#\"]}.\n",
                                "{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}."
                            >>,
                        desc => ?DESC(rules)
                    }
                )}
        ].

desc(file) ->
    ?DESC(file);
desc(api_file) ->
    ?DESC(file).

source_refs() ->
    [?R_REF(file)].

api_source_refs() ->
    [?R_REF(api_file)].

select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, source_refs) ->
    ?R_REF(file);
select_union_member(#{<<"type">> := ?AUTHZ_TYPE_BIN}, api_source_refs) ->
    ?R_REF(api_file);
select_union_member(_Value, _) ->
    undefined.
