%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_gssapi_schema).

-include("emqx_auth_gssapi.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

namespace() -> "authn".

refs() ->
    [?R_REF(gssapi)].

select_union_member(#{
    <<"mechanism">> := ?AUTHN_MECHANISM_GSSAPI_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
}) ->
    refs();
select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_GSSAPI_BIN}) ->
    throw(#{
        reason => "unknown_backend",
        expected => ?AUTHN_BACKEND
    });
select_union_member(_) ->
    undefined.

fields(gssapi) ->
    emqx_authn_schema:common_fields() ++
        [
            {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_GSSAPI)},
            {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
            {principal,
                ?HOCON(binary(), #{
                    required => true,
                    desc => ?DESC(principal)
                })},
            {keytab_file,
                ?HOCON(binary(), #{
                    required => true,
                    desc => ?DESC(keytab_file)
                })}
        ].

desc(gssapi) ->
    "Settings for GSSAPI authentication.";
desc(_) ->
    undefined.
