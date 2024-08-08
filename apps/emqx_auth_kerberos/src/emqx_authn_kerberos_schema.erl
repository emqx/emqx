%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_kerberos_schema).

-include("emqx_auth_kerberos.hrl").
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
    [?R_REF(kerberos)].

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

fields(kerberos) ->
    emqx_authn_schema:common_fields() ++
        [
            {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_GSSAPI)},
            {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
            {principal,
                ?HOCON(binary(), #{
                    required => true,
                    desc => ?DESC(principal),
                    validator => fun validate_principal/1
                })},
            {keytab_file,
                ?HOCON(binary(), #{
                    default => <<"/etc/krb5.keytab">>,
                    desc => ?DESC(keytab_file)
                })}
        ].

desc(kerberos) ->
    "Settings for Kerberos authentication.";
desc(_) ->
    undefined.

validate_principal(S) ->
    P = <<"^([a-zA-Z0-9\\._-]+)/([a-zA-Z0-9\\.-]+)(?:@([A-Z0-9\\.-]+))?$">>,
    case re:run(S, P) of
        nomatch -> {error, invalid_server_principal_string};
        {match, _} -> ok
    end.
