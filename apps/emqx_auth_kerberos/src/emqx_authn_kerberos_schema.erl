%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    select_union_member/1,
    get_default_kt_name/0
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
                    %% do not generate it in config doc
                    %% currently EMQX only works with default keytab file path
                    %% e.g. KRB5_KTNAME="/var/lib/emqx/krb5.keytab
                    %% or set by default_keytab_name config in /etc/krb5.conf
                    %% This config is present only to display the default value to frontend.
                    importance => ?IMPORTANCE_HIDDEN,
                    validator => fun validate_keytab_file_path/1
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

validate_keytab_file_path(<<>>) ->
    ok;
validate_keytab_file_path(<<"DEFAULT">>) ->
    %% A hidden magic value for testing
    ok;
validate_keytab_file_path(Path0) ->
    Path = emqx_schema:naive_env_interpolation(Path0),
    Default = get_default_kt_name(),
    case Path =:= Default of
        true ->
            validate_file_readable(Path);
        false ->
            throw(#{
                cause => bad_keytab_file_path,
                system_default => Default,
                explain =>
                    "This is a limitation of the current version. "
                    "The keytab file must be configured as system default keytab file. "
                    "You may try to configure system default value using "
                    "environment variable KRB5_KTNAME or set default_keytab_name "
                    "in /etc/krb5.conf."
            })
    end.

get_default_kt_name() ->
    case sasl_auth:krb5_kt_default_name() of
        <<"FILE:", Path/binary>> ->
            unicode:characters_to_list(Path, utf8);
        Path ->
            unicode:characters_to_list(Path, utf8)
    end.

validate_file_readable(Path) ->
    case filelib:is_regular(Path) of
        true ->
            ok;
        false ->
            throw(#{
                cause => "cannot read keytab file",
                path => Path
            })
    end.
