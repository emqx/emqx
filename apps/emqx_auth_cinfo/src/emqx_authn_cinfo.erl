%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_cinfo).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("jose/include/jose_jwk.hrl").

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

create(AuthenticatorID, #{checks := Checks}) ->
    case compile(AuthenticatorID, Checks) of
        {ok, Compiled} ->
            {ok, #{
                id => AuthenticatorID,
                checks => Compiled
            }};
        {error, Reason} ->
            {error, Reason}
    end.

compile(ID, Checks) ->
    try
        {ok, compile_checks(Checks, [])}
    catch
        throw:Error ->
            {error, Error#{authenticator => ID}}
    end.

compile_checks([], Acc) ->
    lists:reverse(Acc);
compile_checks([Check | Checks], Acc) ->
    compile_checks(Checks, [compile_check(Check) | Acc]).

compile_check(#{is_match := Expressions} = C) ->
    Compiled = compile_exprs(Expressions),
    %% is_match being non-empty is ensured by schema module
    Compiled =:= [] andalso error(empty),
    C#{is_match => Compiled}.

compile_exprs(Expression) when is_binary(Expression) ->
    [compile_expr(Expression)];
compile_exprs(Expressions) when is_list(Expressions) ->
    lists:map(fun compile_expr/1, Expressions).

compile_expr(Expression) ->
    %% Expression not empty string is ensured by schema
    true = (<<"">> =/= Expression),
    %% emqx_variform:compile(Expression) return 'ok' tuple is ensured by schema
    {ok, Compiled} = emqx_variform:compile(Expression),
    Compiled.

update(#{enable := false}, State) ->
    {ok, State};
update(Config, #{id := ID}) ->
    create(ID, Config).

authenticate(#{auth_method := _}, _) ->
    %% enhanced authentication is not supported by this provider
    ignore;
authenticate(Credential0, #{checks := Checks}) ->
    Credential1 = add_credential_aliases(Credential0),
    Credential = peerhost_as_string(Credential1),
    check(Checks, Credential).

peerhost_as_string(#{peerhost := Peerhost} = Credential) when is_tuple(Peerhost) ->
    Credential#{peerhost => iolist_to_binary(inet:ntoa(Peerhost))};
peerhost_as_string(Credential) ->
    Credential.

check([], _) ->
    ignore;
check([Check | Rest], Credential) ->
    case do_check(Check, Credential) of
        nomatch ->
            check(Rest, Credential);
        {match, ignore} ->
            ignore;
        {match, allow} ->
            {ok, #{}};
        {match, deny} ->
            {error, bad_username_or_password}
    end.

do_check(#{is_match := CompiledExprs, result := Result}, Credential) ->
    case is_match(CompiledExprs, Credential) of
        true ->
            {match, Result};
        false ->
            nomatch
    end.

is_match([], _Credential) ->
    true;
is_match([CompiledExpr | CompiledExprs], Credential) ->
    case emqx_variform:render(CompiledExpr, Credential) of
        {ok, <<"true">>} ->
            is_match(CompiledExprs, Credential);
        {ok, <<"false">>} ->
            false;
        {ok, Other} ->
            ?SLOG(debug, "clientinfo_auth_expression_yield_non_boolean", #{
                expr => emqx_variform:decompile(CompiledExpr),
                yield => Other
            }),
            false;
        {error, Reason} ->
            {error, #{
                cause => "clientinfo_auth_expression_evaluation_error",
                error => Reason
            }}
    end.

destroy(_) ->
    ok.

%% Add aliases for credential fields
%% - cert_common_name for cn
%% - cert_subject for dn
add_credential_aliases(Credential) ->
    Aliases = [
        {cn, cert_common_name},
        {dn, cert_subject}
    ],
    add_credential_aliases(Credential, Aliases).

add_credential_aliases(Credential, []) ->
    Credential;
add_credential_aliases(Credential, [{Field, Alias} | Rest]) ->
    case maps:find(Field, Credential) of
        {ok, Value} ->
            add_credential_aliases(Credential#{Alias => Value}, Rest);
        error ->
            add_credential_aliases(Credential, Rest)
    end.
