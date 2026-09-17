%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_managed_certs).

%% API
-export([
    list_managed_files/2,
    list_bundles/1,
    delete_bundle/2,
    delete_managed_file/3,
    add_managed_files/3,
    merge_ca_certs/3,
    delete_ca_cert/3,
    install_files/3,
    find_references/2
]).

%% RPC targets (v1)
-export([
    add_managed_files_v1/3,
    delete_managed_file_v1/3,
    delete_bundle_v1/2
]).

%% Internal exports for debugging
-export([dir/2]).

-ifdef(TEST).
-export([clean_certs_dir/0]).
-export([do_find_references/3]).
-endif.

-export_type([
    file_kind/0,
    bundle_name/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_managed_certs.hrl").
-include("emqx_config.hrl").

-define(path, path).

-define(FILENAME_KEY, "key.pem").
-define(FILENAME_CHAIN, "chain.pem").
-define(FILENAME_CA, "ca.pem").
-define(FILENAME_ACC_KEY, "acc-key.pem").
-define(FILENAME_KEY_PASSWORD, "key-password").

-define(BPAPI, emqx_managed_certs).

-type maybe_namespace() :: emqx_config:maybe_namespace().
-type file_kind() ::
    ?FILE_KIND_KEY
    | ?FILE_KIND_CHAIN
    | ?FILE_KIND_CA
    | ?FILE_KIND_ACC_KEY.
-type bundle_name() :: binary().
-type contents() :: binary().
-type managed_file() :: #{
    ?path := file:filename()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec list_managed_files(maybe_namespace(), bundle_name()) ->
    {ok, #{file_kind() => managed_file()}} | {error, file:posix()}.
list_managed_files(Namespace, BundleName) ->
    maybe
        ok ?= check_namespace(Namespace),
        Dir = dir(Namespace, BundleName),
        {ok, Files0} ?= file:list_dir(Dir),
        Files = lists:foldl(
            fun(Filename, Acc) ->
                maybe
                    {ok, Kind} ?= filename_to_kind(Filename),
                    Path = filename:join([Dir, Filename]),
                    true ?= filelib:is_regular(Path),
                    Acc#{Kind => #{?path => Path}}
                else
                    _ -> Acc
                end
            end,
            #{},
            Files0
        ),
        {ok, Files}
    end.

-spec list_bundles(maybe_namespace()) ->
    {ok, [bundle_name()]} | {error, file:posix()}.
list_bundles(Namespace) ->
    maybe
        ok ?= check_namespace(Namespace),
        Dir = base_dir(Namespace),
        {ok, Contents} ?= file:list_dir(Dir),
        Bundles = lists:sort(
            lists:filter(
                fun(Filename) ->
                    filelib:is_dir(filename:join([Dir, Filename]))
                end,
                Contents
            )
        ),
        {ok, Bundles}
    else
        {error, enoent} ->
            {ok, []};
        Error ->
            Error
    end.

-spec delete_bundle(maybe_namespace(), bundle_name()) ->
    ok
    | {error, bad_namespace}
    | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
delete_bundle(Namespace, BundleName) ->
    maybe
        ok ?= check_namespace(Namespace),
        Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI, 1),
        Res = emqx_managed_certs_proto_v1:delete_bundle(Nodes, Namespace, BundleName),
        NodeRes = lists:zip(Nodes, Res),
        Errors = lists:filtermap(
            fun
                ({_Node, {ok, ok}}) ->
                    false;
                ({_Node, {ok, {error, enoent}}}) ->
                    false;
                ({Node, {ok, {error, Reason}}}) ->
                    {true, #{node => Node, kind => file, reason => Reason}};
                ({Node, {Class, Reason}}) ->
                    {true, #{node => Node, kind => rpc, reason => {Class, Reason}}}
            end,
            NodeRes
        ),
        case Errors of
            [] ->
                ok;
            [_ | _] ->
                {error, Errors}
        end
    end.

-spec delete_managed_file(maybe_namespace(), bundle_name(), file_kind()) ->
    ok
    | {error, bad_namespace}
    | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
delete_managed_file(Namespace, BundleName, Kind) ->
    maybe
        ok ?= check_namespace(Namespace),
        Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI, 1),
        Res = emqx_managed_certs_proto_v1:delete_managed_file(
            Nodes, Namespace, BundleName, Kind
        ),
        NodeRes = lists:zip(Nodes, Res),
        Errors = lists:filtermap(
            fun
                ({_Node, {ok, ok}}) ->
                    false;
                ({_Node, {ok, {error, enoent}}}) ->
                    false;
                ({Node, {ok, {error, Reason}}}) ->
                    {true, #{node => Node, kind => file, reason => Reason}};
                ({Node, {Class, Reason}}) ->
                    {true, #{node => Node, kind => rpc, reason => {Class, Reason}}}
            end,
            NodeRes
        ),
        case Errors of
            [] ->
                ok;
            [_ | _] ->
                {error, Errors}
        end
    end.

-spec add_managed_files(maybe_namespace(), bundle_name(), #{file_kind() := iodata()}) ->
    ok
    | {error, bad_namespace}
    | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
add_managed_files(Namespace, BundleName, Files) ->
    maybe
        ok ?= check_namespace(Namespace),
        Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI, 1),
        Res = emqx_managed_certs_proto_v1:add_managed_files(
            Nodes, Namespace, BundleName, Files
        ),
        NodeRes = lists:zip(Nodes, Res),
        Errors = lists:filtermap(
            fun
                ({_Node, {ok, ok}}) ->
                    false;
                ({Node, {ok, {error, Reason}}}) ->
                    {true, #{node => Node, kind => file, reason => Reason}};
                ({Node, {Class, Reason}}) ->
                    {true, #{node => Node, kind => rpc, reason => {Class, Reason}}}
            end,
            NodeRes
        ),
        case Errors of
            [] ->
                ok;
            [_ | _] ->
                {error, Errors}
        end
    end.

-doc """
Writes files into a bundle on the local node, leaving every other file in it
alone.

All files are written under temporary names in the bundle's own directory
first. When one of these writes fails, the real files are left untouched.
Then each temporary file is renamed over the real one, so a reader sees either
the previous file or the complete new one. The directory itself is never
replaced or removed: it can be a mount point, where renaming over it or
deleting it fails outright.

Kinds the caller does not pass are left as they are, so writing a key and a
chain into a bundle that already holds a `ca' keeps that `ca'.

Local only: unlike `add_managed_files/3', nothing is sent to the other nodes.
""".
-spec install_files(maybe_namespace(), bundle_name(), #{file_kind() := contents()}) ->
    ok | {error, bad_namespace} | {error, term()}.
install_files(Namespace, BundleName, Files) ->
    maybe
        ok ?= check_namespace(Namespace),
        Dir = dir(Namespace, BundleName),
        ok ?= ensure_dir(Dir),
        write_files_staged(Dir, Namespace, BundleName, Files)
    end.

-doc """
Adds CA certificates to the `ca` file of an existing bundle on all nodes.

`PEM` must hold one or more X.509 certificates and nothing else. Certificates
already in the file are skipped. The current file content is kept byte for byte
and the new certificates are appended to it.

The node that runs this function reads the current file, and sends the merged
file to all nodes with `add_managed_files/3`, like any other upload. Concurrent
writes to the same file are not serialized: the last one wins.
""".
-spec merge_ca_certs(maybe_namespace(), bundle_name(), binary()) ->
    {ok, #{added := non_neg_integer(), total := non_neg_integer()}}
    | {error, bad_namespace}
    | {error, bundle_not_found}
    | {error, {bad_ca_certs, binary()}}
    | {error, {read_ca_file, file:posix()}}
    | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
merge_ca_certs(Namespace, BundleName, PEM) ->
    maybe
        ok ?= check_namespace(Namespace),
        {ok, NewCerts} ?= decode_ca_certs(PEM),
        do_merge_ca_certs(Namespace, BundleName, NewCerts)
    end.

-doc """
Removes one CA certificate from the `ca` file of a bundle on all nodes.

`Fingerprint` is the SHA-256 digest of the DER-encoded certificate, as raw
bytes. The other entries of the file are kept in order.

When no certificate is left, the `ca` file is deleted: a file that holds no
certificate is not a valid `cacertfile`. This is refused while a configuration
refers to the bundle, like deleting the `ca` file directly.
""".
-spec delete_ca_cert(maybe_namespace(), bundle_name(), binary()) ->
    {ok, #{total := non_neg_integer()}}
    | {error, bad_namespace}
    | {error, bundle_not_found}
    | {error, cert_not_found}
    | {error, {referenced, [{maybe_namespace(), [binary()]}]}}
    | {error, {read_ca_file, file:posix()}}
    | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
delete_ca_cert(Namespace, BundleName, Fingerprint) ->
    maybe
        ok ?= check_namespace(Namespace),
        true ?= filelib:is_dir(dir(Namespace, BundleName)) orelse {error, bundle_not_found},
        {ok, Existing} ?= read_ca_file(filename(Namespace, BundleName, ?FILE_KIND_CA)),
        Entries = public_key:pem_decode(Existing),
        Remaining = [E || E <- Entries, not is_cert_with_fingerprint(E, Fingerprint)],
        true ?= length(Remaining) < length(Entries) orelse {error, cert_not_found},
        ok ?= write_remaining_ca_entries(Namespace, BundleName, Remaining),
        {ok, #{total => length([E || {'Certificate', _, _} = E <- Remaining])}}
    end.

is_cert_with_fingerprint({'Certificate', Der, _}, Fingerprint) ->
    crypto:hash(sha256, Der) =:= Fingerprint;
is_cert_with_fingerprint(_Entry, _Fingerprint) ->
    false.

write_remaining_ca_entries(Namespace, BundleName, Remaining) ->
    case [E || {'Certificate', _, _} = E <- Remaining] of
        [] ->
            delete_unreferenced_ca_file(Namespace, BundleName);
        [_ | _] ->
            Contents = public_key:pem_encode(Remaining),
            add_managed_files(Namespace, BundleName, #{?FILE_KIND_CA => Contents})
    end.

delete_unreferenced_ca_file(Namespace, BundleName) ->
    case find_references(Namespace, BundleName) of
        [] -> delete_managed_file(Namespace, BundleName, ?FILE_KIND_CA);
        [_ | _] = Refs -> {error, {referenced, Refs}}
    end.

do_merge_ca_certs(Namespace, BundleName, NewCerts) ->
    maybe
        true ?= filelib:is_dir(dir(Namespace, BundleName)) orelse {error, bundle_not_found},
        {ok, Existing} ?= read_ca_file(filename(Namespace, BundleName, ?FILE_KIND_CA)),
        ExistingCerts = [Der || {'Certificate', Der, _} <- public_key:pem_decode(Existing)],
        ToAdd = NewCerts -- ExistingCerts,
        Result = #{
            added => length(ToAdd),
            total => length(ExistingCerts) + length(ToAdd)
        },
        ok ?= append_ca_certs(Namespace, BundleName, Existing, ToAdd),
        {ok, Result}
    end.

append_ca_certs(_Namespace, _BundleName, _Existing, []) ->
    ok;
append_ca_certs(Namespace, BundleName, Existing, Certs) ->
    Appended = public_key:pem_encode([{'Certificate', Der, not_encrypted} || Der <- Certs]),
    Merged = iolist_to_binary([with_trailing_newline(Existing), Appended]),
    add_managed_files(Namespace, BundleName, #{?FILE_KIND_CA => Merged}).

read_ca_file(Path) ->
    case file:read_file(Path) of
        {ok, Contents} -> {ok, Contents};
        {error, enoent} -> {ok, <<>>};
        {error, Reason} -> {error, {read_ca_file, Reason}}
    end.

with_trailing_newline(<<>>) ->
    <<>>;
with_trailing_newline(Bin) ->
    case binary:last(Bin) of
        $\n -> Bin;
        _ -> <<Bin/binary, "\n">>
    end.

%% Returns the DER of each certificate in `PEM', without duplicates, in input
%% order. Fails when `PEM' holds no entry, or any entry that is not a
%% certificate, such as a private key.
decode_ca_certs(PEM) ->
    try public_key:pem_decode(PEM) of
        [] ->
            {error, {bad_ca_certs, <<"no PEM certificate found">>}};
        Entries ->
            decode_ca_cert_entries(Entries, [])
    catch
        _:_ ->
            {error, {bad_ca_certs, <<"malformed PEM data">>}}
    end.

decode_ca_cert_entries([], Acc) ->
    {ok, lists:reverse(Acc)};
decode_ca_cert_entries([{'Certificate', Der, not_encrypted} | Rest], Acc) ->
    case is_x509_cert(Der) of
        true ->
            decode_ca_cert_entries(Rest, add_new(Der, Acc));
        false ->
            {error, {bad_ca_certs, <<"malformed X.509 certificate">>}}
    end;
decode_ca_cert_entries([{Type, _, _} | _], _Acc) ->
    Msg = iolist_to_binary(["PEM entry is not a certificate: ", atom_to_binary(Type)]),
    {error, {bad_ca_certs, Msg}}.

add_new(Der, Acc) ->
    case lists:member(Der, Acc) of
        true -> Acc;
        false -> [Der | Acc]
    end.

is_x509_cert(Der) ->
    try public_key:pkix_decode_cert(Der, otp) of
        _ -> true
    catch
        _:_ -> false
    end.

ensure_dir(Dir) ->
    case filelib:ensure_path(Dir) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% Writes all files under temporary names first. When any of these writes
%% fails, removes the temporary files and leaves the real ones untouched.
%% Then renames the temporary files over the real ones, one right after
%% another. The directory is never replaced: it can be a mount point.
%% Returns the failed kinds with their errors.
write_files_staged(Dir, Namespace, BundleName, Files) ->
    Staged = maps:map(
        fun(Kind, Contents) ->
            Path = filename:join(Dir, filename:basename(filename(Namespace, BundleName, Kind))),
            Tmp = tmp_filename(Path),
            {Path, Tmp, file:write_file(Tmp, Contents)}
        end,
        Files
    ),
    case maps:filter(fun(_Kind, {_Path, _Tmp, Res}) -> Res =/= ok end, Staged) of
        Failed when map_size(Failed) =:= 0 ->
            rename_staged(maps:to_list(Staged), #{});
        Failed ->
            maps:foreach(fun(_Kind, {_Path, Tmp, _Res}) -> _ = file:delete(Tmp) end, Staged),
            {error, maps:map(fun(_Kind, {_Path, _Tmp, Res}) -> Res end, Failed)}
    end.

rename_staged([], Errors) when map_size(Errors) =:= 0 ->
    ok;
rename_staged([], Errors) ->
    {error, Errors};
rename_staged([{Kind, {Path, Tmp, ok}} | Rest], Errors) ->
    case file:rename(Tmp, Path) of
        ok ->
            rename_staged(Rest, Errors);
        {error, _} = Error ->
            _ = file:delete(Tmp),
            rename_staged(Rest, Errors#{Kind => Error})
    end.

%% In the same directory as `Path', so the rename stays on one filesystem.
%% `list_managed_files/2' does not list files with this name.
tmp_filename(Path) ->
    %% `Path' may be a binary: build the temporary name without assuming a list.
    iolist_to_binary([Path, ".tmp.", binary:encode_hex(crypto:strong_rand_bytes(8))]).

%%------------------------------------------------------------------------------
%% RPC Targets
%%------------------------------------------------------------------------------

-spec add_managed_files_v1(maybe_namespace(), bundle_name(), #{file_kind() := contents()}) ->
    ok | {error, bad_namespace} | {error, #{file_kind() := file:posix()}}.
-doc #{since => <<"6.1.0">>}.
add_managed_files_v1(Namespace, BundleName, Files) ->
    maybe
        ok ?= check_namespace(Namespace),
        add_managed_files_local(Namespace, BundleName, Files)
    end.

add_managed_files_local(Namespace, BundleName, Files) ->
    Dir = dir(Namespace, BundleName),
    case ensure_dir(Dir) of
        ok ->
            write_files_staged(Dir, Namespace, BundleName, Files);
        {error, _} = Error ->
            {error, maps:map(fun(_Kind, _Contents) -> Error end, Files)}
    end.

-spec delete_managed_file_v1(maybe_namespace(), bundle_name(), file_kind()) ->
    ok | {error, bad_namespace | file:posix()}.
-doc #{since => <<"6.1.0">>}.
delete_managed_file_v1(Namespace, BundleName, Kind) ->
    maybe
        ok ?= check_namespace(Namespace),
        Filename = filename(Namespace, BundleName, Kind),
        file:delete(Filename)
    end.

-spec delete_bundle_v1(maybe_namespace(), bundle_name()) ->
    ok | {error, bad_namespace | file:posix()}.
-doc #{since => <<"6.1.0">>}.
delete_bundle_v1(Namespace, BundleName) ->
    maybe
        ok ?= check_namespace(Namespace),
        Dir = dir(Namespace, BundleName),
        file:del_dir_r(Dir)
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

filename(Namespace, BundleName, ?FILE_KIND_KEY) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_KEY);
filename(Namespace, BundleName, ?FILE_KIND_CHAIN) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_CHAIN);
filename(Namespace, BundleName, ?FILE_KIND_CA) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_CA);
filename(Namespace, BundleName, ?FILE_KIND_ACC_KEY) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_ACC_KEY);
filename(Namespace, BundleName, ?FILE_KIND_KEY_PASSWORD) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_KEY_PASSWORD).

base_dir(?global_ns) ->
    DataDir = emqx:data_dir(),
    filename:join([DataDir, certs2, global]);
base_dir(Namespace0) when is_binary(Namespace0) ->
    DataDir = emqx:data_dir(),
    Namespace = escape_name(Namespace0),
    filename:join([DataDir, certs2, ns, Namespace]).

dir(Namespace, BundleName) ->
    BaseDir = base_dir(Namespace),
    %% Bundle name is already safe (validated in HTTP API)
    filename:join([BaseDir, BundleName]).

filename_to_kind(?FILENAME_KEY) ->
    {ok, ?FILE_KIND_KEY};
filename_to_kind(?FILENAME_CHAIN) ->
    {ok, ?FILE_KIND_CHAIN};
filename_to_kind(?FILENAME_CA) ->
    {ok, ?FILE_KIND_CA};
filename_to_kind(?FILENAME_ACC_KEY) ->
    {ok, ?FILE_KIND_ACC_KEY};
filename_to_kind(?FILENAME_KEY_PASSWORD) ->
    {ok, ?FILE_KIND_KEY_PASSWORD};
filename_to_kind(_) ->
    error.

escape_name(Name) ->
    uri_string:quote(Name).

%% The namespace becomes a directory name under `<data>/certs2/ns/'. It must be
%% a single path component that is not a special one, so it cannot escape into
%% the shared `certs2' directory or another tenant's directory. `escape_name/1'
%% (applied when building the path) percent-encodes characters such as `:' or
%% spaces, but path separators and the components `.' and `..' would still let
%% the name traverse, so reject them here by checking the raw name. The
%% namespace comes from the caller or config and is never recovered from the
%% directory name, so the check does not need to be reversible.
check_namespace(?global_ns) ->
    ok;
check_namespace(Namespace) when is_binary(Namespace) ->
    case filename:split(Namespace) of
        [Namespace] when Namespace =/= <<".">>, Namespace =/= <<"..">> ->
            ok;
        _ ->
            {error, bad_namespace}
    end.

find_references(TargetNamespace, TargetBundleName) ->
    NsConfigs = maps:merge(
        #{?global_ns => emqx_config:get_raw([])},
        emqx_config:get_all_raw_namespaced_configs()
    ),
    do_find_references(NsConfigs, TargetNamespace, TargetBundleName).

do_find_references(NsConfigs, TargetNamespace, TargetBundleName) ->
    emqx_config_lib:fold_namespace_configs(
        fun
            (Ns, [<<"managed_certs">> | _] = Stack, Value, Acc) ->
                case contains_managed_cert(Value, TargetNamespace, TargetBundleName) of
                    true ->
                        PrettyStack =
                            case Stack of
                                [_, <<"ssl">> | Stack0] ->
                                    Stack0;
                                [_, <<"ssl_options">> | Stack0] ->
                                    Stack0;
                                [_ | Stack0] ->
                                    Stack0
                            end,
                        {stop, [{Ns, lists:reverse(PrettyStack)} | Acc]};
                    false ->
                        {cont, Acc}
                end;
            (_Ns, _Stack, _Value, Acc) ->
                {cont, Acc}
        end,
        [],
        NsConfigs
    ).

contains_managed_cert(
    #{<<"bundle_name">> := TargetBundleName} = Config,
    ?global_ns = _TargetNamespace,
    TargetBundleName
) when
    not is_map_key(<<"namespace">>, Config)
->
    true;
contains_managed_cert(
    #{<<"bundle_name">> := TargetBundleName, <<"namespace">> := TargetNamespace},
    TargetNamespace,
    TargetBundleName
) ->
    true;
contains_managed_cert(Items, TargetNamespace, TargetBundleName) when is_list(Items) ->
    lists:any(
        fun(Item) -> contains_managed_cert(Item, TargetNamespace, TargetBundleName) end,
        Items
    );
contains_managed_cert(_, _TargetNamespace, _TargetBundleName) ->
    false.

-ifdef(TEST).
clean_certs_dir() ->
    DataDir = emqx:data_dir(),
    case file:del_dir_r(filename:join([DataDir, certs2])) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        Error ->
            Error
    end.
-endif.
