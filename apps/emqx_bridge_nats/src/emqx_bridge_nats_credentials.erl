-module(emqx_bridge_nats_credentials).

-export([materialize/2]).

materialize(Path, Config) ->
    case authentication(Config) of
        #{mechanism := jwt, credentials_file := Credentials} = Auth ->
            case materialize_file(Path, Credentials) of
                {ok, Filename} ->
                    {ok, put_authentication(Config, Auth#{credentials_file => Filename})};
                {error, _} = Error ->
                    Error
            end;
        #{<<"mechanism">> := <<"jwt">>, <<"credentials_file">> := Credentials} = Auth ->
            case materialize_file(Path, Credentials) of
                {ok, Filename} ->
                    {ok, put_authentication(Config, Auth#{<<"credentials_file">> => Filename})};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {ok, Config}
    end.

authentication(#{authentication := Auth}) ->
    Auth;
authentication(#{<<"authentication">> := Auth}) ->
    Auth;
authentication(_) ->
    undefined.

put_authentication(#{authentication := _} = Config, Auth) ->
    Config#{authentication := Auth};
put_authentication(#{<<"authentication">> := _} = Config, Auth) ->
    Config#{<<"authentication">> := Auth}.

materialize_file(_Path, Filename) when is_list(Filename) ->
    {ok, Filename};
materialize_file(Path, Filename) when is_binary(Filename) ->
    case binary:match(Filename, <<"-----BEGIN NATS USER JWT-----">>) of
        nomatch ->
            {ok, Filename};
        _ ->
            save_credentials(Path, Filename)
    end.

save_credentials(Path, Contents) ->
    case enats_credentials:from_binary(Contents) of
        {ok, _Auth} ->
            RelativeDir = filename:join(Path),
            Dir = emqx_tls_lib:pem_dir(RelativeDir),
            Digest = binary:encode_hex(crypto:hash(md5, [RelativeDir, Contents])),
            Filename = filename:join(Dir, "credentials-" ++ binary_to_list(Digest) ++ ".creds"),
            case filelib:ensure_dir(Filename) of
                ok ->
                    write_credentials(Filename, Contents);
                {error, Reason} ->
                    {error, #{reason => failed_to_create_credentials_dir, detail => Reason}}
            end;
        {error, Reason} ->
            {error, #{reason => invalid_credentials, detail => Reason}}
    end.

write_credentials(Filename, Contents) ->
    Tmp = iolist_to_binary([
        Filename,
        ".tmp.",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    case file:write_file(Tmp, Contents) of
        ok ->
            case file:change_mode(Tmp, 8#600) of
                ok ->
                    case file:rename(Tmp, Filename) of
                        ok ->
                            {ok, Filename};
                        {error, Reason} ->
                            _ = file:delete(Tmp),
                            {error, #{reason => failed_to_install_credentials, detail => Reason}}
                    end;
                {error, Reason} ->
                    _ = file:delete(Tmp),
                    {error, #{reason => failed_to_set_credentials_permissions, detail => Reason}}
            end;
        {error, Reason} ->
            {error, #{reason => failed_to_write_credentials, detail => Reason}}
    end.
