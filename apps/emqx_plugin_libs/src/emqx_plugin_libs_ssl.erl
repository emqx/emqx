%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_libs_ssl).

-export([save_files_return_opts/2,
         save_files_return_opts/3,
         save_files_return_opts/4,
         save_file/2
        ]).

-export([maybe_delete_dir/1,
         maybe_delete_dir/2
        ]).

-type file_input_key() :: binary(). %% <<"file">> | <<"filename">>
-type file_input() :: #{file_input_key() => binary()}.

%% options are below paris
%% <<"keyfile">> => file_input()
%% <<"certfile">> => file_input()
%% <<"cafile">> => file_input() %% backward compatible
%% <<"cacertfile">> => file_input()
%% <<"verify">> => boolean()
%% <<"tls_versions">> => binary()
%% <<"ciphers">> => binary()
-type opts_key() :: binary().
-type opts_input() :: #{opts_key() => file_input() | boolean() | binary()}.

-type opt_key() :: keyfile | certfile | cacertfile | verify | versions | ciphers.
-type opt_value() :: term().
-type opts() :: [{opt_key(), opt_value()}].
-type dirname() :: atom() | string() | binary().

%% @doc Parse ssl options input.
%% If the input contains file content, save the files in the given dir.
%% Returns ssl options for Erlang's ssl application.
-spec save_files_return_opts(opts_input(), dirname(),
                             string() | binary(), dirname()) -> opts().
save_files_return_opts(Options, SubDir, ResId, ResSubdir) ->
    Dir = filename:join([emqx:get_env(data_dir), SubDir, ResId, ResSubdir]),
    save_files_return_opts(Options, Dir).

-spec save_files_return_opts(opts_input(), dirname(),
                             string() | binary()) -> opts().
save_files_return_opts(Options, SubDir, ResId) ->
    Dir = filename:join([emqx:get_env(data_dir), SubDir, ResId]),
    save_files_return_opts(Options, Dir).

%% @doc Parse ssl options input.
%% If the input contains file content, save the files in the given dir.
%% Returns ssl options for Erlang's ssl application.
-spec save_files_return_opts(opts_input(), file:name_all()) -> opts().
save_files_return_opts(Options, Dir) ->
    GetD = fun(Key, Default) -> maps:get(Key, Options, Default) end,
    Get = fun(Key) -> GetD(Key, undefined) end,
    KeyFile = Get(<<"keyfile">>),
    CertFile = Get(<<"certfile">>),
    CAFile = GetD(<<"cacertfile">>, Get(<<"cafile">>)),
    Key = maybe_save_file(KeyFile, Dir),
    Cert = maybe_save_file(CertFile, Dir),
    CA = maybe_save_file(CAFile, Dir),
    Verify = case GetD(<<"verify">>, false) of
                  false -> verify_none;
                  _ -> verify_peer
             end,
    SNI = case Get(<<"server_name_indication">>) of
              <<"disable">> -> disable;
              "disable" -> disable;
              "" -> undefined;
              <<>> -> undefined;
              undefined -> undefined;
              SNI0 -> ensure_str(SNI0)
          end,
    Versions = emqx_tls_lib:integral_versions(Get(<<"tls_versions">>)),
    Ciphers = emqx_tls_lib:integral_ciphers(Versions, Get(<<"ciphers">>)),
    filter([ {keyfile, Key}
           , {certfile, Cert}
           , {cacertfile, CA}
           , {verify, Verify}
           , {server_name_indication, SNI}
           , {versions, Versions}
           , {ciphers, Ciphers}
           ]).

%% @doc Save a key or certificate file in data dir,
%% and return path of the saved file.
%% empty string is returned if the input is empty.
-spec save_file(file_input(), atom() | string() | binary()) -> string().
save_file(Param, SubDir) ->
   Dir = filename:join([emqx:get_env(data_dir), SubDir]),
   maybe_save_file(Param, Dir).

filter([]) -> [];
filter([{_, ""} | T]) -> filter(T);
filter([{_, undefined} | T]) -> filter(T);
filter([H | T]) -> [H | filter(T)].

maybe_save_file(#{<<"filename">> := FileName, <<"file">> := Content}, Dir)
  when FileName =/= undefined andalso Content =/= undefined ->
    maybe_save_file(ensure_str(FileName), iolist_to_binary(Content), Dir);
maybe_save_file(FilePath, _) when is_binary(FilePath) ->
    ensure_str(FilePath);
maybe_save_file(FilePath, _) when is_list(FilePath) ->
    FilePath;
maybe_save_file(_, _) -> "".

maybe_save_file("", _, _Dir) -> ""; %% no filename, ignore
maybe_save_file(FileName, <<>>, Dir) ->  %% no content, see if file exists
    {ok, Cwd} = file:get_cwd(),
    %% NOTE: when FileName is an absolute path, filename:join has no effect
    CwdFile = ensure_str(filename:join([Cwd, FileName])),
    DataDirFile = ensure_str(filename:join([Dir, FileName])),
    Possibles0 = case CwdFile =:= DataDirFile of
                    true -> [CwdFile];
                    false -> [CwdFile, DataDirFile]
                end,
    Possibles = Possibles0 ++
                 case FileName of
                     "etc/certs/" ++ Path ->
                         %% this is the dir hard-coded in rule-engine resources as
                         %% default, unfortunatly we cannot change the deaults
                         %% due to compatibilty reasons, so we have to make a guess
                         ["/etc/emqx/certs/" ++ Path];
                     _ ->
                         []
                 end,
    case find_exist_file(FileName, Possibles) of
        false -> erlang:throw({bad_cert_file, Possibles});
        Found -> Found
    end;
maybe_save_file(FileName, Content, Dir) ->
     FullFilename = filename:join([Dir, FileName]),
     ok = filelib:ensure_dir(FullFilename),
     case file:write_file(FullFilename, Content) of
          ok ->
               ensure_str(FullFilename);
          {error, Reason} ->
               logger:error("failed_to_save_ssl_file ~s: ~0p", [FullFilename, Reason]),
               error({"failed_to_save_ssl_file", FullFilename, Reason})
     end.

maybe_delete_dir(SubDir, ResId) ->
    Dir = filename:join([emqx:get_env(data_dir), SubDir, ResId]),
    maybe_delete_dir(Dir).

maybe_delete_dir(Dir) ->
    case file:del_dir_r(Dir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            logger:error("Delete Resource dir ~p failed for reason: ~p", [Dir, Reason])
    end.

ensure_str(L) when is_list(L) -> L;
ensure_str(B) when is_binary(B) -> unicode:characters_to_list(B, utf8).

find_exist_file(_Name, []) -> false;
find_exist_file(Name, [F | Rest]) ->
    case filelib:is_regular(F) of
        true -> F;
        false -> find_exist_file(Name, Rest)
    end.
