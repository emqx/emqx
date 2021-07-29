%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
         save_file/2
        ]).

-type file_input_key() :: atom() | binary(). %% <<"file">> | <<"filename">>
-type file_input() :: #{file_input_key() => binary()}.

%% options are below paris
%% <<"keyfile">> => file_input()
%% <<"certfile">> => file_input()
%% <<"cafile">> => file_input() %% backward compatible
%% <<"cacertfile">> => file_input()
%% <<"verify">> => boolean()
%% <<"tls_versions">> => binary()
%% <<"ciphers">> => binary()
-type opts_key() :: binary() | atom().
-type opts_input() :: #{opts_key() => term()}.

-type opt_key() :: keyfile | certfile | cacertfile | verify | versions | ciphers.
-type opt_value() :: term().
-type opts() :: [{opt_key(), opt_value()}].

%% @doc Parse ssl options input.
%% If the input contains file content, save the files in the given dir.
%% Returns ssl options for Erlang's ssl application.
-spec save_files_return_opts(opts_input(), atom() | string() | binary(),
                             string() | binary()) -> opts().
save_files_return_opts(Options, SubDir, ResId) ->
    Dir = filename:join([emqx_config:get([node, data_dir]), SubDir, ResId]),
    save_files_return_opts(Options, Dir).

%% @doc Parse ssl options input.
%% If the input contains file content, save the files in the given dir.
%% Returns ssl options for Erlang's ssl application.
-spec save_files_return_opts(opts_input(), file:name_all()) -> opts().
save_files_return_opts(Options, Dir) ->
    GetD = fun(Key, Default) -> fuzzy_map_get(Key, Options, Default) end,
    Get = fun(Key) -> GetD(Key, undefined) end,
    KeyFile = Get(keyfile),
    CertFile = Get(certfile),
    CAFile = Get(cacertfile),
    Key = do_save_file(KeyFile, Dir),
    Cert = do_save_file(CertFile, Dir),
    CA = do_save_file(CAFile, Dir),
    Verify = case GetD(verify, false) of
                  false -> verify_none;
                  _ -> verify_peer
             end,
    SNI = Get(server_name_indication),
    Versions = emqx_tls_lib:integral_versions(Get(tls_versions)),
    Ciphers = emqx_tls_lib:integral_ciphers(Versions, Get(ciphers)),
    filter([{keyfile, Key}, {certfile, Cert}, {cacertfile, CA},
            {verify, Verify}, {server_name_indication, SNI}, {versions, Versions}, {ciphers, Ciphers}]).

%% @doc Save a key or certificate file in data dir,
%% and return path of the saved file.
%% empty string is returned if the input is empty.
-spec save_file(file_input(), atom() | string() | binary()) -> string().
save_file(Param, SubDir) ->
   Dir = filename:join([emqx_config:get([node, data_dir]), SubDir]),
   do_save_file(Param, Dir).

filter([]) -> [];
filter([{_, ""} | T]) -> filter(T);
filter([H | T]) -> [H | filter(T)].

do_save_file(#{filename := FileName, file := Content}, Dir)
  when FileName =/= undefined andalso Content =/= undefined ->
    do_save_file(ensure_str(FileName), iolist_to_binary(Content), Dir);
do_save_file(FilePath, _) when is_list(FilePath) ->
    FilePath;
do_save_file(FilePath, _) when is_binary(FilePath) ->
    ensure_str(FilePath);
do_save_file(_, _) -> "".

do_save_file("", _, _Dir) -> ""; %% ignore
do_save_file(_, <<>>, _Dir) -> ""; %% ignore
do_save_file(FileName, Content, Dir) ->
     FullFilename = filename:join([Dir, FileName]),
     ok = filelib:ensure_dir(FullFilename),
     case file:write_file(FullFilename, Content) of
          ok ->
               ensure_str(FullFilename);
          {error, Reason} ->
               logger:error("failed_to_save_ssl_file ~s: ~0p", [FullFilename, Reason]),
               error({"failed_to_save_ssl_file", FullFilename, Reason})
     end.

ensure_str(L) when is_list(L) -> L;
ensure_str(B) when is_binary(B) -> unicode:characters_to_list(B, utf8).

-spec fuzzy_map_get(atom() | binary(), map(), any()) -> any().
fuzzy_map_get(Key, Options, Default) ->
    case maps:find(Key, Options) of
        {ok, Val} -> Val;
        error when is_atom(Key) ->
            fuzzy_map_get(atom_to_binary(Key, utf8), Options, Default);
        error -> Default
    end.
