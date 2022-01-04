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
-module(emqx_bpapi).

-export([parse_semver/1, api_and_version/1]).

-export_type([var_name/0, call/0, rpc/0, bpapi_meta/0]).

-type semver() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-type api() :: atom().
-type api_version() :: non_neg_integer().
-type var_name() :: atom().
-type call() :: {module(), atom(), [var_name()]}.
-type rpc() :: {_From :: call(), _To :: call()}.

-type bpapi_meta() ::
        #{ api     := api()
         , version := api_version()
         , calls   := [rpc()]
         , casts   := [rpc()]
         }.

-spec parse_semver(string()) -> {ok, semver()}
                              | false.
parse_semver(Str) ->
    Opts = [{capture, all_but_first, list}],
    case re:run(Str, "^([0-9]+)\\.([0-9]+)\\.([0-9]+)$", Opts) of
        {match, [A, B, C]} -> {ok, {list_to_integer(A), list_to_integer(B), list_to_integer(C)}};
        nomatch            -> error
    end.

-spec api_and_version(module()) -> {atom(), non_neg_integer()}.
api_and_version(Module) ->
  Opts = [{capture, all_but_first, list}],
  case re:run(atom_to_list(Module), "(.*)_proto_v([0-9]+)$", Opts) of
    {match, [API, VsnStr]} ->
      {ok, list_to_atom(API), list_to_integer(VsnStr)};
    nomatch ->
      error(Module)
  end.
