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

-module(emqx_ft_schema).

-behaviour(hocon_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-export([namespace/0, roots/0, fields/1, tags/0]).

-export([schema/1]).

-type json_value() ::
    null
    | boolean()
    | binary()
    | number()
    | [json_value()]
    | #{binary() => json_value()}.

-reflect_type([json_value/0]).

%%

namespace() -> file_transfer.

tags() ->
    [<<"File Transfer">>].

roots() -> [file_transfer].

fields(file_transfer) ->
    [
        {storage, #{
            type => hoconsc:union([
                hoconsc:ref(?MODULE, local_storage)
            ])
        }}
    ];
fields(local_storage) ->
    [
        {type, #{
            type => local,
            default => local,
            required => false,
            desc => ?DESC("local")
        }},
        {root, #{
            type => binary(),
            desc => ?DESC("local_storage_root"),
            required => false
        }}
    ].

schema(filemeta) ->
    #{
        roots => [
            {name, hoconsc:mk(string(), #{required => true})},
            {size, hoconsc:mk(non_neg_integer())},
            {expire_at, hoconsc:mk(non_neg_integer())},
            {checksum, hoconsc:mk({atom(), binary()}, #{converter => converter(checksum)})},
            {segments_ttl, hoconsc:mk(pos_integer())},
            {user_data, hoconsc:mk(json_value())}
        ]
    }.

converter(checksum) ->
    fun
        (undefined, #{}) ->
            undefined;
        ({sha256, Bin}, #{make_serializable := true}) ->
            _ = is_binary(Bin) orelse throw({expected_type, string}),
            _ = byte_size(Bin) =:= 32 orelse throw({expected_length, 32}),
            binary:encode_hex(Bin);
        (Hex, #{}) ->
            _ = is_binary(Hex) orelse throw({expected_type, string}),
            _ = byte_size(Hex) =:= 64 orelse throw({expected_length, 64}),
            {sha256, binary:decode_hex(Hex)}
    end.
