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

-export([namespace/0, roots/0, fields/1, tags/0, desc/1]).

-export([schema/1]).

-type json_value() ::
    null
    | boolean()
    | binary()
    | number()
    | [json_value()]
    | #{binary() => json_value()}.

-reflect_type([json_value/0]).

-import(hoconsc, [ref/2, mk/2]).

namespace() -> file_transfer.

tags() ->
    [<<"File Transfer">>].

roots() -> [file_transfer].

fields(file_transfer) ->
    [
        {init_timeout,
            mk(
                emqx_schema:duration_ms(),
                #{
                    desc => ?DESC("init_timeout"),
                    required => false,
                    default => "10s"
                }
            )},
        {store_segment_timeout,
            mk(
                emqx_schema:duration_ms(),
                #{
                    desc => ?DESC("store_segment_timeout"),
                    required => false,
                    default => "5m"
                }
            )},
        {assemble_timeout,
            mk(
                emqx_schema:duration_ms(),
                #{
                    desc => ?DESC("assemble_timeout"),
                    required => false,
                    default => "5m"
                }
            )},
        {storage,
            mk(
                hoconsc:union([
                    ref(local_storage)
                ]),
                #{
                    required => false,
                    desc => ?DESC("storage"),
                    default => default_storage()
                }
            )}
    ];
fields(local_storage) ->
    [
        {type,
            mk(
                local,
                #{
                    default => local,
                    required => false,
                    desc => ?DESC("local_type")
                }
            )},
        {segments,
            mk(
                ref(local_storage_segments),
                #{
                    desc => ?DESC("local_storage_segments"),
                    required => false
                }
            )},
        {exporter,
            mk(
                hoconsc:union([
                    ref(local_storage_exporter),
                    ref(s3_exporter)
                ]),
                #{
                    desc => ?DESC("local_storage_exporter"),
                    required => true
                }
            )}
    ];
fields(local_storage_segments) ->
    [
        {root,
            mk(
                binary(),
                #{
                    desc => ?DESC("local_storage_segments_root"),
                    required => false
                }
            )},
        {gc,
            mk(
                ref(local_storage_segments_gc), #{
                    desc => ?DESC("local_storage_segments_gc"),
                    required => false
                }
            )}
    ];
fields(local_storage_exporter) ->
    [
        {type,
            mk(
                local,
                #{
                    default => local,
                    required => false,
                    desc => ?DESC("local_storage_exporter_type")
                }
            )},
        {root,
            mk(
                binary(),
                #{
                    desc => ?DESC("local_storage_exporter_root"),
                    required => false
                }
            )}
    ];
fields(s3_exporter) ->
    [
        {type,
            mk(
                s3,
                #{
                    default => s3,
                    required => false,
                    desc => ?DESC("s3_exporter_type")
                }
            )}
    ] ++
        emqx_s3_schema:fields(s3);
fields(local_storage_segments_gc) ->
    [
        {interval,
            mk(
                emqx_schema:duration_ms(),
                #{
                    desc => ?DESC("storage_gc_interval"),
                    required => false,
                    default => "1h"
                }
            )},
        {maximum_segments_ttl,
            mk(
                emqx_schema:duration_s(),
                #{
                    desc => ?DESC("storage_gc_max_segments_ttl"),
                    required => false,
                    default => "24h"
                }
            )},
        {minimum_segments_ttl,
            mk(
                emqx_schema:duration_s(),
                #{
                    desc => ?DESC("storage_gc_min_segments_ttl"),
                    required => false,
                    default => "5m",
                    % NOTE
                    % This setting does not seem to be useful to an end-user.
                    hidden => true
                }
            )}
    ].

desc(file_transfer) ->
    "File transfer settings";
desc(local_storage) ->
    "File transfer local storage settings";
desc(local_storage_segments) ->
    "File transfer local segments storage settings";
desc(local_storage_exporter) ->
    "Local Exporter settings for the File transfer local storage backend";
desc(s3_exporter) ->
    "S3 Exporter settings for the File transfer local storage backend";
desc(local_storage_segments_gc) ->
    "Garbage collection settings for the File transfer local segments storage";
desc(_) ->
    undefined.

schema(filemeta) ->
    #{
        roots => [
            {name,
                hoconsc:mk(string(), #{
                    required => true,
                    validator => validator(filename),
                    converter => converter(unicode_string)
                })},
            {size, hoconsc:mk(non_neg_integer())},
            {expire_at, hoconsc:mk(non_neg_integer())},
            {checksum, hoconsc:mk({atom(), binary()}, #{converter => converter(checksum)})},
            {segments_ttl, hoconsc:mk(pos_integer())},
            {user_data, hoconsc:mk(json_value())}
        ]
    }.

validator(filename) ->
    fun emqx_ft_fs_util:is_filename_safe/1.

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
    end;
converter(unicode_string) ->
    fun
        (undefined, #{}) ->
            undefined;
        (Str, #{make_serializable := true}) ->
            _ = is_list(Str) orelse throw({expected_type, string}),
            unicode:characters_to_binary(Str);
        (Str, #{}) ->
            _ = is_binary(Str) orelse throw({expected_type, string}),
            unicode:characters_to_list(Str)
    end.

ref(Ref) ->
    ref(?MODULE, Ref).

default_storage() ->
    #{
        <<"type">> => <<"local">>,
        <<"exporter">> => #{
            <<"type">> => <<"local">>
        }
    }.
