%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Utilities
-export([backend/1, encode/2, decode/2]).

%% Test-only helpers
-export([translate/1]).

-type json_value() ::
    null
    | boolean()
    | binary()
    | number()
    | [json_value()]
    | #{binary() => json_value()}.

-reflect_type([json_value/0]).

%% NOTE
%% This is rather conservative limit, mostly dictated by the filename limitations
%% on most filesystems. Even though, say, S3 does not have such limitations, it's
%% still useful to have a limit on the filename length, to avoid having to deal with
%% limits in the storage backends.
%% Usual realistic limit is 255 bytes actually, but we leave some room for backends
%% to spare.
-define(MAX_FILENAME_BYTELEN, 240).

-import(hoconsc, [ref/2, mk/2]).

namespace() -> file_transfer.

tags() ->
    [<<"File Transfer">>].

roots() -> [file_transfer].

fields(file_transfer) ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("enable"),
                    %% importance => ?IMPORTANCE_NO_DOC,
                    required => false,
                    default => false
                }
            )},
        {init_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    desc => ?DESC("init_timeout"),
                    required => false,
                    importance => ?IMPORTANCE_LOW,
                    default => <<"10s">>
                }
            )},
        {store_segment_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    desc => ?DESC("store_segment_timeout"),
                    required => false,
                    importance => ?IMPORTANCE_LOW,
                    default => <<"5m">>
                }
            )},
        {assemble_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    desc => ?DESC("assemble_timeout"),
                    required => false,
                    importance => ?IMPORTANCE_LOW,
                    default => <<"5m">>
                }
            )},
        {storage,
            mk(
                ref(storage_backend),
                #{
                    desc => ?DESC("storage_backend"),
                    required => false,
                    validator => validator(backend),
                    default => #{
                        <<"local">> => #{}
                    }
                }
            )}
    ];
fields(storage_backend) ->
    [
        {local,
            mk(
                ref(local_storage),
                #{
                    desc => ?DESC("local_storage"),
                    required => {false, recursively}
                }
            )}
    ];
fields(local_storage) ->
    [
        {segments,
            mk(
                ref(local_storage_segments),
                #{
                    desc => ?DESC("local_storage_segments"),
                    required => false,
                    default => #{
                        <<"gc">> => #{}
                    }
                }
            )},
        {exporter,
            mk(
                ref(local_storage_exporter_backend),
                #{
                    desc => ?DESC("local_storage_exporter_backend"),
                    required => false,
                    validator => validator(backend),
                    default => #{
                        <<"local">> => #{}
                    }
                }
            )}
    ] ++ common_backend_fields();
fields(local_storage_segments) ->
    [
        {root,
            mk(
                string(),
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
fields(local_storage_exporter_backend) ->
    [
        {local,
            mk(
                ref(local_storage_exporter),
                #{
                    desc => ?DESC("local_storage_exporter"),
                    required => {false, recursively}
                }
            )},
        {s3,
            mk(
                ref(s3_exporter),
                #{
                    desc => ?DESC("s3_exporter"),
                    required => {false, recursively}
                }
            )}
    ];
fields(local_storage_exporter) ->
    [
        {root,
            mk(
                string(),
                #{
                    desc => ?DESC("local_storage_exporter_root"),
                    required => false
                }
            )}
    ] ++ common_backend_fields();
fields(s3_exporter) ->
    emqx_s3_schema:fields(s3) ++ common_backend_fields();
fields(local_storage_segments_gc) ->
    [
        {interval,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    desc => ?DESC("storage_gc_interval"),
                    required => false,
                    default => <<"1h">>
                }
            )},
        {maximum_segments_ttl,
            mk(
                %% not used in a `receive ... after' block, just timestamp comparison
                emqx_schema:duration_s(),
                #{
                    desc => ?DESC("storage_gc_max_segments_ttl"),
                    required => false,
                    default => <<"24h">>
                }
            )},
        {minimum_segments_ttl,
            mk(
                %% not used in a `receive ... after' block, just timestamp comparison
                emqx_schema:duration_s(),
                #{
                    desc => ?DESC("storage_gc_min_segments_ttl"),
                    required => false,
                    default => <<"5m">>,
                    % NOTE
                    % This setting does not seem to be useful to an end-user.
                    hidden => true
                }
            )}
    ].

common_backend_fields() ->
    [
        {enable,
            mk(
                boolean(), #{
                    desc => ?DESC("backend_enable"),
                    importance => ?IMPORTANCE_NO_DOC,
                    required => false,
                    default => true
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
desc(local_storage_exporter_backend) ->
    "Exporter for the local file system storage backend";
desc(storage_backend) ->
    "Storage backend settings for file transfer";
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
    };
schema(command_response) ->
    #{
        roots => [
            {vsn, hoconsc:mk(string(), #{default => <<"0.1">>})},
            {topic, hoconsc:mk(string())},
            {packet_id, hoconsc:mk(pos_integer())},
            {reason_code, hoconsc:mk(non_neg_integer())},
            {reason_description, hoconsc:mk(binary())}
        ]
    }.

validator(filename) ->
    [
        fun(Value) ->
            Bin = unicode:characters_to_binary(Value),
            byte_size(Bin) =< ?MAX_FILENAME_BYTELEN orelse {error, max_length_exceeded}
        end,
        fun emqx_ft_fs_util:is_filename_safe/1
    ];
validator(backend) ->
    fun(Config) ->
        Enabled = maps:filter(fun(_, #{<<"enable">> := E}) -> E end, Config),
        case maps:to_list(Enabled) of
            [{_Type, _BackendConfig}] ->
                ok;
            _Conflicts = [_ | _] ->
                {error, multiple_enabled_backends};
            _None = [] ->
                {error, no_enabled_backend}
        end
    end.

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

%% Utilities

-spec backend(emqx_config:config()) ->
    {_Type :: atom(), emqx_config:config()}.
backend(Config) ->
    catch maps:foreach(fun emit_enabled/2, Config).

-spec emit_enabled(atom(), emqx_config:config()) ->
    no_return().
emit_enabled(Type, BConf = #{enable := Enabled}) ->
    Enabled andalso throw({Type, BConf}).

decode(SchemaName, Payload) when is_binary(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, Map} ->
            decode(SchemaName, Map);
        {error, Error} ->
            {error, {invalid_filemeta_json, Error}}
    end;
decode(SchemaName, Map) when is_map(Map) ->
    Schema = schema(SchemaName),
    try
        Meta = hocon_tconf:check_plain(Schema, Map, #{atom_key => true, required => false}),
        {ok, Meta}
    catch
        throw:{_Schema, Errors} ->
            {error, {invalid_filemeta, Errors}}
    end.

encode(SchemaName, Map = #{}) ->
    Schema = schema(SchemaName),
    hocon_tconf:make_serializable(Schema, emqx_utils_maps:binary_key_map(Map), #{}).

%% Test-only helpers

-spec translate(emqx_config:raw_config()) ->
    emqx_config:config().
translate(Conf) ->
    [Root] = roots(),
    RootRaw = atom_to_binary(Root),
    ConfChecked = hocon_tconf:check_plain(?MODULE, #{RootRaw => Conf}, #{}, [Root]),
    emqx_utils_maps:unsafe_atom_key_map(maps:get(RootRaw, ConfChecked)).
