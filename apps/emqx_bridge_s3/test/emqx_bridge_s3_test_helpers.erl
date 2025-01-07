%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_utils_conv, [bin/1]).

parse_and_check_config(Root, Type, Name, Config) ->
    Schema =
        case Root of
            <<"connectors">> -> emqx_connector_schema;
            <<"actions">> -> emqx_bridge_v2_schema
        end,
    #{Root := #{Type := #{Name := _ConfigParsed}}} =
        hocon_tconf:check_plain(
            Schema,
            #{Root => #{Type => #{Name => Config}}},
            #{required => false, atom_key => false}
        ),
    Config.

mk_message_event(ClientId, Topic, Payload) ->
    Message = emqx_message:make(bin(ClientId), bin(Topic), Payload),
    {Event, _} = emqx_rule_events:eventmsg_publish(Message),
    emqx_utils_maps:binary_key_map(Event).

create_bucket(Bucket) ->
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    erlcloud_s3:create_bucket(Bucket, AwsConfig).

list_objects(Bucket) ->
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    Response = erlcloud_s3:list_objects(Bucket, AwsConfig),
    false = proplists:get_value(is_truncated, Response),
    Contents = proplists:get_value(contents, Response),
    lists:map(fun maps:from_list/1, Contents).

get_object(Bucket, Key) ->
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    maps:from_list(erlcloud_s3:get_object(Bucket, Key, AwsConfig)).

list_pending_uploads(Bucket, Key) ->
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    {ok, Props} = erlcloud_s3:list_multipart_uploads(Bucket, [{prefix, Key}], [], AwsConfig),
    Uploads = proplists:get_value(uploads, Props),
    lists:map(fun maps:from_list/1, Uploads).
