%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% NOTE
%% We share the shard with `emqx_auth_mnesia` to ensure backward compatibility
%% with EMQX 5.2.x and earlier.
-define(AUTHN_SHARD, emqx_authn_shard).

%% Management
-export([put_device/1, get_device/1, remove_device/1]).
%% Management: import
-export([import_devices/1]).
%% Authentication
-export([get_device_actual_keys/1]).
%% Internal API
-export([create_table/0, clear_table/0, format_device/1]).

-ifdef(TEST).
-export([config_topic/1]).
% to avoid test flakiness
-define(ACTIVITY, sync_dirty).
-else.
-define(ACTIVITY, async_dirty).
-endif.

-type deviceid() :: binary().
-type project() :: binary().
-type location() :: binary().
-type registry() :: binary().
-type device_loc() :: {project(), location(), registry()}.
-type key_type() :: binary().
-type key() :: binary().
-type expires_at() :: pos_integer().
-type key_record() :: {key_type(), key(), expires_at()}.
-type created_at() :: pos_integer().
-type extra() :: map().

-record(emqx_gcp_device, {
    id :: deviceid(),
    keys :: [key_record()],
    device_loc :: device_loc(),
    created_at :: created_at(),
    extra :: extra()
}).
-type emqx_gcp_device() :: #emqx_gcp_device{}.

-type formatted_key() ::
    #{
        key_type := key_type(),
        key := key(),
        expires_at := expires_at()
    }.
-type encoded_config() :: binary().
-type formatted_device() ::
    #{
        deviceid := deviceid(),
        keys := [formatted_key()],
        config := encoded_config(),
        project => project(),
        location => location(),
        registry => registry(),
        created_at => created_at()
    }.
-export_type([formatted_device/0, deviceid/0, encoded_config/0]).

-define(TAB, ?MODULE).

-dialyzer({nowarn_function, perform_dirty/2}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec put_device(formatted_device()) -> ok.
put_device(FormattedDevice) ->
    try
        perform_dirty(?ACTIVITY, fun() -> put_device_no_transaction(FormattedDevice) end)
    catch
        _Error:Reason ->
            ?SLOG(error, #{
                msg => "failed_to_put_device",
                device => FormattedDevice,
                reason => Reason
            }),
            {error, Reason}
    end.

-spec get_device(deviceid()) -> {ok, formatted_device()} | not_found.
get_device(DeviceId) ->
    case ets:lookup(?TAB, DeviceId) of
        [] ->
            not_found;
        [Device] ->
            {ok, format_device(Device)}
    end.

-spec remove_device(deviceid()) -> ok.
remove_device(DeviceId) ->
    ok = mria:dirty_delete({?TAB, DeviceId}),
    ok = put_config(DeviceId, <<>>).

-spec get_device_actual_keys(deviceid()) -> [key()] | not_found.
get_device_actual_keys(DeviceId) ->
    try ets:lookup(?TAB, DeviceId) of
        [] ->
            not_found;
        [Device] ->
            actual_keys(Device)
    catch
        error:badarg ->
            not_found
    end.

-spec import_devices([formatted_device()]) ->
    {NumImported :: non_neg_integer(), NumError :: non_neg_integer()}.
import_devices(FormattedDevices) when is_list(FormattedDevices) ->
    perform_dirty(fun() -> lists:foldl(fun import_device/2, {0, 0}, FormattedDevices) end).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

-spec create_table() -> ok.
create_table() ->
    ok = mria:create_table(?TAB, [
        {rlog_shard, ?AUTHN_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, emqx_gcp_device},
        {attributes, record_info(fields, emqx_gcp_device)},
        {storage_properties, [{ets, [{read_concurrency, true}]}, {dets, [{auto_save, 10_000}]}]}
    ]),
    ok = mria:wait_for_tables([?TAB]).

-spec clear_table() -> ok.
clear_table() ->
    {atomic, ok} = mria:clear_table(?TAB),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec perform_dirty(function()) -> term().
perform_dirty(Fun) ->
    perform_dirty(?ACTIVITY, Fun).

-spec perform_dirty(async_dirty | sync_dirty, function()) -> term().
perform_dirty(async_dirty, Fun) ->
    mria:async_dirty(?AUTHN_SHARD, Fun);
perform_dirty(sync_dirty, Fun) ->
    mria:sync_dirty(?AUTHN_SHARD, Fun).

-spec put_device_no_transaction(formatted_device()) -> ok.
put_device_no_transaction(
    #{
        deviceid := DeviceId,
        keys := Keys,
        config := EncodedConfig
    } = Device
) ->
    DeviceLoc =
        list_to_tuple([maps:get(Key, Device, <<>>) || Key <- [project, location, registry]]),
    ok = put_device_no_transaction(DeviceId, DeviceLoc, Keys),
    ok = put_config(DeviceId, EncodedConfig).

-spec put_device_no_transaction(deviceid(), device_loc(), [key()]) -> ok.
put_device_no_transaction(DeviceId, DeviceLoc, Keys) ->
    CreatedAt = erlang:system_time(second),
    Extra = #{},
    Device =
        #emqx_gcp_device{
            id = DeviceId,
            keys = formatted_keys_to_records(Keys),
            device_loc = DeviceLoc,
            created_at = CreatedAt,
            extra = Extra
        },
    mnesia:write(Device).

-spec formatted_keys_to_records([formatted_key()]) -> [key_record()].
formatted_keys_to_records(Keys) ->
    lists:map(fun formatted_key_to_record/1, Keys).

-spec formatted_key_to_record(formatted_key()) -> key_record().
formatted_key_to_record(#{
    key_type := KeyType,
    key := Key,
    expires_at := ExpiresAt
}) ->
    {KeyType, Key, ExpiresAt}.

-spec format_device(emqx_gcp_device()) -> formatted_device().
format_device(#emqx_gcp_device{
    id = DeviceId,
    device_loc = {Project, Location, Registry},
    keys = Keys,
    created_at = CreatedAt
}) ->
    #{
        deviceid => DeviceId,
        project => Project,
        location => Location,
        registry => Registry,
        keys => lists:map(fun format_key/1, Keys),
        created_at => CreatedAt,
        config => base64:encode(get_device_config(DeviceId))
    }.

-spec format_key(key_record()) -> formatted_key().
format_key({KeyType, Key, ExpiresAt}) ->
    #{
        key_type => KeyType,
        key => Key,
        expires_at => ExpiresAt
    }.

-spec put_config(deviceid(), encoded_config()) -> ok.
put_config(DeviceId, EncodedConfig) ->
    Config = base64:decode(EncodedConfig),
    Topic = config_topic(DeviceId),
    Message = emqx_message:make(DeviceId, 1, Topic, Config, #{retain => true}, #{}),
    _ = emqx_broker:publish(Message),
    ok.

-spec get_device_config(deviceid()) -> emqx_types:payload().
get_device_config(DeviceId) ->
    Topic = config_topic(DeviceId),
    get_retained_payload(Topic).

-spec actual_keys(emqx_gcp_device()) -> [key()].
actual_keys(#emqx_gcp_device{keys = Keys}) ->
    Now = erlang:system_time(second),
    [Key || {_KeyType, Key, ExpiresAt} <- Keys, ExpiresAt == 0 orelse ExpiresAt >= Now].

-spec import_device(formatted_device(), {
    NumImported :: non_neg_integer(), NumError :: non_neg_integer()
}) -> {NumImported :: non_neg_integer(), NumError :: non_neg_integer()}.
import_device(Device, {NumImported, NumError}) ->
    try
        ok = put_device_no_transaction(Device),
        {NumImported + 1, NumError}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "failed_to_import_device",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {NumImported, NumError + 1}
    end.

-spec get_retained_payload(binary()) -> emqx_types:payload().
get_retained_payload(Topic) ->
    case emqx_retainer:read_message(Topic) of
        {ok, []} ->
            <<>>;
        {ok, [Message]} ->
            Message#message.payload
    end.

-spec config_topic(deviceid()) -> binary().
config_topic(DeviceId) ->
    <<"/devices/", DeviceId/binary, "/config">>.
