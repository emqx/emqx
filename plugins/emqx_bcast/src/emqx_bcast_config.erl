%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_config).

-export([load/0, update/1, get/1, get/2]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_MSG_TTL, 15 * 86400).
-define(DEFAULT_CLEANUP_INTERVAL, 60).

%% The plugin config namespace is keyed by name-vsn (e.g. "emqx_bcast-0.1.1");
%% derive it from the app version so it always matches the installed package.
%% application:get_key/2 returns the version as a charlist.
name_vsn() ->
    Name = atom_to_binary(?APP),
    Vsn =
        case application:get_key(?APP, vsn) of
            {ok, V} -> iolist_to_binary(V);
            _ -> <<"0.0.0">>
        end,
    <<Name/binary, "-", Vsn/binary>>.

-spec load() -> ok.
load() ->
    Config =
        try emqx_plugins:get_config(name_vsn(), #{}) of
            C -> C
        catch
            _:_ -> #{}
        end,
    update(Config).

-spec update(map()) -> ok.
update(Config) ->
    persistent_term:put({?APP, config}, normalize(Config)),
    %% Re-schedule the cleanup timer so a changed cleanup_interval takes
    %% effect without a node restart. The gen_server only runs on core
    %% nodes, so catch the cast on replicants.
    catch gen_server:cast(emqx_bcast_cleanup, reschedule),
    ok.

%% Single access point for normalized plugin config. Consumer modules must
%% use this instead of persistent_term:get + their own defaults; that kept
%% five copies of the default values in sync and let readers bypass the
%% per-device quota clamp.
-spec get(atom()) -> term().
get(Key) ->
    maps:get(Key, config()).

-spec get(atom(), term()) -> term().
get(Key, Default) ->
    maps:get(Key, config(), Default).

config() ->
    try persistent_term:get({?APP, config}, #{}) of
        Cfg when map_size(Cfg) > 0 -> Cfg;
        _ -> normalize(defaults())
    catch
        _:_ -> normalize(defaults())
    end.

defaults() ->
    #{
        <<"broadcast_topic">> => <<"/sys/broadcast/${productKey}">>,
        <<"batch_topic">> => <<"/${productKey}/${deviceName}/user/get">>,
        <<"msg_ttl">> => <<"15d">>,
        <<"cleanup_interval">> => <<"60s">>,
        <<"max_device_count">> => 10000,
        <<"max_message_size_broadcast">> => 65536,
        <<"max_message_size_batch">> => 10240,
        <<"max_pending_deliveries">> => 10000000,
        <<"max_pending_deliveries_per_device">> => 100,
        <<"msg_warn_threshold">> => 100000,
        <<"delivery_pool_size">> => 0
    }.

%% The plugin config map from emqx_plugins uses binary keys (JSON-decoded),
%% while the normalized config in persistent_term uses atoms.
normalize(Config) ->
    Defaults = defaults(),
    #{
        broadcast_topic => topic_or(<<"broadcast_topic">>, Config, Defaults),
        batch_topic => topic_or(<<"batch_topic">>, Config, Defaults),
        msg_ttl => duration_to_sec(msg_ttl, bin_or(<<"msg_ttl">>, Config, Defaults)),
        cleanup_interval => duration_to_sec(
            cleanup_interval, bin_or(<<"cleanup_interval">>, Config, Defaults)
        ),
        max_device_count => num_or(<<"max_device_count">>, Config, Defaults),
        max_message_size_broadcast => num_or(
            <<"max_message_size_broadcast">>, Config, Defaults
        ),
        max_message_size_batch => num_or(<<"max_message_size_batch">>, Config, Defaults),
        max_pending_deliveries => num_or(<<"max_pending_deliveries">>, Config, Defaults),
        max_pending_deliveries_per_device => clamp_per_device(
            num_or(<<"max_pending_deliveries_per_device">>, Config, Defaults)
        ),
        msg_warn_threshold => num_or(<<"msg_warn_threshold">>, Config, Defaults),
        delivery_pool_size => pool_size(num_or(<<"delivery_pool_size">>, Config, Defaults))
    }.

%% A null (or otherwise mistyped) value in the stored plugin config - e.g. a
%% field cleared in the dashboard persists as null - must not leak into the
%% runtime config: fall back to the default.
bin_or(Key, Config, Defaults) ->
    case maps:get(Key, Config, maps:get(Key, Defaults)) of
        V when is_binary(V) -> V;
        _ -> maps:get(Key, Defaults)
    end.

num_or(Key, Config, Defaults) ->
    case maps:get(Key, Config, maps:get(Key, Defaults)) of
        V when is_integer(V) -> V;
        _ -> maps:get(Key, Defaults)
    end.

%% Configured topic templates take the default-topic path, which skips the
%% per-request template validation. Apply the same rules here (no wildcards,
%% no unknown placeholders) so a bad template fails loudly at config time
%% instead of breaking every publish at runtime.
topic_or(Key, Config, Defaults) ->
    Template = bin_or(Key, Config, Defaults),
    case valid_topic_template(Template) of
        true ->
            Template;
        false ->
            ?SLOG(warning, #{
                msg => invalid_plugin_config_topic_template,
                field => Key,
                value => Template,
                default => maps:get(Key, Defaults)
            }),
            maps:get(Key, Defaults)
    end.

valid_topic_template(Template) ->
    case binary:match(Template, [<<"+">>, <<"#">>]) of
        nomatch -> known_placeholders_only(Template);
        _ -> false
    end.

known_placeholders_only(Template) ->
    Rest0 = binary:replace(Template, <<"${productKey}">>, <<>>, [global]),
    Rest = binary:replace(Rest0, <<"${deviceName}">>, <<>>, [global]),
    binary:match(Rest, <<"${">>) =:= nomatch.

%% Per-device quota is bounded to [10, 200] so an operator cannot
%% accidentally disable the protection or configure an unbounded value.
%% Values outside the range are clamped, with a warning: a silent rewrite
%% would make the config surface lie about the effective value.
clamp_per_device(N) when is_integer(N), N >= 10, N =< 200 ->
    N;
clamp_per_device(N) when is_integer(N), N < 10 ->
    clamp_warn(N, 10),
    10;
clamp_per_device(N) ->
    clamp_warn(N, 200),
    200.

clamp_warn(Configured, Effective) ->
    ?SLOG(warning, #{
        msg => per_device_quota_clamped,
        field => max_pending_deliveries_per_device,
        configured => Configured,
        effective => Effective
    }).

pool_size(0) -> erlang:system_info(schedulers);
pool_size(N) when is_integer(N), N > 0 -> N;
pool_size(_) -> erlang:system_info(schedulers).

duration_to_sec(Field, Value) when is_binary(Value) ->
    case parse_duration(Value) of
        {ok, Sec} ->
            Sec;
        error ->
            Default = field_default(Field),
            ?SLOG(warning, #{
                msg => "invalid_plugin_config_duration",
                field => Field,
                value => Value,
                default => Default
            }),
            Default
    end;
duration_to_sec(_Field, Value) when is_integer(Value), Value > 0 ->
    Value;
duration_to_sec(Field, Value) ->
    Default = field_default(Field),
    ?SLOG(warning, #{
        msg => "invalid_plugin_config_duration",
        field => Field,
        value => Value,
        default => Default
    }),
    Default.

parse_duration(TTL) ->
    case re:run(TTL, <<"^(\\d+)([smhd])$">>, [{capture, [1, 2], binary}]) of
        {match, [N, <<"s">>]} -> {ok, binary_to_integer(N)};
        {match, [N, <<"m">>]} -> {ok, binary_to_integer(N) * 60};
        {match, [N, <<"h">>]} -> {ok, binary_to_integer(N) * 3600};
        {match, [N, <<"d">>]} -> {ok, binary_to_integer(N) * 86400};
        _ -> error
    end.

field_default(msg_ttl) -> ?DEFAULT_MSG_TTL;
field_default(cleanup_interval) -> ?DEFAULT_CLEANUP_INTERVAL.
