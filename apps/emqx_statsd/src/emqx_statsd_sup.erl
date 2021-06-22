%%%-------------------------------------------------------------------
%% @doc emqx_statsd top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_statsd_sup).

-behaviour(supervisor).

-include("emqx_statsd.hrl").

-export([start_link/0]).

-export([start_statsd/0, stop_statsd/0]).

-export([init/1]).

-export([estatsd_options/0]).

 start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

 init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

start_statsd() ->
    {ok, Pid} = supervisor:start_child(?MODULE, estatsd_child_spec()),
    {ok, _Pid1} = supervisor:start_child(?MODULE, emqx_statsd_child_spec(Pid)).

stop_statsd() ->
    ok = supervisor:terminate_child(?MODULE, emqx_statsd),
    ok = supervisor:terminate_child(?MODULE, estatsd).
%%==============================================================================================
%% internal
estatsd_child_spec() ->
    #{id       => estatsd
    , start    => {estatsd, start_link, [estatsd_options()]}
    , restart  => permanent
    , shutdown => 5000
    , type     => worker
    , modules  => [estatsd]}.

estatsd_options() ->
    {Host, Port} =  get_conf(server, {?DEFAULT_HOST, ?DEFAULT_PORT}),
    Prefix = get_conf(prefix, ?DEFAULT_PREFIX),
    Tags = tags(get_conf(tags, ?DEFAULT_TAGS)),
    BatchSize = get_conf(batch_size, ?DEFAULT_BATCH_SIZE),
    [{host, Host}, {port, Port}, {prefix, Prefix}, {tags, Tags}, {batch_size, BatchSize}].

tags(Map) ->
    Tags = maps:to_list(Map),
    [{atom_to_binary(Key, utf8), Value} || {Key, Value} <- Tags].

emqx_statsd_child_spec(Pid) ->
    #{id       => emqx_statsd
    , start    => {emqx_statsd, start_link, [[{estatsd_pid, Pid} | emqx_statsd_options()]]}
    , restart  => permanent
    , shutdown => 5000
    , type     => worker
    , modules  => [emqx_statsd]}.

emqx_statsd_options() ->
    SampleTimeInterval = get_conf(sample_time_interval, ?DEFAULT_SAMPLE_TIME_INTERVAL) * 1000,
    FlushTimeInterval = get_conf(flush_time_interval, ?DEFAULT_FLUSH_TIME_INTERVAL) * 1000,
    [{sample_time_interval, SampleTimeInterval}, {flush_time_interval, FlushTimeInterval}].

get_conf(Key, Default) ->
    emqx_config:get([?APP, Key], Default).
