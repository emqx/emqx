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

 start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

 init([]) ->
    {ok, { {one_for_one, 10, 100}, []} }.

start_statsd() ->
    {ok, Pid} = supervisor:start_child(?MODULE, estatsd_child_spec()),
    {ok, _Pid1} = supervisor:start_child(?MODULE, emqx_statsd_child_spec(Pid)).

stop_statsd() ->
    supervisor:terminate_child(emqx_statsd_sup, emqx_statsd),
    supervisor:terminate_child(emqx_statsd_sup, estatsd).
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
    Host = application:get_env(?APP, host, ?DEFAULT_HOST),
    Port = application:get_env(?APP, port, ?DEFAULT_PORT),
    Prefix = application:get_env(?APP, prefix, ?DEFAULT_PREFIX),
    Tags = application:get_env(?APP, tags, ?DEFAULT_TAGS),
    BatchSize = application:get_env(?APP, batch_size, ?DEFAULT_BATCH_SIZE),
    [{host, Host}, {port, Port}, {prefix, Prefix}, {tags, Tags}, {batch_size, BatchSize}].

emqx_statsd_child_spec(Pid) ->
    #{id       => emqx_statsd
    , start    => {emqx_statsd, start_link, [emqx_statsd_options(Pid)]}
    , restart  => permanent
    , shutdown => 5000
    , type     => worker
    , modules  => [emqx_statsd]}.

emqx_statsd_options(Pid) ->
    SampleTimeInterval = application:get_env(?APP, sample_time_interval, ?DEFAULT_SAMPLE_TIME_INTERVAL),
    FlushTimeInterval = application:get_env(?APP, flush_time_interval, ?DEFAULT_FLUSH_TIME_INTERVAL),
    [{estatsd_pid, Pid}, {sample_time_interval, SampleTimeInterval}, {flush_time_interval, FlushTimeInterval}].
