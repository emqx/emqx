-define(APP, emqx_statsd).

-define(DEFAULT_HOST, {127, 0, 0, 1}).
-define(DEFAULT_PORT, 8125).
-define(DEFAULT_PREFIX, undefined).
-define(DEFAULT_TAGS, #{}).
-define(DEFAULT_BATCH_SIZE, 10).
-define(DEFAULT_SAMPLE_TIME_INTERVAL, 10).
-define(DEFAULT_FLUSH_TIME_INTERVAL, 10).