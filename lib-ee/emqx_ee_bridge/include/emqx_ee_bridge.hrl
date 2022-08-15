-define(METRICS(MATCH, SUCC, FAILED, RATE, RATE_5, RATE_MAX), #{
    matched => MATCH,
    success => SUCC,
    failed => FAILED,
    rate => RATE,
    rate_last5m => RATE_5,
    rate_max => RATE_MAX
}).

-define(METRICS_EXAMPLE, #{
    metrics => ?METRICS(0, 0, 0, 0, 0, 0),
    node_metrics => [
        #{
            node => node(),
            metrics => ?METRICS(0, 0, 0, 0, 0, 0)
        }
    ]
}).
