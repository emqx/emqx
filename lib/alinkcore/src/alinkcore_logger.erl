-module(alinkcore_logger).

%% API
-export([log/3]).

log(Level, Fmt, Args) ->
    logger:log(Level, Fmt, Args).
