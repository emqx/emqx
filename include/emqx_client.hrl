
%%-define(CLIENT_IN_BROKER, true).

%% Default timeout
-define(DEFAULT_KEEPALIVE,       60000).
-define(DEFAULT_ACK_TIMEOUT,     20000).
-define(DEFAULT_CONNECT_TIMEOUT, 30000).
-define(DEFAULT_TCP_OPTIONS,
        [binary, {packet, raw}, {active, false},
         {nodelay, true}, {reuseaddr, true}]).

-ifdef(CLIENT_IN_BROKER).

-define(LOG(Level, Msg), emqx_log:Level(Msg)).
-define(LOG(Level, Format, Args), emqx_log:Level(Format, Args)).

-else.

-define(LOG(Level, Msg),
        (case Level of
             debug    -> error_logger:info_msg(Msg);
             info     -> error_logger:info_msg(Msg);
             warning  -> error_logger:warning_msg(Msg);
             error    -> error_logger:error_msg(Msg);
             critical -> error_logger:error_msg(Msg)
         end)).
-define(LOG(Level, Format, Args),
        (case Level of
             debug    -> error_logger:info_msg(Format, Args);
             info     -> error_logger:info_msg(Format, Args);
             warning  -> error_logger:warning_msg(Format, Args);
             error    -> error_logger:error_msg(Format, Args);
             critical -> error_logger:error_msg(Format, Args)
         end)).

-endif.

