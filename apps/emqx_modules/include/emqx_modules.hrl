%% The destination URL for the telemetry data report
-define(TELEMETRY_URL, "https://telemetry.emqx.io/api/telemetry").

%% Interval for reporting telemetry data, Default: 7d
-define(REPORT_INTERVAR, 604800).

-define(BASE_TOPICS, [<<"$event/client_connected">>,
                      <<"$event/client_disconnected">>,
                      <<"$event/session_subscribed">>,
                      <<"$event/session_unsubscribed">>,
                      <<"$event/message_delivered">>,
                      <<"$event/message_acked">>,
                      <<"$event/message_dropped">>]).
