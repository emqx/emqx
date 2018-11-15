%%--------------------------------------------------------------------
%% Logs with header
%%--------------------------------------------------------------------
-ifdef(LOG_HEADER).
%% with header
-define(LOG(Level, Format, Args),
        begin
          (emqx_logger:Level(#{},#{report_cb =>
                                    fun(_) ->
                                        {?LOG_HEADER ++ " "++ (Format), (Args)}
                                    end}))
        end).
-else.
%% without header
-define(LOG(Level, Format, Args),
        emqx_logger:Level(Format, Args)).
-endif.