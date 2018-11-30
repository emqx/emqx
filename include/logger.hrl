%%--------------------------------------------------------------------
%% Logs with a header prefixed to the log message.
%% And the log args are puted into report_cb for lazy evaluation.
%%--------------------------------------------------------------------
-ifdef(LOG_HEADER).
%% with header
-define(LOG(Level, Format, Args),
        begin
          (logger:log(Level,#{},#{report_cb =>
                                    fun(_) ->
                                        {?LOG_HEADER ++ " "++ (Format), (Args)}
                                    end}))
        end).
-else.
%% without header
-define(LOG(Level, Format, Args),
        begin
          (logger:log(Level,#{},#{report_cb =>
                                    fun(_) ->
                                        {(Format), (Args)}
                                    end}))
        end).
-endif.