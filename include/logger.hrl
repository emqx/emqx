%%--------------------------------------------------------------------
%% The args are put into report_cb for lazy evaluation.
%%--------------------------------------------------------------------
-define(LOG_LZ(Level, Format, Args),
        begin
          (logger:log(Level,#{},#{report_cb =>
                                    fun(_) ->
                                        {(Format), (Args)}
                                    end}))
        end).