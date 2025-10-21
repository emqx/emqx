The signature of the `message.delivered`, `message.acked` and `client.timeout` callbacks has changed.  Plugins that rely on these callbacks will need to update their code to continue functioning properly.

For `message.delivered` and `message.acked`, where a client info map was previously passed down to the callback, now it's an open map with currently the following structure: `#{chan_info_fn => ChanInfoFn, session_info_fn => SessionInfoFn}`, where `ChanInfoFn` and `SessionInfoFn` behave as `emqx_channel:info/2` and `emqx_session:info/2` applied to the current channel and session states, respectively.

For `client.timeout`, the same open context map describe above is passed down as a new argument.
