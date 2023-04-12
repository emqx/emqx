# EMQX Machine

This application manages other OTP applications in EMQX and serves as the entry point when BEAM VM starts up.
It prepares the node before starting mnesia/mria, as well as EMQX business logic.
It keeps track of the business applications storing data in Mnesia, which need to be restarted when the node joins the cluster by registering `ekka` callbacks.
