
etc/vm.args for C1000K
======================

::

    ## 12 threads/core.
    +A 64

    ## Max process numbers > connections * 2
    ## 2M
    +P 2097152

    ## Sets the maximum number of simultaneously existing ports for this system
    ## 1M
    +Q 1048576

    ## Increase number of concurrent ports/sockets, deprecated in R17
    -env ERL_MAX_PORTS 1048576

    -env ERTS_MAX_PORTS 1048576

    ## Mnesia and SSL will create temporary ets tables
    ## 16K
    -env ERL_MAX_ETS_TABLES 16384

