etc/app.config for C1000K
=========================

::

    {mqtt, 1883, [
        %% Size of acceptor pool
        {acceptors, 64},

        %% Maximum number of concurrent clients
        {max_clients, 1000000},

        %% Socket Access Control
        {access, [{allow, all}]},

        %% Connection Options
        {connopts, [
            %% Rate Limit. Format is 'burst, rate', Unit is KB/Sec
            %% {rate_limit, "100,10"} %% 100K burst, 10K rate
        ]},

        %% Socket Options
        {sockopts, [
            %Tune buffer if hight thoughtput
            %{recbuf, 4096},
            %{sndbuf, 4096},
            %{buffer, 4096},
            %{nodelay, true},
            {backlog, 1024}
        ]}
    ]},

