
Linux Kernel Tuning for C1000K concurrent connections::

    # system-wide limit on max opened files for all processes
    # sysctl -n fs.nr_open
    # 1M
    sysctl -w fs.file-max=1048576
    sysctl -w fs.nr_open=1048576
    ulimit -n 1048576
    # Increase number of incoming connections
    net.core.somaxconn = 65536

    sysctl -w net.ipv4.tcp_mem='10000000 10000000 10000000'
    sysctl -w net.ipv4.tcp_rmem='1024 4096 16384'
    sysctl -w net.ipv4.tcp_wmem='1024 4096 16384'
    sysctl -w net.core.rmem_max=16384
    sysctl -w net.core.wmem_max=16384

    net.core.rmem_max = 16777216
    net.core.wmem_max = 16777216
    net.core.rmem_default = 16777216
    net.core.wmem_default = 16777216
    net.core.optmem_max = 2048000
    net.core.netdev_max_backlog = 50000

    net.ipv4.tcp_rmem = 4096 4096 16777216
    net.ipv4.tcp_wmem = 4096 4096 16777216
    net.ipv4.tcp_max_syn_backlog = 30000
    net.ipv4.tcp_max_tw_buckets = 2000000
    net.ipv4.tcp_tw_reuse = 1
    net.ipv4.tcp_fin_timeout = 10
    net.ipv4.conf.all.send_redirects = 0
    net.ipv4.conf.all.accept_redirects = 0
    net.ipv4.conf.all.accept_source_route = 0
    net.ipv4.tcp_slow_start_after_idle = 0
    net.ipv4.ip_local_port_range = 8000 65535

    net.netfilter.nf_conntrack_max = 1000000
    net.netfilter.nf_conntrack_tcp_timeout_time_wait = 30

    vm.min_free_kbytes = 65536
    vm.swappiness = 0
    vm.overcommit_memory = 1


/etc/sysctl.conf
----------------

::

  fs.file-max = 1048576


/etc/security/limits.conf
-------------------------

::

    *      soft   nofile      1048576
    *      hard   nofile      1048576


  ## Kernel/Network Tunings from @galvezlj

  Server side:
  ```
  fs.file-max = 1000000
  net.core.somaxconn = 65536
  net.ipv4.ip_local_port_range = 500 65535
  net.nf_conntrack_max = 1000000
  net.netfilter.nf_conntrack_max = 1000000
  net.ipv4.tcp_rmem = 4096 4096 16777216
  net.ipv4.tcp_wmem = 4096 4096 16777216
  ```

  Client side:

  ```
  sysctl -w net.ipv4.ip_local_port_range="500 65535"
  echo 1000000 > /proc/sys/fs/nr_open
  ```

  ## Kernel/Network Tunings from my benchmark server

  ```
  fs.file-max = 1000000

  net.core.somaxconn = 65536
  net.core.wmem_max = 124928
  net.core.rmem_max = 124928
  net.core.wmem_default = 124928
  net.core.rmem_default = 124928
  net.core.dev_weight = 64
  net.core.netdev_max_backlog = 1000
  net.core.message_cost = 5
  net.core.message_burst = 10
  net.core.optmem_max = 20480
  net.core.busy_poll = 0
  net.core.busy_read = 0
  net.core.netdev_budget = 300

  net.ipv4.tcp_timestamps = 1
  net.ipv4.tcp_window_scaling = 1
  net.ipv4.tcp_sack = 1
  net.ipv4.tcp_retrans_collapse = 1
  net.ipv4.ip_default_ttl = 64
  net.ipv4.tcp_syn_retries = 5
  net.ipv4.tcp_synack_retries = 5
  net.ipv4.tcp_max_orphans = 262144
  net.ipv4.tcp_max_tw_buckets = 262144
  net.ipv4.ip_dynaddr = 0
  net.ipv4.tcp_keepalive_time = 7200
  net.ipv4.tcp_keepalive_probes = 9
  net.ipv4.tcp_keepalive_intvl = 75
  net.ipv4.tcp_retries1 = 3
  net.ipv4.tcp_retries2 = 15
  net.ipv4.tcp_fin_timeout = 60
  net.ipv4.tcp_syncookies = 1
  net.ipv4.tcp_tw_recycle = 0
  net.ipv4.tcp_abort_on_overflow = 0
  net.ipv4.tcp_max_syn_backlog = 2048
  net.ipv4.ip_local_port_range = 32768  61000
  net.ipv4.inet_peer_threshold = 65664
  net.ipv4.inet_peer_minttl = 120
  net.ipv4.inet_peer_maxttl = 600
  net.ipv4.inet_peer_gc_mintime = 10
  net.ipv4.inet_peer_gc_maxtime = 120
  net.ipv4.tcp_mem = 3080640    4107520 6161280
  net.ipv4.tcp_wmem = 4096  16384   4194304
  net.ipv4.tcp_rmem = 4096  87380   4194304
  net.ipv4.tcp_app_win = 31
  net.ipv4.tcp_adv_win_scale = 2
  net.ipv4.tcp_tw_reuse = 0
  net.ipv4.tcp_frto = 2
  net.ipv4.tcp_frto_response = 0
  net.ipv4.tcp_low_latency = 0
  net.ipv4.tcp_no_metrics_save = 0
  net.ipv4.tcp_moderate_rcvbuf = 1
  net.ipv4.tcp_tso_win_divisor = 3
  net.ipv4.tcp_congestion_control = cubic
  net.ipv4.tcp_abc = 0
  net.ipv4.tcp_mtu_probing = 0
  net.ipv4.tcp_base_mss = 512
  net.ipv4.tcp_workaround_signed_windows = 0
  net.ipv4.tcp_challenge_ack_limit = 100
  net.ipv4.tcp_limit_output_bytes = 131072
  net.ipv4.tcp_dma_copybreak = 4096
  net.ipv4.tcp_slow_start_after_idle = 1
  net.ipv4.tcp_available_congestion_control = cubic reno
  net.ipv4.tcp_allowed_congestion_control = cubic reno
  net.ipv4.tcp_max_ssthresh = 0
  net.ipv4.tcp_thin_linear_timeouts = 0
  net.ipv4.tcp_thin_dupack = 0
  net.ipv4.tcp_min_tso_segs = 2
  net.ipv4.udp_mem = 3080640    4107520 6161280
  net.ipv4.udp_rmem_min = 4096
  net.ipv4.udp_wmem_min = 4096
  net.ipv4.conf.all.forwarding = 0
  net.ipv4.conf.all.mc_forwarding = 0
  net.ipv4.conf.all.accept_redirects = 1
  net.ipv4.conf.all.secure_redirects = 1
  net.ipv4.conf.all.shared_media = 1
  net.ipv4.conf.all.rp_filter = 0
  net.ipv4.conf.all.send_redirects = 1
  net.ipv4.conf.all.src_valid_mark = 0
  net.ipv4.conf.all.medium_id = 0
  net.ipv4.conf.all.bootp_relay = 0
  net.ipv4.conf.all.log_martians = 0
  net.ipv4.conf.all.tag = 0

  vm.min_free_kbytes = 67584
  vm.swappiness = 60
  vm.overcommit_memory = 0
  ```

