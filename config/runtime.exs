import Config
import EmqxConfigHelper.Hocon
import EmqxConfigHelper.Cuttlefish

hocon :emqx_prometheus,
  schema_module: :emqx_prometheus_schema,
  config_file: "etc/plugins/emqx_prometheus.conf"

cuttlefish :emqx_sn,
  schema_file: "emqx_sn.schema",
  config_file: "etc/plugins/emqx_sn.conf"

config :mnesia, dir: '/tmp/mnesia'
