# EMQX Prometheus Agent

This application provides the ability to integrate with Prometheus. It provides
an HTTP API for collecting metrics of the current node
and also supports configuring a Push Gateway URL address for pushing these metrics.


More introduction about [Integrate with Prometheus](https://www.emqx.io/docs/en/v5.0/observability/prometheus.html#integrate-with-prometheus)

See HTTP API docs to learn how to
[Update Prometheus config](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Monitor/paths/~1prometheus/put)
and [Get all metrics data](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Monitor/paths/~1prometheus~1stats/get).


Correspondingly, we have also provided a [Grafana template](https://grafana.com/grafana/dashboards/17446-emqx/)
for visualizing these metrics.
