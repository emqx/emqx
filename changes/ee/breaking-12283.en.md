Fixed the `resource_opts` configuration schema for the GCP PubSub Producer connector so that it contains only relevant fields.
This affects the creation of GCP PubSub Producer connectors via HOCON configuration (`connectors.gcp_pubsub_producer.*.resource_opts`) and the HTTP APIs `POST /connectors` / `PUT /connectors/:id` for this particular connector type.
