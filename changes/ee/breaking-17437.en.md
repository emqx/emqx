Prometheus scrape endpoints (`/api/v5/prometheus/*`) now require authentication
by default. Set `prometheus.enable_basic_auth = false` explicitly to restore
the previous unauthenticated behavior. Deployments that scrape these endpoints
without credentials will need to either configure credentials on the scraper
or set the config field. The recommended setup is a dedicated API key with the
`monitoring` scope, used with Bearer auth in the scraper.
