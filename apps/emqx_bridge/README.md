# emqx_bridge

EMQX Data Bridge is an application that managing the resources (see emqx_resource) used by emqx
rule engine.

It provides CRUD HTTP APIs of the resources, and is also responsible for loading the resources at
startup, and saving configs of resources to `data/` after configs updated.

The application depends on `emqx_connector` as that's where all the callback modules of `connector`
resources placed.
