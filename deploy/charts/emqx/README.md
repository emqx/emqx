# Introduction
This chart bootstraps an emqx deployment on a Kubernetes (K8s) cluster using the Helm package manager.

# Prerequisites
+ Kubernetes 1.6+
+ Helm

# Installing the Chart
To install the chart with the release name `my-emqx`:

+   From github
    ```
    $ git clone https://github.com/emqx/emqx.git
    $ cd emqx/deploy/charts/emqx
    $ helm install my-emqx .
    ```

+   From chart repos
    ```
    helm repo add emqx https://repos.emqx.io/charts
    helm install my-emqx emqx/emqx
    ```
    > If you want to install an unstable version, you need to add `--devel` when you execute the `helm install` command.

# Uninstalling the Chart
To uninstall/delete the `my-emqx` deployment:
```
$ helm del  my-emqx
```

# Configuration
The following sections describe the configurable parameters of the EMQx chart and their default values.
## Kubernetes-specific
The following table lists the configurable K8s parameters of the EMQx chart and their default values.
| Parameter  | Description | Default Value
| ---        |  ---        | ---
| `replicaCount` | It is recommended to have odd number of nodes in a cluster, otherwise the emqx cluster cannot be automatically healed in case of net-split. | `3`
| `image.repository` | EMQ X Image name | `emqx/emqx`
| `image.pullPolicy`  | The image pull policy  | `IfNotPresent`
| `image.pullSecrets `  | The image pull secrets (does not add image pull secrets to deployed pods)  |``[]``
| `envFromSecret` | The name pull a secret in the same kubernetes namespace which contains values that will be added to the environment | `nil`
| `recreatePods` | Forces the recreation of pods during upgrades, which can be useful to always apply the most recent configuration. | `false`
| `persistence.enabled` | Enable EMQX persistence using PVC | `false`
| `persistence.storageClass` | Storage class of backing PVC (uses alpha storage class annotation) | `nil`
| `persistence.existingClaim` | EMQ X data Persistent Volume existing claim name, evaluated as a template | `""`
| `persistence.accessMode` | PVC Access Mode for EMQX volume | `ReadWriteOnce`
| `persistence.size` | PVC Storage Request for EMQX volume | `20Mi`
| `initContainers` | Containers that run before the creation of EMQX containers. They can contain utilities or setup scripts. |`{}`
| `resources` | CPU/Memory resource requests/limits |`{}`
| `nodeSelector` | Node labels for pod assignment |`{}`
| `tolerations` | Toleration labels for pod assignment |``[]``
| `affinity` | Map of node/pod affinities |`{}`
| `service.type`  | Kubernetes Service type. | `ClusterIP`
| `service.mqtt`  | Port for MQTT. | `1883`
| `service.mqttssl` | Port for MQTT(SSL). | `8883`
| `service.mgmt`  | Port for mgmt API. | `8081`
| `service.ws`  | Port for WebSocket/HTTP. | `8083`
| `service.wss`  | Port for WSS/HTTPS. | `8084`
| `service.dashboard`  | Port for dashboard. | `18083`
| `service.nodePorts.mqtt`  | Kubernetes node port for MQTT. | `nil`
| `service.nodePorts.mqttssl` | Kubernetes node port for MQTT(SSL). | `nil`
| `service.nodePorts.mgmt`  | Kubernetes node port for mgmt API. | `nil`
| `service.nodePorts.ws`  | Kubernetes node port for WebSocket/HTTP. | `nil`
| `service.nodePorts.wss`  | Kubernetes node port for WSS/HTTPS. | `nil`
| `service.nodePorts.dashboard`  | Kubernetes node port for dashboard. | `nil`
| `service.loadBalancerIP`  | loadBalancerIP for Service |	`nil`
| `service.loadBalancerSourceRanges` |	Address(es) that are allowed when service is LoadBalancer |	`[]`
| `service.externalIPs` |	ExternalIPs for the service |	`[]`
| `service.annotations` |	Service annotations (evaluated as a template) |	`{}`
| `ingress.dashboard.enabled` |	Enable ingress for EMQX Dashboard |	false
| `ingress.dashboard.ingressClassName` |	Set the ingress class for EMQX Dashboard
| `ingress.dashboard.path` | Ingress path for EMQX Dashboard |	`/`
| `ingress.dashboard.hosts` | Ingress hosts for EMQX Mgmt API |	dashboard.emqx.local
| `ingress.dashboard.tls` | Ingress tls for EMQX Mgmt API |	`[]`
| `ingress.dashboard.annotations` | Ingress annotations for EMQX Mgmt API |	`{}`
| `ingress.mgmt.enabled` |	Enable ingress for EMQX Mgmt API |	`false`
| `ingress.mqtt.ingressClassName` |	Set the ingress class for EMQX Mgmt API | `nil`
| `ingress.mgmt.path` | Ingress path for EMQX Mgmt API | `/`
| `ingress.mgmt.hosts` | Ingress hosts for EMQX Mgmt API |	`api.emqx.local`
| `ingress.mgmt.tls` | Ingress tls for EMQX Mgmt API |	`[]`
| `ingress.mgmt.annotations` | Ingress annotations for EMQX Mgmt API |	`{}`
| `ingress.wss.enabled` |	Enable ingress for EMQX Mgmt API |	`false`
| `ingress.wss.ingressClassName` |	Set the ingress class for EMQX Mgmt API | `nil`
| `ingress.wss.path` | Ingress path for EMQX WSS |	`/`
| `ingress.wss.hosts` | Ingress hosts for EMQX WSS |    `wss.emqx.local`
| `ingress.wss.tls` | Ingress tls for EMQX WSS |	`[]`
| `ingress.wss.annotations` | Ingress annotations for EMQX WSS |	`{}`

## EMQx-specific
The following table lists the configurable EMQx parameters of the EMQx chart and their default values.
| Parameter  | Description | Default Value
| ---        |  ---        | ---
| `emqxConfig` | [Global configuration](https://hub.docker.com/r/emqx/emqx) items | `nil`
| `emqxLicenseSecretName` | Name of the secret that holds the license information | `nil`
| `emqxAclConfig` | [ACL]((https://docs.emqx.io/broker/latest/en/advanced/acl-file.html)) configuration | `{allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}. {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}. {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}. {allow, all}.`
| `emqxLoadedModules` | Modules to load on start | `{emqx_mod_acl_internal, true}. {emqx_mod_presence, true}. {emqx_mod_delayed, false}. {emqx_mod_rewrite, false}. {emqx_mod_subscription, false}. {emqx_mod_topic_metrics, false}.`
| `emqxLoadedPlugins` | Plugins to load on start | `{emqx_management, true}. {emqx_recon, true}. {emqx_retainer, true}. {emqx_dashboard, true}. {emqx_telemetry, true}. {emqx_rule_engine, true}. {emqx_bridge_mqtt, false}.`