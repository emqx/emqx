# Introduction
This chart bootstraps an [EMQX](https://www.emqx.io/) deployment on a [Kubernetes](https://kubernetes.io/) (K8s) cluster using the [Helm](https://helm.sh/) package manager.

# Prerequisites
+ [Kubernetes](https://kubernetes.io/) 1.6+
+ [Helm](https://helm.sh/)

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
$ helm del my-emqx
```

# Configuration
The following sections describe the configurable parameters of the chart and their default values.
## [K8s]((https://kubernetes.io/)) specific settings
The following table lists the configurable K8s parameters of the [EMQX](https://www.emqx.io/) chart and their default values.
Parameter  | Description | Default Value
---        |  ---        | ---
`replicaCount` | It is recommended to have odd number of nodes in a cluster, otherwise the emqx cluster cannot be automatically healed in case of net-split. | `3`
`image.tag` | EMQX Image tag (defaults to `.Chart.AppVersion`) | `nil`
`image.repository` | EMQX Image repository | `emqx/emqx`
`image.pullPolicy`  | The image pull policy  | `IfNotPresent`
`image.pullSecrets `  | The image pull secrets (does not add image pull secrets to deployed pods)  |``[]``
`recreatePods` | Forces the recreation of pods during upgrades, which can be useful to always apply the most recent configuration. | `false`
`serviceAccount.create` | If `true`, create a new service account | `true`
`serviceAccount.name` | Service account to be used. If not set and `serviceAccount.create` is `true`, a name is generated using the fullname template |
`serviceAccount.annotations` | Annotations to add to the service account |
`podAnnotations ` | Annotations for pod | `{}`
`podManagementPolicy`| To redeploy a chart with existing PVC(s), the value must be set to Parallel to avoid deadlock | `Parallel`
`persistence.enabled` | Enable EMQX persistence using PVC | `false`
`persistence.storageClass` | Storage class of backing PVC (uses alpha storage class annotation) | `nil`
`persistence.existingClaim` | EMQX data Persistent Volume existing claim name, evaluated as a template | `""`
`persistence.accessMode` | PVC Access Mode for EMQX volume | `ReadWriteOnce`
`persistence.size` | PVC Storage Request for EMQX volume | `20Mi`
`initContainers` | Containers that run before the creation of EMQX containers. They can contain utilities or setup scripts. |`{}`
`resources` | CPU/Memory resource requests/limits |`{}`
`nodeSelector` | Node labels for pod assignment |`{}`
`tolerations` | Toleration labels for pod assignment |``[]``
`affinity` | Map of node/pod affinities |`{}`
`service.type`  | Kubernetes Service type. | `ClusterIP`
`service.mqtt`  | Port for MQTT. | `1883`
`service.mqttssl` | Port for MQTT(SSL). | `8883`
`service.mgmt`  | Port for mgmt API. | `8081`
`service.ws`  | Port for WebSocket/HTTP. | `8083`
`service.wss`  | Port for WSS/HTTPS. | `8084`
`service.dashboard`  | Port for dashboard. | `18083`
`service.nodePorts.mqtt`  | Kubernetes node port for MQTT. | `nil`
`service.nodePorts.mqttssl` | Kubernetes node port for MQTT(SSL). | `nil`
`service.nodePorts.mgmt`  | Kubernetes node port for mgmt API. | `nil`
`service.nodePorts.ws`  | Kubernetes node port for WebSocket/HTTP. | `nil`
`service.nodePorts.wss`  | Kubernetes node port for WSS/HTTPS. | `nil`
`service.nodePorts.dashboard`  | Kubernetes node port for dashboard. | `nil`
`service.loadBalancerIP`  | loadBalancerIP for Service |	`nil`
`service.loadBalancerSourceRanges` |	Address(es) that are allowed when service is LoadBalancer |	`[]`
`service.externalIPs` |	ExternalIPs for the service |	`[]`
`service.externalTrafficPolicy` |	External Traffic Policy for the service |	`Cluster`
`service.annotations` |	Service annotations (evaluated as a template) |	`{}`
`ingress.dashboard.enabled` |	Enable ingress for EMQX Dashboard |	false
`ingress.dashboard.ingressClassName` |	Set the ingress class for EMQX Dashboard
`ingress.dashboard.path` | Ingress path for EMQX Dashboard |	`/`
`ingress.dashboard.pathType` | Ingress pathType for EMQX Dashboard |	`ImplementationSpecific`
`ingress.dashboard.hosts` | Ingress hosts for EMQX Mgmt API |	dashboard.emqx.local
`ingress.dashboard.tls` | Ingress tls for EMQX Mgmt API |	`[]`
`ingress.dashboard.annotations` | Ingress annotations for EMQX Mgmt API |	`{}`
`ingress.mgmt.enabled` |	Enable ingress for EMQX Mgmt API |	`false`
`ingress.mqtt.ingressClassName` |	Set the ingress class for EMQX Mgmt API | `nil`
`ingress.mgmt.path` | Ingress path for EMQX Mgmt API | `/`
`ingress.mgmt.pathType` | Ingress pathType for EMQX Mgmt API |	`ImplementationSpecific`
`ingress.mgmt.hosts` | Ingress hosts for EMQX Mgmt API |	`api.emqx.local`
`ingress.mgmt.tls` | Ingress tls for EMQX Mgmt API |	`[]`
`ingress.mgmt.annotations` | Ingress annotations for EMQX Mgmt API |	`{}`
`ingress.wss.enabled` |	Enable ingress for EMQX Mgmt API |	`false`
`ingress.wss.ingressClassName` |	Set the ingress class for EMQX Mgmt API | `nil`
`ingress.wss.path` | Ingress path for EMQX WSS |	`/`
`ingress.wss.pathType` | Ingress pathType for EMQX WSS |	`ImplementationSpecific`
`ingress.wss.hosts` | Ingress hosts for EMQX WSS |    `wss.emqx.local`
`ingress.wss.tls` | Ingress tls for EMQX WSS |	`[]`
`ingress.wss.annotations` | Ingress annotations for EMQX WSS |	`{}`
| `metrics.enable` | If set to true, [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) needs to be installed, and [emqx_prometheus](https://github.com/emqx/emqx/tree/main-v4.4/apps/emqx_prometheus) needs to enable | false |
| `metrics.type` | Now we only supported "prometheus" | "prometheus" |
`extraEnv` | Aditional container env vars | `[]`
`extraEnvFrom` | Aditional container env from vars (eg. [config map](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/), [secrets](https://kubernetes.io/docs/concepts/configuration/secret/) | `[]`
`extraArgs` | Additional container executable arguments | `[]`
`extraVolumes` | Additional container volumes (eg. for mounting certs from secrets) | `[]`
`extraVolumeMounts` | Additional container volume mounts (eg. for mounting certs from secrets) | `[]`

## EMQX specific settings
The following table lists the configurable [EMQX](https://www.emqx.io/)-specific parameters of the chart and their default values.
Parameter  | Description | Default Value
---        |  ---        | ---
`emqxConfig` | Map of [configuration](https://www.emqx.io/docs/en/latest/configuration/configuration.html) items expressed as [environment variables](https://www.emqx.io/docs/en/v4.3/configuration/environment-variable.html) (prefix can be omitted) or using the configuration files [namespaced dotted notation](https://www.emqx.io/docs/en/latest/configuration/configuration.html) | `nil`
`emqxLicenseSecretName` | Name of the secret that holds the license information | `nil`
`emqxAclConfig` | [ACL](https://docs.emqx.io/broker/latest/en/advanced/acl-file.html) configuration | `{allow, {user, "dashboard"}, subscribe, ["$SYS/#"]}. {allow, {ipaddr, "127.0.0.1"}, pubsub, ["$SYS/#", "#"]}. {deny, all, subscribe, ["$SYS/#", {eq, "#"}]}. {allow, all}.`
`emqxLoadedModules` | Modules to load on startup | `{emqx_mod_acl_internal, true}. {emqx_mod_presence, true}. {emqx_mod_delayed, false}. {emqx_mod_rewrite, false}. {emqx_mod_subscription, false}. {emqx_mod_topic_metrics, false}.`
`emqxLoadedPlugins` | Plugins to load on startup | `{emqx_management, true}. {emqx_recon, true}. {emqx_retainer, true}. {emqx_dashboard, true}. {emqx_telemetry, true}. {emqx_rule_engine, true}. {emqx_bridge_mqtt, false}.`

# Examples
This section provides some examples for the configuration of common scenarios.
## Enable Websockets SSL via [nginx-ingress community controller](https://kubernetes.github.io/ingress-nginx/)
The following settings describe a working scenario for acessing [EMQX](https://www.emqx.io/) Websockets with SSL termination at the [nginx-ingress community controller](https://kubernetes.github.io/ingress-nginx/).
```yaml
ingress:
  wss:
    enabled: true
    # ingressClassName: nginx
    annotations:
      nginx.ingress.kubernetes.io/backend-protocol: "http"
      nginx.ingress.kubernetes.io/use-forwarded-headers: "true"
      nginx.ingress.kubernetes.io/enable-real-ip: "true"
      nginx.ingress.kubernetes.io/proxy-request-buffering: "off"
      nginx.ingress.kubernetes.io/proxy-connect-timeout: "120"
      nginx.ingress.kubernetes.io/proxy-http-version: "1.1"
      nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
      nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
      nginx.ingress.kubernetes.io/use-proxy-protocol: "false"
      nginx.ingress.kubernetes.io/proxy-protocol-header-timeout: "5s"
    path: /mqtt
    pathType: ImplementationSpecific
    hosts:
    - myhost.example.com
    tls:
    - hosts:
        - myhost.example.com
      secretName: myhost-example-com-tls # Name of the secret that holds the certificates for the domain
```
