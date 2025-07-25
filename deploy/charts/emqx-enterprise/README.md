# Introduction

This chart bootstraps an emqx deployment on a Kubernetes cluster using the Helm package manager.

# Prerequisites

+ Kubernetes 1.6+
+ Helm

# Installing the Chart

To install the chart with the release name `my-emqx`:

+ From Github
  ```
  $ git clone https://github.com/emqx/emqx.git
  $ cd emqx/deploy/charts/emqx-enterprise
  $ helm install my-emqx .
  ```

+ From chart Repos
  ```
  helm repo add emqx https://repos.emqx.io/charts
  helm install my-emqx emqx/emqx-enterprise
  ```
  > If you want to install an unstable version, you need to add `--devel` when you execute the `helm install` command.

# Uninstalling the Chart

To uninstall/delete the `my-emqx` deployment:

```
$ helm del  my-emqx
```

# Configuration

The following table lists the configurable parameters of the emqx chart and their default values.

| Parameter                            | Description                                                                                                                                                  | Default Value                                           |
|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| `replicaCount`                       | It is recommended to have odd number of nodes in a cluster, otherwise the emqx cluster cannot be automatically healed in case of net-split.                  | 3                                                       |
| `image.repository`                   | EMQX Image name                                                                                                                                              | emqx/emqx-enterprise                                    |
| `image.pullPolicy`                   | The image pull policy                                                                                                                                        | IfNotPresent                                            |
| `image.pullSecrets `                 | The image pull secrets                                                                                                                                       | `[]` (does not add image pull secrets to deployed pods) |
| `serviceAccount.create`              | If `true`, create a new service account                                                                                                                      | `true`                                                  |
| `serviceAccount.name`                | Service account to be used. If not set and `serviceAccount.create` is `true`, a name is generated using the full-name template                               |                                                         |
| `serviceAccount.annotations`         | Annotations to add to the service account                                                                                                                    |                                                         |
| `envFromSecret`                      | The name pull a secret in the same Kubernetes namespace which contains values that will be added to the environment                                          | nil                                                     |
| `recreatePods`                       | Forces the recreation of pods during upgrades, which can be useful to always apply the most recent configuration.                                            | false                                                   |
| `podAnnotations `                    | Annotations for pod                                                                                                                                          | `{}`                                                    |
| `podManagementPolicy`                | To redeploy a chart with existing PVC(s), the value must be set to Parallel to avoid deadlock                                                                | `Parallel`                                              |
| `persistence.enabled`                | Enable EMQX persistence using PVC                                                                                                                            | false                                                   |
| `persistence.storageClass`           | Storage class of backing PVC                                                                                                                                 | `nil` (uses alpha storage class annotation)             |
| `persistence.existingClaim`          | EMQX data Persistent Volume existing claim name, evaluated as a template                                                                                     | ""                                                      |
| `persistence.accessMode`             | PVC Access Mode for EMQX volume                                                                                                                              | ReadWriteOnce                                           |
| `persistence.size`                   | PVC Storage Request for EMQX volume                                                                                                                          | 20Mi                                                    |
| `initContainers`                     | Containers that run before the creation of EMQX containers. They can contain utilities or setup scripts.                                                     | `{}`                                                    |
| `resources`                          | CPU/Memory resource requests/limits                                                                                                                          | {}                                                      |
| `extraVolumeMounts`                  | Additional volumeMounts to the default backend container.                                                                                                    | []                                                      |
| `extraVolumes`                       | Additional volumes to the default backend pod.                                                                                                               | []                                                      |
| `nodeSelector`                       | Node labels for pod assignment                                                                                                                               | `{}`                                                    |
| `tolerations`                        | Toleration labels for pod assignment                                                                                                                         | `[]`                                                    |
| `affinity`                           | Map of node/pod affinities                                                                                                                                   | `{}`                                                    |
| `service.type`                       | Kubernetes Service type.                                                                                                                                     | ClusterIP                                               |
| `service.mqtt`                       | Port for MQTT.                                                                                                                                               | 1883                                                    |
| `service.mqttssl`                    | Port for MQTT(SSL).                                                                                                                                          | 8883                                                    |
| `service.ws`                         | Port for WebSocket/HTTP.                                                                                                                                     | 8083                                                    |
| `service.wss`                        | Port for WSS/HTTPS.                                                                                                                                          | 8084                                                    |
| `service.dashboard`                  | Port for dashboard and API.                                                                                                                                  | 18083                                                   |
| `service.nodePorts.mqtt`             | Kubernetes node port for MQTT.                                                                                                                               | nil                                                     |
| `service.nodePorts.mqttssl`          | Kubernetes node port for MQTT(SSL).                                                                                                                          | nil                                                     |
| `service.nodePorts.ws`               | Kubernetes node port for WebSocket/HTTP.                                                                                                                     | nil                                                     |
| `service.nodePorts.wss`              | Kubernetes node port for WSS/HTTPS.                                                                                                                          | nil                                                     |
| `service.nodePorts.dashboard`        | Kubernetes node port for dashboard.                                                                                                                          | nil                                                     |
| `service.loadBalancerClass`          | The load balancer implementation this Service belongs to                                                                                                     |                                                         |
| `service.loadBalancerIP`             | loadBalancerIP for Service                                                                                                                                   | nil                                                     |
| `service.loadBalancerSourceRanges`   | Address(es) that are allowed when service is LoadBalancer                                                                                                    | []                                                      |
| `service.externalIPs`                | ExternalIPs for the service                                                                                                                                  | []                                                      |
| `service.externalTrafficPolicy`      | External Traffic Policy for the service                                                                                                                      | `Cluster`                                               |
| `service.annotations`                | Service/ServiceMonitor annotations                                                                                                                           | {}(evaluated as a template)                             |
| `service.labels`                     | Service/ServiceMonitor labels                                                                                                                                | {}(evaluated as a template)                             |
| `ingress.dashboard.enabled`          | Enable ingress for EMQX Dashboard                                                                                                                            | false                                                   |
| `ingress.dashboard.ingressClassName` | Set the ingress class for EMQX Dashboard                                                                                                                     |                                                         |
| `ingress.dashboard.path`             | Ingress path for EMQX Dashboard                                                                                                                              | /                                                       |
| `ingress.dashboard.pathType`         | Ingress pathType for EMQX Dashboard                                                                                                                          | `ImplementationSpecific`                                |
| `ingress.dashboard.hosts`            | Ingress hosts for EMQX Dashboard                                                                                                                             | dashboard.emqx.local                                    |
| `ingress.dashboard.tls`              | Ingress tls for EMQX Dashboard                                                                                                                               | []                                                      |
| `ingress.dashboard.annotations`      | Ingress annotations for EMQX Dashboard                                                                                                                       | {}                                                      |
| `ingress.dashboard.ingressClassName` | Set the ingress class for EMQX Dashboard                                                                                                                     |                                                         |
| `ingress.mqtt.enabled`               | Enable ingress for MQTT                                                                                                                                      | false                                                   |
| `ingress.mqtt.ingressClassName`      | Set the ingress class for MQTT                                                                                                                               |                                                         |
| `ingress.mqtt.path`                  | Ingress path for MQTT                                                                                                                                        | /                                                       |
| `ingress.mqtt.pathType`              | Ingress pathType for MQTT                                                                                                                                    | `ImplementationSpecific`                                |
| `ingress.mqtt.hosts`                 | Ingress hosts for MQTT                                                                                                                                       | mqtt.emqx.local                                         |
| `ingress.mqtt.tls`                   | Ingress tls for MQTT                                                                                                                                         | []                                                      |
| `ingress.mqtt.annotations`           | Ingress annotations for MQTT                                                                                                                                 | {}                                                      |
| `ingress.mqtt.ingressClassName`      | Set the ingress class for MQTT                                                                                                                               |                                                         |
| `metrics.enable`                     | If set to true, [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator) needs to be installed, and emqx_prometheus needs to enable | false                                                   |
| `metrics.type`                       | Now we only supported "prometheus"                                                                                                                           | "prometheus"                                            |
| `ssl.enabled`                        | Enable SSL support                                                                                                                                           | false                                                   |
| `ssl.useExisting`                    | Use existing certificate or let cert-manager generate one                                                                                                    | false                                                   |
| `ssl.existingName`                   | Name of existing certificate                                                                                                                                 | emqx-tls                                                |
| `ssl.dnsnames`                       | DNS name(s) for certificate to be generated                                                                                                                  | {}                                                      |
| `ssl.commonName`                     | Common name for or certificate to be generated                                                                                                               |                                                         |
| `ssl.issuer.name`                    | Issuer name for certificate generation                                                                                                                       | letsencrypt-dns                                         |
| `ssl.issuer.kind`                    | Issuer kind for certificate generation                                                                                                                       | ClusterIssuer                                           |

## EMQX specific settings

The following table lists the configurable [EMQX](https://www.emqx.io/)-specific parameters of the chart and their
default values.
| Parameter                                                                                                                                                              | Description                                                                   | Default Value |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|---------------|
| `emqxConfig`                                                                                                                                                           | Map of [configuration](https://www.emqx.io/docs/en/v5.0/admin/cfg.html) items expressed as [environment variables](https://www.emqx.io/docs/en/v5.0/admin/cfg.html#environment-variables) (prefix `EMQX_` can be omitted) or using the configuration files [namespaced dotted notation](https://www.emqx.io/docs/en/v5.0/admin/cfg.html#syntax)                                                                             | `nil`                                                                         |               |
| `emqxLicenseSecretName`                                                                                                                                                | Name of the secret that holds the license information (deprecated)         | `nil`         |
| `emqxLicenseSecretRef.name`                                                                                                                                         | Name of the secret that holds the license information                         | `""`         |
| `emqxLicenseSecretRef.key`                                                                                                                                          | Key of the secret that holds the license information                          | `""`         |

## SSL settings
`cert-manager` generates secrets with certificate data using the keys `tls.crt` and `tls.key`. The helm chart always mounts those keys as files to `/tmp/ssl/`
which needs to explicitly configured by either changing the emqx config file or by passing the following environment variables:

```
  EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__CERTFILE: /tmp/ssl/tls.crt
  EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__KEYFILE: /tmp/ssl/tls.key
```

If you chose to use an existing certificate, make sure, you update the filenames accordingly.

## Tips
Enable the Proxy Protocol V1/2 if the EMQX cluster is deployed behind HAProxy or Nginx.
In order to preserve the original client's IP address, you could change the emqx config by passing the following environment variable:

```
EMQX_LISTENERS__TCP__DEFAULT__PROXY_PROTOCOL: "true"
```

With HAProxy you'd also need the following ingress annotation:

```
haproxy-ingress.github.io/proxy-protocol: "v2"
```
