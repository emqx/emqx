# EMQX File Transfer

EMQX File Transfer application enables the _File Transfer over MQTT_ feature described in [EIP-0021](https://github.com/emqx/eip), and provides support to publish transferred files either to the node-local file system or to the S3 API compatible remote object storage.

## Usage

As almost any other EMQX application, `emqx_ft` is configured via the EMQX configuration system. The following snippet is the minimal configuration that will enable File Transfer over MQTT.

```
file_transfer {
  enable = true
}
```

The configuration above will make File Transfer available to all MQTT clients, and will use the default storage backend, which in turn uses node-local file system both for temporary storage and for the final destination of the transferred files.

## Configuration

Every configuration parameter is described in the `emqx_ft_schema` module.

The most important configuration parameter is `storage`, which defines the storage backend to use. Currently, only `local` storage backend is available, which stores all the temporary data accumulating during file transfers in the node-local file system. Those go into `${EMQX_DATA_DIR}/file_transfer` directory by default, but can be configured via `local.storage.segments.root` parameter. The final destination of the transferred files on the other hand is defined by `local.storage.exporter` parameter, and currently can be either `local` or `s3`.

### Local Exporter

The `local` exporter is the default one, and it stores the transferred files in the node-local file system. The final destination directory is defined by `local.storage.exporter.local.root` parameter, and defaults to `${EMQX_DATA_DIR}/file_transfer/exports` directory.

```
file_transfer {
  enable = true
  storage {
    local {
      enable = true
      exporter {
        local { root = "/var/lib/emqx/transfers" }
      }
    }
  }
}
```

Important to note that even though the transferred files go into the node-local file system, the File Transfer API provides a cluster-wide view of the transferred files, and any file can be downloaded from any node in the cluster.

### S3 Exporter

The `s3` exporter stores the transferred files in the S3 API compatible remote object storage. The destination bucket is defined by `local.storage.exporter.s3.bucket` parameter.

This snippet configures File Transfer to store the transferred files in the `my-bucket` bucket in the `us-east-1` region of the AWS S3 service.

```
file_transfer {
  enable = true
  storage {
    local {
      enable = true
      exporter {
        s3 {
          enable = true
          host = "s3.us-east-1.amazonaws.com"
          port = 443
          access_key_id = "AKIA27EZDDM9XLINWXFE"
          secret_access_key = "..."
          bucket = "my-bucket"
          transport_options = {
            ssl { enable = true }
          }
        }
      }
    }
  }
}

```

## API

### MQTT

When enabled, File Transfer application reserves MQTT topics starting with `$file/` prefix for the purpose of serving the File Transfer protocol, as described in [EIP-0021](https://github.com/emqx/eip).

### REST

Application publishes a basic set of APIs, to:
* List all the transferred files available for download.
* Configure the application, including the storage backend.
* (When using `local` storage exporter) Download the transferred files.

Switching to the `s3` storage exporter is possible at any time, but the files transferred before the switch will not be
available for download anymore. Though, the files will still be available in the node-local file system.

## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
