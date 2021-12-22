# emqx-plugin-template

This is a template plugin for EMQ X >= 5.0.

For EMQ X >= 4.3, please see branch emqx-v4

For older EMQ X versions, plugin development is no longer maintained.

## Release

A EMQ X plugin release is a zip package including

1. A JSON format metadata file
2. A tar file with plugin's apps packed

Execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_template-<vsn>.tar.gz
```
See EMQ X documents for details on how to deploy the plugin.
