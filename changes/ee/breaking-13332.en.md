When an S3 Bridge is improperly configured, error messages now contain more informative and easy to read details.

## Breaking changes
* S3 Bridge configuration with invalid aggregated upload key template will no longer work. Before this change, such configuration was considered valid but the bridge would never work anyway.
