The directory path scheme for on-disk Kafka/Confluent/Azure Event Hub buffers has changed.  It now uses the Action name instead of the topic name.

Upgrading to this version will invalidate (not use) old buffer files, and will require manual cleanup of the old directories.
