Fixed Redis Sentinel resources sharing one global Sentinel manager.  Multiple
Redis Sentinel resources on the same node now keep independent Sentinel server
lists and credentials, preventing one resource from connecting through another
resource's Sentinel configuration.
