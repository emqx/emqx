# emqx_mcp_gateway

The MCP Gateway is a component of EMQX that enables MCP clients using the MCP over MQTT protocol to interact with MCP servers using other protocols.

```
                                                               ┌────────────────┐         
                                                   ┌─MCP/HTTP──┼ MCP HTTP Server│         
                                                   │           └────────────────┘         
                                                   │                                      
                                                   │                                      
                               ┌───────────────┐   │                                      
     ┌──────────┐              │               │   │           ┌─────────────────┐        
     │MCP Client┼──MCP/MQTT────┼    Gateway    ┼───┼─MCP/STDIO─┼ MCP STDIO Server│        
     └──────────┘              │               │   │           └─────────────────┘        
                               └───────────────┘   │                                      
                                                   │                                      
                                                   │                                      
                                                   │           ┌──────────────────┐       
                                                   └─CALL/gRPC─┼ MCP Server Plugin│       
                                                               └──────────────────┘       
```

## Usage Examples

### Configure the Server Name for MCP/MQTT Servers

We can import the MCP server name configuration from a file. Prepare an `mcp_server_name.csv` file with the following content:

```csv
# mqtt_username, server_name
username1, devices/vehicle/vin001
username2, devices/vehicle/vin002
```

Import the configuration using CLI:

```shell
bin/emqx_ctl mcp server_name import --file /path/to/mcp_server_name.csv
```

### Connect to a STDIO MCP server

Add a STDIO MCP server to the configuration file:

```hocon
mcp.servers.weather = {
  enable = true
  server_name = "system_tools/office/weather"
  server_type = "stdio"
  command = "uv"
  args = [
    "--directory",
    "/ABSOLUTE/PATH/TO/PARENT/FOLDER/weather",
    "run",
    "weather.py"
  ]
  env = [
    {"API_KEY", "eee" }
  ]
}
```

### Connect to a HTTP MCP server

Add an HTTP MCP server to the configuration file:

```hocon
mcp.servers.calculator = {
  enable = true
  server_name = "system_tools/math/calculator"
  server_type = "http"
  url = "https://localhost:3300"
}
```

### Setup a Embedded MCP server using EMQX plugin

Build and install the plugin according to the docs.
