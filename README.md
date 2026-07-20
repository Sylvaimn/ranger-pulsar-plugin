# Ranger Pulsar Plugin

Apache Ranger authorization plugin for Apache Pulsar, providing centralized fine-grained access control for Pulsar resources.

## Features

- **Centralized Authorization**: Integrates Pulsar with Apache Ranger for unified policy management
- **Fine-grained Access Control**: Supports cluster, namespace, topic, and subscription level permissions
- **Multiple Access Types**: Supports produce, consume, lookup, admin, and function operations
- **Audit Support**: Complete audit trail of authorization decisions
- **Async/Sync API**: Supports both Pulsar's async and sync authorization APIs

## Architecture

```
┌─────────────────┐
│  Pulsar Broker  │
│                 │
│  ┌───────────┐  │
│  │  Ranger   │  │
│  │Authorization│ │
│  │ Provider  │  │
│  └─────┬─────┘  │
└────────┼────────┘
         │
         ▼
┌─────────────────┐
│  Ranger Plugin  │
│  (Policy Engine)│
└─────┬───────────┘
      │
      ▼
┌─────────────────┐
│  Ranger Admin   │
│  (Policy Store) │
└─────────────────┘
```

## Project Structure

```
ranger-pulsar-plugin/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   ├── java/
    │   │   └── com/apache/ranger/pulsar/
    │   │       ├── RangerPulsarConstants.java
    │   │       ├── auth/
    │   │       │   └── RangerAuthorizationProvider.java
    │   │       ├── plugin/
    │   │       │   └── RangerPulsarPlugin.java
    │   │       ├── resource/
    │   │       │   └── RangerPulsarResource.java
    │   │       └── audit/
    │   │           └── RangerPulsarAuditHandler.java
    │   └── resources/
    │       ├── META-INF/
    │       │   └── services/
    │       │       └── org.apache.pulsar.broker.authorization.AuthorizationProvider
    │       ├── ranger-pulsar-audit.xml
    │       ├── ranger-pulsar-security.xml
    │       └── ranger-servicedef-pulsar.json
    └── test/
        └── java/
            └── com/apache/ranger/pulsar/
                └── RangerPulsarPluginTest.java
```

## Build

```bash
mvn clean package
```

## Configuration

### Pulsar Configuration

Add to `broker.conf` or `standalone.conf`:

```properties
# Enable authentication
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# Enable authorization
authorizationEnabled=true
authorizationProvider=com.apache.ranger.pulsar.auth.RangerAuthorizationProvider

# Superuser roles
superUserRoles=admin
```

### Ranger Service Definition

1. Import `ranger-servicedef-pulsar.json` into Ranger Admin as a new service type
2. Create a Pulsar service instance in Ranger Admin
3. Define policies for your Pulsar resources

### Plugin Configuration

Place configuration files in Pulsar's classpath or conf directory:

- **ranger-pulsar-security.xml**: Plugin security configuration including Ranger Admin URL
- **ranger-pulsar-audit.xml**: Audit configuration for logging authorization events

## Resource Hierarchy

Resources are defined in hierarchical order:

1. **Cluster** - Pulsar cluster name
2. **Namespace** - Tenant/namespace (e.g., `public/default`)
3. **Topic** - Topic name within namespace
4. **Subscription** - Subscription name (optional)

Wildcards (`*`) are supported at any level.

## Access Types

| Access Type | Description |
|-------------|-------------|
| `produce`   | Produce messages to topics |
| `consume`   | Consume messages from topics |
| `lookup`    | Lookup topic metadata |
| `admin`     | Administrative operations |
| `function`  | Pulsar Functions operations |

## Policy Examples

### Allow Produce/Consume on a Topic

```json
{
  "resources": {
    "cluster": {"values": ["standalone"]},
    "namespace": {"values": ["public/default"]},
    "topic": {"values": ["my-topic"]}
  },
  "accesses": [
    {"type": "produce", "isAllowed": true},
    {"type": "consume", "isAllowed": true}
  ]
}
```

### Admin Access to Namespace

```json
{
  "resources": {
    "cluster": {"values": ["*"]},
    "namespace": {"values": ["public/admin"]},
    "topic": {"values": ["*"]}
  },
  "accesses": [
    {"type": "admin", "isAllowed": true}
  ]
}
```

## Deployment

1. Build the plugin JAR:
   ```bash
   mvn clean package
   ```

2. Copy the JAR to Pulsar's lib directory:
   ```bash
   cp target/ranger-pulsar-plugin-*.jar $PULSAR_HOME/lib/
   ```

3. Update Pulsar broker configuration as described above

4. Restart Pulsar broker

## License

Apache License 2.0
