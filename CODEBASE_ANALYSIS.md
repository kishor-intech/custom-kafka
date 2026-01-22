# Kafka Helm Chart - Complete Codebase Analysis

## Overview

This is a **production-ready Helm chart** for deploying Apache Kafka on Kubernetes with Kafka UI dashboard. The chart supports:
- ✅ **KRaft Mode** (controller + broker combined)
- ✅ **Multi-node setup** with StatefulSet
- ✅ **Replication** (configurable replication factors)
- ✅ **Health checks** (liveness & readiness probes)
- ✅ **High Availability** (pod anti-affinity)
- ✅ **Persistent Storage** (with PVC or ephemeral emptyDir)
- ✅ **Security** (RBAC, security context, non-root user)
- ✅ **Monitoring** (Kafka UI dashboard)
- ✅ **Performance tuning** (thread pools, buffer sizes)

---

## Project Structure

```
kafka-custom-testing-v3/
├── README.md                           # Quick start guide
├── CODEBASE_ANALYSIS.md               # This file
│
├── apache-kafka/                       # Docker image for Kafka
│   ├── Dockerfile                      # Builds Kafka image with KRaft support
│   ├── entrypoint.sh                   # Container startup script
│   ├── docker-compose-test-kraft.yml   # Local testing with Docker Compose
│   └── DOCKERFILE_ENTRYPOINT_GUIDE.md  # Docker setup documentation
│
├── kafka-helm/                         # Helm chart (Recommended for production)
│   ├── HELM_DEPLOYMENT.md              # Helm installation guide
│   ├── HELM_PARAMETERS.md              # Parameter reference
│   ├── custom.helm.parameters.md       # Custom configuration guide
│   ├── EXTERNAL_ACCESS_GUIDE.md        # Accessing Kafka externally
│   ├── kafka-parameters-guide.md       # Kafka config parameters
│   ├── setup-external-access.sh        # Script for external access setup
│   │
│   └── kafka/                          # Helm chart package
│       ├── Chart.yaml                  # Chart metadata (v1.0.0, app v4.0.0)
│       ├── values.yaml                 # All configurable parameters
│       │
│       └── templates/                  # Kubernetes manifests (Helm templates)
│           ├── _helpers.tpl            # Helm template helpers
│           ├── rbac.yaml               # ServiceAccount, ClusterRole, ClusterRoleBinding
│           ├── configmap.yaml          # Kafka broker configuration (env vars)
│           ├── statefulset.yaml        # Kafka broker deployment (main)
│           ├── service.yaml            # Headless, ClusterIP, LoadBalancer services
│           └── kafka-ui.yaml           # Kafka UI deployment + service
│
└── kafka-manifest/                     # Kubernetes manifests (alternative to Helm)
    ├── MANIFEST_DEPLOYMENT.md          # Manual deployment guide
    ├── kafka.yaml                      # Kafka broker manifest
    └── kafka-ui.yaml                   # Kafka UI manifest
```

---

## 1. Core Components

### 1.1 Docker Image (apache-kafka/)

**File:** [apache-kafka/Dockerfile](apache-kafka/Dockerfile)

```dockerfile
FROM apache/kafka:4.1.1
USER root
RUN mkdir -p /opt/kafka/config /opt/kafka/data /bitnami/kafka/data \
    && chown -R 1001:1001 /opt/kafka /bitnami/kafka
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER 1001
ENTRYPOINT ["/entrypoint.sh"]
```

**Key Points:**
- Based on official Apache Kafka 4.1.1 image
- Runs as non-root user (1001 = kafka user)
- Custom entrypoint for KRaft mode initialization
- Writable directories for config and data

### 1.2 Container Entrypoint (apache-kafka/entrypoint.sh)

**Responsibilities:**
1. **Validate critical variables** (NODE_ID, CONTROLLER_QUORUM_VOTERS)
2. **Build `server.properties`** from environment variables
3. **Initialize KRaft metadata** using `kafka-storage.sh format`
4. **Start Kafka broker** with `kafka-server-start.sh`

**Key Variables Generated:**
```bash
NODE_ID           # Auto-calculated from pod ordinal (0→1, 1→2, etc.)
CONTROLLER_QUORUM_VOTERS  # Comma-separated list of controller nodes
CLUSTER_ID        # Shared cluster identifier
PROCESS_ROLES     # "broker,controller" for KRaft mode
ADVERTISED_LISTENERS  # Pod DNS for internal Kubernetes communication
```

---

## 2. Helm Chart Configuration

### 2.1 Chart Metadata - [Chart.yaml](kafka-helm/kafka/Chart.yaml)

```yaml
apiVersion: v2
name: kafka
description: Apache Kafka on Kubernetes - Simplified production-ready chart
type: application
version: 1.0.0
appVersion: "4.0.0"
```

### 2.2 Configuration Values - [values.yaml](kafka-helm/kafka/values.yaml)

#### **Namespace & Image**

```yaml
namespace: default                    # Must match helm install -n flag
image:
  registry: ""                        # Docker registry (empty = default)
  repository: kfk                     # Image repository
  tag: "latest"                       # Image tag
  pullPolicy: IfNotPresent
initImage: alpine:3.18                # For volume permissions init container
```

#### **Kafka Cluster Configuration**

```yaml
kafka:
  replicas: 1                         # Number of brokers (scale to 3+ for HA)
  clusterId: "MkU3OEVBNTcwNTJENDM2Qk"  # Shared cluster identifier
  kraftVersion: 1                     # KRaft protocol version
  processRoles: "broker,controller"   # Combined role (both broker + controller)
  externalBootstrapHost: ""           # For external access (e.g., localhost, DNS)
  externalBootstrapPort: 9092
```

#### **Listeners Configuration (Multi-Protocol)**

```yaml
listeners:
  client:           # External clients
    enabled: true
    port: 9092
    protocol: PLAINTEXT
  
  controller:       # KRaft controller communication
    enabled: true
    port: 9093
    protocol: PLAINTEXT
  
  internal:         # Inter-broker communication
    enabled: true
    port: 9094
    protocol: PLAINTEXT
```

**Listener Map (LISTENER_SECURITY_PROTOCOL_MAP):**
- `PLAINTEXT:PLAINTEXT` → No encryption, no auth
- `CONTROLLER:PLAINTEXT` → Controller nodes communicate via this
- `INTERNAL:PLAINTEXT` → Brokers communicate via this

#### **Replication (Data Safety)**

```yaml
replication:
  offsetsTopicReplicationFactor: 1    # Change to 3 for production (3+ brokers)
  transactionLogReplicationFactor: 1  # Transaction log replication
  transactionLogMinIsr: 1             # Minimum in-sync replicas for transactions
  minInSyncReplicas: 1                # Minimum brokers that must ack writes
  defaultReplicationFactor: 1         # Default for new topics
```

| Setting | Current | Production (3+ brokers) | Purpose |
|---------|---------|------------------------|---------|
| offsetsTopicReplicationFactor | 1 | 3 | Consumer offset replication |
| minInSyncReplicas | 1 | 2 | Durability for producer acks=all |
| transactionLogMinIsr | 1 | 2 | Transaction durability |

#### **Storage Configuration**

```yaml
storage:
  enabled: true
  size: "20Gi"                        # PVC size per broker
  className: "longhorn"               # Storage provisioner class
  mountPath: "/bitnami/kafka/data"    # Data directory inside container
  ephemeral: false                    # false=PVC, true=emptyDir (testing only)
  logs:
    retentionHours: 168               # 7 days
    retentionBytes: ""                # Unlimited by size
    segmentBytes: "1073741824"         # 1GB per log segment
```

**Volume Strategy:**
- **ephemeral: false** → StatefulSet uses `volumeClaimTemplates` (persistent)
- **ephemeral: true** → StatefulSet uses `emptyDir` (lost on pod restart)

#### **Performance Tuning**

```yaml
performance:
  networkThreads: 8                   # Threads handling client requests
  ioThreads: 8                        # Threads handling disk I/O
  socketSendBufferBytes: "102400"     # 100KB
  socketReceiveBufferBytes: "102400"
  socketRequestMaxBytes: "104857600"  # 100MB
  batchSize: "16384"                  # 16KB batches
  lingerMs: "10"                      # 10ms batching window
```

#### **Resource Requests & Limits**

```yaml
resources:
  requests:                           # Guaranteed resources
    cpu: "500m"                       # 0.5 CPU cores
    memory: "1Gi"                     # 1 GB RAM
  limits:                             # Max resources
    cpu: "2000m"                      # 2 CPU cores
    memory: "2Gi"                     # 2 GB RAM

jvmHeap:
  min: "512M"                         # Xms
  max: "1G"                           # Xmx
```

#### **Kafka UI Configuration**

```yaml
kafkaUI:
  enabled: true
  replicas: 1                         # Number of UI instances
  image:
    registry: docker.io
    repository: provectuslabs/kafka-ui
    tag: "latest"
  service:
    type: LoadBalancer                # ClusterIP, NodePort, LoadBalancer
    port: 8080
```

#### **Health Checks**

```yaml
healthChecks:
  enabled: true
  liveness:
    enabled: true
    initialDelaySeconds: 60           # Wait 60s before first check
    periodSeconds: 10                 # Check every 10s
  readiness:
    enabled: true
    initialDelaySeconds: 45
    periodSeconds: 10
```

**Probe Type:** `tcpSocket` on client port (9092)
- **Liveness:** Restarts pod if Kafka unresponsive
- **Readiness:** Removes pod from service if unhealthy

#### **High Availability (Pod Anti-Affinity)**

```yaml
affinity:
  podAntiAffinity:
    enabled: true
    type: "preferred"                 # preferred or required
```

**Effect:** Spreads Kafka pods across different nodes (if available)

---

## 3. Kubernetes Templates

### 3.1 RBAC (Role-Based Access Control) - [rbac.yaml](kafka-helm/kafka/templates/rbac.yaml)

**Creates:**
1. **ServiceAccount** - Identity for Kafka pod
2. **ClusterRole** - Permissions to read pods, services, configmaps, endpoints
3. **ClusterRoleBinding** - Binds role to service account

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "endpoints", "configmaps"]
    verbs: ["get", "list", "watch"]
```

### 3.2 ConfigMap - [configmap.yaml](kafka-helm/kafka/templates/configmap.yaml)

**Contains:** Kafka broker configuration as environment variables

```yaml
KAFKA_CFG_PROCESS_ROLES: "broker,controller"
KAFKA_CFG_CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
KAFKA_CFG_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094"
KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-0.kafka-headless.namespace.svc.cluster.local:9092,..."
KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
KAFKA_CFG_LOG_DIRS: "/bitnami/kafka/data"
KAFKA_CFG_LOG_RETENTION_HOURS: "168"
KAFKA_CFG_NUM_NETWORK_THREADS: "8"
# ... 30+ other settings
```

**Mounted as environment variables into Kafka pod**

### 3.3 StatefulSet - [statefulset.yaml](kafka-helm/kafka/templates/statefulset.yaml)

**Key Features:**

#### **StatefulSet Properties**
```yaml
serviceName: kafka-headless                    # Headless service (pod DNS)
replicas: 1                                    # Scale to 3+ for HA
volumeClaimTemplates:                          # Dynamic PVC per pod
  - metadata:
      name: kafka-data
    spec:
      storageClassName: longhorn
      requests:
        storage: 20Gi
```

#### **Init Containers** (Only if not ephemeral)
```yaml
initContainers:
  - name: volume-permissions
    image: alpine:3.18
    command:
      - chown -R 1001:1001 /bitnami/kafka/data
      - chmod -R 755 /bitnami/kafka/data
```

**Purpose:** Fix volume permissions for non-root user (1001)

#### **Container Security Context**
```yaml
securityContext:
  fsGroup: 1001                                # File system group (kafka user)
  
containers:
  - securityContext:
      runAsUser: 1001                          # Non-root user
      runAsNonRoot: true
      allowPrivilegeEscalation: false
      capabilities:
        drop: [ALL]
```

#### **Startup Command** (Dynamic Configuration)
```bash
# Calculate Pod Index from Pod name (kafka-0 → 0, kafka-1 → 1)
export POD_ORDINAL=${HOSTNAME##*-}
export NODE_ID=$((POD_ORDINAL + 1))

# Build voter list (KRaft mode - who can vote on cluster decisions)
VOTERS="${NODE_ID}@kafka-${POD_ORDINAL}.kafka-headless.namespace.svc.cluster.local:9093"
for i in $(seq 0 2); do  # For 3 replicas
  if [ $i -ne $POD_ORDINAL ]; then
    OTHER_POD="kafka-${i}.kafka-headless.namespace.svc.cluster.local"
    if timeout 2 bash -c "echo > /dev/tcp/${OTHER_POD}/9093" 2>/dev/null; then
      VOTERS="${VOTERS},$(($i + 1))@${OTHER_POD}:9093"
    fi
  fi
done
export CONTROLLER_QUORUM_VOTERS=$VOTERS

# Set advertised listeners (internal Kubernetes DNS)
export ADVERTISED_LISTENERS="PLAINTEXT://kafka-${POD_ORDINAL}.kafka-headless.namespace.svc.cluster.local:9092,..."

exec /entrypoint.sh
```

**Key Logic:**
1. Each pod gets unique NODE_ID (1, 2, 3, ...)
2. Discovers other brokers via DNS
3. Builds voter quorum dynamically
4. Uses internal Kubernetes DNS for pod discovery

#### **Environment Variables**
```yaml
env:
  - name: CLUSTER_ID
    value: "MkU3OEVBNTcwNTJENDM2Qk"
  - name: PROCESS_ROLES
    value: "broker,controller"
  - name: LISTENERS
    value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094"
  - name: CONTROLLER_QUORUM_VOTERS
    value: "1@kafka-0.kafka-headless.ns.svc.cluster.local:9093,..."
  # ... plus config from ConfigMap
```

#### **Health Checks**
```yaml
livenessProbe:
  tcpSocket:
    port: 9092                                 # Check client port
  initialDelaySeconds: 60                      # Wait 60s for startup
  periodSeconds: 10                            # Check every 10s

readinessProbe:
  tcpSocket:
    port: 9092
  initialDelaySeconds: 45
  periodSeconds: 10
```

#### **Pod Anti-Affinity** (Spread across nodes)
```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:  # Best effort
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              app: kafka
          topologyKey: kubernetes.io/hostname        # Different nodes
```

#### **Termination Grace Period**
```yaml
terminationGracePeriodSeconds: 60              # Allow 60s for graceful shutdown
```

#### **Resource Limits**
```yaml
resources:
  requests:
    cpu: "500m"
    memory: "1Gi"
  limits:
    cpu: "2000m"
    memory: "2Gi"
```

### 3.4 Services - [service.yaml](kafka-helm/kafka/templates/service.yaml)

**Creates 3 services:**

#### **1. Headless Service (for StatefulSet DNS)**
```yaml
kind: Service
metadata:
  name: kafka-headless
spec:
  clusterIP: None                               # Headless = no cluster IP
  ports:
    - name: client
      port: 9092
    - name: controller
      port: 9093
    - name: internal
      port: 9094
  publishNotReadyAddresses: true                # DNS even before ready
```

**Purpose:** Enables DNS names for pods (kafka-0.kafka-headless.ns.svc.cluster.local)

#### **2. ClusterIP Service (internal access)**
```yaml
kind: Service
metadata:
  name: kafka
spec:
  type: ClusterIP
  ports:
    - name: client
      port: 9092
    - name: internal
      port: 9094
```

**Purpose:** Load-balanced access from within cluster

#### **3. External Service (LoadBalancer)**
```yaml
kind: Service
metadata:
  name: kafka-external
spec:
  type: LoadBalancer                           # AWS/Cloud load balancer
  ports:
    - name: client
      port: 9092
```

**Purpose:** External access (e.g., from outside Kubernetes cluster)

### 3.5 Kafka UI - [kafka-ui.yaml](kafka-helm/kafka/templates/kafka-ui.yaml)

**Creates:**
1. **ConfigMap** - Kafka UI configuration
2. **Deployment** - Kafka UI pods (replicas: 1)
3. **Service** - LoadBalancer or ClusterIP

**Configuration:**
```yaml
KAFKA_CLUSTERS_0_NAME: "local-cluster"
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:9092"
SERVER_PORT: 8080
```

**Health Checks:**
```yaml
livenessProbe:
  httpGet:
    path: /actuator/health
    port: 8080
  initialDelaySeconds: 30

readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 10
```

---

## 4. Deployment Methods

### 4.1 Helm Chart (Recommended)

**Location:** [kafka-helm/kafka/](kafka-helm/kafka/)

```bash
# Install
helm install kafka ./kafka -n kafka-ns --create-namespace \
  --set kafka.replicas=3 \
  --set kafka.storage.size=50Gi \
  --set kafka.storage.className=longhorn

# Upgrade
helm upgrade kafka ./kafka -n kafka-ns --set kafka.replicas=5

# Rollback
helm rollback kafka -n kafka-ns
```

**Advantages:**
- ✅ Templating (values.yaml)
- ✅ Versioning (helm list, helm history)
- ✅ Rollback capability
- ✅ Easy scaling
- ✅ Package management

### 4.2 Kubernetes Manifests (Alternative)

**Location:** [kafka-manifest/](kafka-manifest/)

```bash
# Deploy
kubectl apply -f kafka.yaml
kubectl apply -f kafka-ui.yaml

# Scale (edit replicas, then apply)
kubectl apply -f kafka.yaml
```

**Advantages:**
- ✅ Simpler for development
- ✅ No Helm required
- ✅ Direct manifest control

---

## 5. Multi-Node Setup & Replication

### 5.1 Scaling to 3 Brokers

```bash
helm upgrade kafka ./kafka -n kafka-ns \
  --set kafka.replicas=3 \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2 \
  --set kafka.replication.defaultReplicationFactor=3
```

### 5.2 Pod Deployment Process

1. **Pod 1 (kafka-0) starts**
   - NODE_ID = 1
   - CONTROLLER_QUORUM_VOTERS = "1@kafka-0.kafka-headless.ns.svc.cluster.local:9093"
   - Initializes KRaft metadata (single-node quorum)

2. **Pod 2 (kafka-1) starts**
   - NODE_ID = 2
   - Discovers kafka-0 via DNS
   - CONTROLLER_QUORUM_VOTERS = "1@kafka-0...:9093,2@kafka-1...:9093"
   - Joins quorum

3. **Pod 3 (kafka-2) starts**
   - NODE_ID = 3
   - Discovers kafka-0 and kafka-1
   - CONTROLLER_QUORUM_VOTERS = "1@kafka-0...:9093,2@kafka-1...:9093,3@kafka-2...:9093"
   - Quorum now has 3 voters (majority = 2, fault tolerance = 1 failure)

### 5.3 Replication Behavior

| Setting | 1 Broker | 3 Brokers |
|---------|----------|-----------|
| offsetsTopicReplicationFactor | 1 | 3 |
| minInSyncReplicas | 1 | 2 |
| Durability | No redundancy | 1 failure tolerance |
| Consistency | No acks needed | 2 acks needed |

**Example Topic Partition (RF=3, minISR=2):**
```
Replicas: [broker-1, broker-2, broker-3]  (RF=3)
Leader: broker-1
ISR: [broker-1, broker-2, broker-3]       (all in-sync)

Producer with acks=all waits for:
- Leader acknowledgment (broker-1)
- 1 more follower ack (broker-2 or broker-3)
- Total: 2 acks (minISR requirement)

Data is safe: 1 broker can fail, data still on 2 others
```

---

## 6. Health Checks & Monitoring

### 6.1 Liveness Probe (Restart if dead)

```yaml
livenessProbe:
  tcpSocket:
    port: 9092              # Check if port responds
  initialDelaySeconds: 60   # Wait 60s for startup
  periodSeconds: 10         # Check every 10s
  failureThreshold: 3       # 3 failed checks = restart
```

**Timeline:**
- 0-60s: Kafka starting (no checks)
- 60-70s: First check
- 70s onward: Check every 10s
- After 30s of failures: Pod restarted

### 6.2 Readiness Probe (Remove from service if unhealthy)

```yaml
readinessProbe:
  tcpSocket:
    port: 9092
  initialDelaySeconds: 45   # Faster than liveness
  periodSeconds: 10
  failureThreshold: 3
```

**Effect:**
- Pod not ready → removed from service endpoints
- New producers/consumers directed to other brokers
- Automatic failover without hard restart

### 6.3 Kafka UI Monitoring

**Web Dashboard:** `http://localhost:8080` (after port-forward)

**Features:**
- Broker health status
- Topic/partition distribution
- Consumer groups
- Message browsing
- Performance metrics

### 6.4 Manual Health Checks

```bash
# Check pod status
kubectl get pods -n kafka-ns -o wide

# Check service endpoints
kubectl get endpoints -n kafka-ns

# Describe pod for events
kubectl describe pod kafka-0 -n kafka-ns

# Check logs
kubectl logs kafka-0 -n kafka-ns
kubectl logs kafka-0 -n kafka-ns -f  # tail

# Test broker connectivity
kubectl run -it --rm --image=bitnami/kafka:4.0.0 \
  --restart=Never kafka-test bash
# Inside pod:
kafka-topics.sh --list --bootstrap-server kafka:9092
```

---

## 7. Storage & Persistence

### 7.1 Volume Strategy

**Configuration:**
```yaml
storage:
  ephemeral: false         # false = PVC (persistent)
                           # true = emptyDir (ephemeral)
```

#### **Persistent Storage (ephemeral: false)**

```yaml
volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: longhorn      # Your storage provisioner
      resources:
        requests:
          storage: 20Gi
```

**Behavior:**
- Each pod gets its own 20Gi PVC
- Data survives pod restarts
- Data survives node failures (if storage backend is replicated)
- Suitable for production

#### **Ephemeral Storage (ephemeral: true)**

```yaml
volumes:
  - name: kafka-data
    emptyDir: {}
```

**Behavior:**
- In-memory or node-local disk
- Lost on pod restart
- No cross-node replication
- Suitable for testing only

### 7.2 Storage Classes

**Check available classes:**
```bash
kubectl get storageclass
```

**Common options:**
- `longhorn` - Distributed block storage (HA)
- `local-path` - Node-local (single node only)
- `ebs` - AWS Elastic Block Store
- `azure-disk` - Azure managed disks
- `gce-pd` - Google Compute Engine persistent disks

### 7.3 Data Retention

```yaml
logs:
  retentionHours: 168               # 7 days
  retentionBytes: ""                # Unlimited by size
  segmentBytes: "1073741824"         # 1GB segments
```

**Topic-specific override:**
```bash
kafka-configs.sh --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --alter \
  --add-config retention.ms=86400000  # 1 day override
```

---

## 8. Security Model

### 8.1 Container Security

```yaml
securityContext:
  runAsUser: 1001                    # Non-root (kafka user)
  runAsNonRoot: true
  allowPrivilegeEscalation: false    # Prevent sudo
  capabilities:
    drop: [ALL]                      # Drop all Linux capabilities
  fsGroup: 1001                      # File system group
```

### 8.2 RBAC (Who can do what)

```yaml
serviceAccountName: kafka
```

**Permissions:**
```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "endpoints", "configmaps"]
    verbs: ["get", "list", "watch"]
```

**Pod can:**
- ✅ Read pod metadata
- ✅ Read service definitions
- ✅ List endpoints (for service discovery)
- ❌ Cannot create/delete anything

### 8.3 Network Security

**Listener Configuration:**
```yaml
protocol: "PLAINTEXT"  # No encryption, no authentication
```

**Production Options:**
- `SSL` - TLS encryption
- `SASL_PLAINTEXT` - Username/password auth (no encryption)
- `SASL_SSL` - Auth + encryption

**Not configured in current version** (suitable for development/testing)

---

## 9. Performance Tuning

### 9.1 Thread Pool Configuration

```yaml
performance:
  networkThreads: 8                  # Threads for client requests
  ioThreads: 8                       # Threads for disk I/O
```

**Recommendation:**
- **networkThreads** = (CPU cores / 2) to CPU cores
- **ioThreads** = same as networkThreads or 2x networkThreads

### 9.2 Buffer Sizes

```yaml
performance:
  socketSendBufferBytes: "102400"              # 100KB (OS default)
  socketReceiveBufferBytes: "102400"           # 100KB
  socketRequestMaxBytes: "104857600"           # 100MB (max message size)
```

### 9.3 Producer Batching

```yaml
performance:
  batchSize: "16384"                 # Batch messages in 16KB chunks
  lingerMs: "10"                     # Wait up to 10ms for batching
```

**Effect:** Trades latency for throughput
- lingerMs=0 → Send immediately (low latency)
- lingerMs=100 → Batch for 100ms (high throughput)

### 9.4 Compression

```yaml
# In ConfigMap
KAFKA_CFG_COMPRESSION_TYPE: "snappy"
```

**Options:**
- `snappy` - Fast compression (default)
- `gzip` - Better compression ratio, slower
- `lz4` - Fast + decent ratio
- `zstd` - Best compression ratio, slower

### 9.5 Resource Allocation

```yaml
resources:
  requests:
    cpu: "500m"                      # Guaranteed 0.5 CPU
    memory: "1Gi"                    # Guaranteed 1GB RAM
  limits:
    cpu: "2000m"                     # Max 2 CPU
    memory: "2Gi"                    # Max 2GB RAM
```

**Kubernetes behavior:**
- **If no CPU available:** Pod queued (QoS: Guaranteed)
- **If memory exceeded:** Pod evicted/killed
- **For production:** Set requests = limits (predictable performance)

---

## 10. KRaft Mode (Controller + Broker Combined)

### 10.1 What is KRaft?

**Kafka Raft Mode** = Kafka + built-in consensus (no ZooKeeper needed)

```
Traditional Kafka:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Kafka Broker│     │ Kafka Broker│     │ Kafka Broker│
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                    ┌──────▼──────┐
                    │ ZooKeeper   │  (separate system)
                    │ Ensemble    │
                    └─────────────┘

KRaft Mode (No ZooKeeper):
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Kafka Broker +   │     │ Kafka Broker +   │     │ Kafka Broker +   │
│ KRaft Controller │     │ KRaft Controller │     │ KRaft Controller │
└──────────────────┘     └──────────────────┘     └──────────────────┘
        ↕                         ↕                         ↕
        └─────────────────────────┼─────────────────────────┘
                    (Raft consensus, port 9093)
```

### 10.2 Configuration

```yaml
kafka:
  kraftVersion: 1
  processRoles: "broker,controller"   # Both roles in same pod
  clusterId: "MkU3OEVBNTcwNTJENDM2Qk" # Shared cluster ID
```

**In Kafka config:**
```
process.roles=broker,controller
cluster.id=MkU3OEVBNTcwNTJENDM2Qk
controller.quorum.voters=1@kafka-0.kafka-headless.ns:9093,\
                         2@kafka-1.kafka-headless.ns:9093,\
                         3@kafka-2.kafka-headless.ns:9093
```

### 10.3 Raft Consensus

**Quorum voting:**
```
3 brokers → 3 votes total → majority = 2
  - If broker-1 fails: broker-2 + broker-3 can still decide ✅
  - If broker-2 fails: broker-1 + broker-3 can still decide ✅
  - If 2 brokers fail: No majority ❌ (cluster unavailable)

1 broker → 1 vote (always majority) ✅
2 brokers → 2 votes → majority = 2 (both must alive) ⚠️
```

### 10.4 Ports

| Port | Name | Purpose |
|------|------|---------|
| 9092 | PLAINTEXT | Client connections (producers/consumers) |
| 9093 | CONTROLLER | KRaft controller Raft communication |
| 9094 | INTERNAL | Inter-broker replication traffic |

---

## 11. Common Operations

### 11.1 Create a Topic

```bash
# Get Kafka pod
kubectl get pods -n kafka-ns

# Create topic in pod
kubectl exec -it kafka-0 -n kafka-ns -- bash

# Inside pod:
kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=86400000

# Verify
kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

### 11.2 Produce Messages

```bash
# Inside Kafka pod or test pod:
kafka-console-producer.sh \
  --broker-list kafka:9092 \
  --topic my-topic

# Type messages, Ctrl+C to exit
```

### 11.3 Consume Messages

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic my-topic \
  --from-beginning    # From start (or --to-latest)
```

### 11.4 Check Topic Details

```bash
kafka-topics.sh \
  --describe \
  --bootstrap-server kafka:9092 \
  --topic my-topic

# Output:
# Topic: my-topic	Partitions: 3	Replication: 3	Isr: 1,2,3
# 	Topic: my-topic	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
# 	Topic: my-topic	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
# 	Topic: my-topic	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
```

### 11.5 Scale Brokers

```bash
# Current: 1 broker, Target: 3 brokers
helm upgrade kafka ./kafka -n kafka-ns \
  --set kafka.replicas=3 \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2

# Watch pods start
kubectl get pods -n kafka-ns -w
```

### 11.6 Update Configuration

```bash
# Edit values
helm upgrade kafka ./kafka -n kafka-ns \
  --set kafka.storage.size=50Gi \
  --set kafka.performance.networkThreads=16 \
  --set kafka.performance.ioThreads=16

# Pods will restart with new config
```

### 11.7 Rollback Deployment

```bash
# View history
helm history kafka -n kafka-ns

# Rollback to previous
helm rollback kafka -n kafka-ns

# Rollback to specific revision
helm rollback kafka 2 -n kafka-ns  # Revision 2
```

### 11.8 Delete Deployment

```bash
# Helm
helm uninstall kafka -n kafka-ns

# Manifests
kubectl delete -f kafka.yaml -f kafka-ui.yaml -n kafka-ns

# Cleanup PVCs (optional, to free storage)
kubectl delete pvc --all -n kafka-ns
```

---

## 12. Troubleshooting

### 12.1 Pod Not Starting

```bash
kubectl describe pod kafka-0 -n kafka-ns
kubectl logs kafka-0 -n kafka-ns

# Common issues:
# - PVC not provisioning (check storage class)
# - Namespace mismatch (helm -n flag vs values.yaml)
# - Image not found (check registry/repository/tag)
# - InsufficientMemory (check node resources)
```

### 12.2 Cannot Connect to Broker

```bash
# Inside test pod:
nc -zv kafka 9092           # Test connectivity
kafka-topics.sh --list --bootstrap-server kafka:9092

# Check service
kubectl get svc -n kafka-ns
kubectl get endpoints kafka -n kafka-ns

# Common issues:
# - Service not created
# - Pod not ready (readiness probe failing)
# - Network policy blocking traffic
```

### 12.3 Cluster Not Forming (KRaft issues)

```bash
# Check each pod's log for controller quorum
kubectl logs kafka-0 -n kafka-ns | grep -i "quorum\|controller"

# Verify DNS resolution
kubectl run --rm -it --image=alpine:3.18 --restart=Never test-dns -- \
  nslookup kafka-0.kafka-headless.kafka-ns.svc.cluster.local

# Common issues:
# - Pods not discovering each other
# - CONTROLLER_QUORUM_VOTERS mismatch
# - TCP connectivity on port 9093 blocked
```

### 12.4 Storage Issues

```bash
# Check PVCs
kubectl get pvc -n kafka-ns

# Check PV
kubectl get pv

# Describe problematic PVC
kubectl describe pvc kafka-data-kafka-0 -n kafka-ns

# Check storage class
kubectl get storageclass

# Common issues:
# - Storage class doesn't exist
# - Storage class doesn't have provisioner
# - Insufficient storage capacity
```

---

## 13. Production Checklist

### Deployment Checklist

- [ ] Use **3 brokers minimum** for HA (odd number)
- [ ] Set **replication factor = 3** for all topics
- [ ] Set **minInSyncReplicas = 2** (requires acks=all)
- [ ] Use **persistent storage** (ephemeral: false)
- [ ] Verify **storage class exists** (kubectl get storageclass)
- [ ] Set **storage size appropriately** (consider retention * throughput)
- [ ] Configure **resource requests/limits** (no burstable)
- [ ] Enable **health checks** (liveness + readiness)
- [ ] Set **pod anti-affinity to required** (not preferred)
- [ ] Configure **graceful shutdown** (terminationGracePeriodSeconds: 60+)
- [ ] Setup **monitoring** (Kafka UI or Prometheus)
- [ ] Test **failover** (kill a broker, verify recovery)
- [ ] Plan **backup strategy** (backup PVCs regularly)

### Sizing Recommendation

| Component | Development | Production |
|-----------|-------------|-----------|
| Brokers | 1 | 3-5 |
| CPU per broker | 500m | 2-4 cores |
| Memory per broker | 1Gi | 4-8Gi |
| Storage per broker | 20Gi | 100Gi+ |
| Replication factor | 1 | 3 |
| minISR | 1 | 2 |
| Retention | 7 days | 30-90 days |
| Network threads | 8 | 16-32 |

### Monitoring Strategy

1. **Kafka UI** - Web dashboard (included)
2. **JMX Metrics** - CPU, memory, GC (not configured)
3. **Kubernetes** - Pod status, PVC usage, node resources
4. **Application logs** - Consumer lag, producer errors

---

## 14. Summary

**This Helm chart provides:**

| Feature | Implementation | Production Ready |
|---------|-----------------|------------------|
| Multi-node Kafka | StatefulSet + headless service | ✅ Yes |
| Replication | Configurable RF, minISR | ✅ Yes |
| KRaft Mode | Dynamic quorum voter discovery | ✅ Yes |
| Health Checks | TCP probes (liveness + readiness) | ✅ Yes |
| HA Setup | Pod anti-affinity | ✅ Yes |
| Storage | PVC with volumeClaimTemplates | ✅ Yes |
| Security | RBAC, security context, non-root | ✅ Yes |
| Monitoring | Kafka UI included | ✅ Yes (basic) |
| Scaling | One-command via Helm | ✅ Yes |
| Rollback | Full Helm versioning support | ✅ Yes |

---

## 15. Quick Reference Commands

```bash
# Install
helm install kafka ./kafka -n kafka-ns --create-namespace --set kafka.replicas=3

# Check status
kubectl get pods -n kafka-ns
kubectl get svc -n kafka-ns
helm status kafka -n kafka-ns

# Scale
helm upgrade kafka ./kafka -n kafka-ns --set kafka.replicas=5

# Logs
kubectl logs kafka-0 -n kafka-ns -f

# Access Kafka UI
kubectl port-forward -n kafka-ns svc/kafka-ui 8080:8080
# Visit: http://localhost:8080

# Test broker
kubectl run -it --rm --image=bitnami/kafka:4.0.0 --restart=Never kafka-test -- \
  kafka-topics.sh --list --bootstrap-server kafka:9092

# Upgrade Kafka version
helm upgrade kafka ./kafka -n kafka-ns --set image.tag=4.2.0

# Rollback
helm rollback kafka -n kafka-ns

# Uninstall
helm uninstall kafka -n kafka-ns

# Cleanup PVCs
kubectl delete pvc --all -n kafka-ns
```

---

**Document Generated:** Analysis of Kafka Helm Chart v1.0.0 (Kafka 4.0.0 + KRaft Mode)

