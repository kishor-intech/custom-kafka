# Kafka External Access Guide

## Problem
When connecting to Kafka from outside the Kubernetes cluster (e.g., from localhost via port-forward), clients receive broker advertised addresses pointing to internal Kubernetes DNS names which are not resolvable externally.

## Solutions

### Solution 1: Dynamic Bootstrap Override (Recommended for Development)

This is now configured in `values.yaml`. Simply set the external bootstrap host before deployment:

#### For Local Testing with Port-Forward:
```bash
# Set external bootstrap host to localhost
helm install kafka . -n k \
  --set kafka.externalBootstrapHost="localhost" \
  --set kafka.externalBootstrapPort="9092"

# Port-forward the service
kubectl port-forward -n k service/kafka-headless 9092:9092

# Connect from localhost
export ip="localhost:9092"
./kafka-topics.sh --bootstrap-server $ip --list
```

#### For Production with DNS:
```bash
# Set external bootstrap host to your production DNS
helm install kafka . -n k \
  --set kafka.externalBootstrapHost="kafka.example.com" \
  --set kafka.externalBootstrapPort="9092"
```

#### For AWS/Cloud LoadBalancer:
```bash
# Get the LoadBalancer IP/DNS
KAFKA_LB=$(kubectl get svc -n k kafka-external -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

helm install kafka . -n k \
  --set kafka.externalBootstrapHost="$KAFKA_LB" \
  --set kafka.externalBootstrapPort="9092"
```

### Solution 2: Kubernetes /etc/hosts Mapping

Add the Kafka pod DNS to your local /etc/hosts:
```bash
sudo echo "127.0.0.1 kafka-0.kafka-headless.k.svc.cluster.local" >> /etc/hosts

# Now connect via port-forward without setting externalBootstrapHost
kubectl port-forward -n k service/kafka-headless 9092:9092
export ip="localhost:9092"
./kafka-topics.sh --bootstrap-server $ip --list
```

### Solution 3: NodePort Service (for Kind/Local Clusters)

Modify `templates/service.yaml` to use NodePort:
```yaml
service:
  type: NodePort  # Instead of LoadBalancer
  nodePort: 30092
```

Then connect to the node IP:
```bash
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
export ip="$NODE_IP:30092"
```

## Testing Connectivity

### 1. Forward the port
```bash
kubectl port-forward -n k service/kafka-headless 9092:9092 &
```

### 2. Create a test topic
```bash
./kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-topic \
  --partitions 1 --replication-factor 1
```

### 3. List topics
```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 4. Produce messages
```bash
echo "test message" | ./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

### 5. Consume messages
```bash
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

## How It Works

When you set `externalBootstrapHost`, the Kafka broker's advertised listeners change:

**Without externalBootstrapHost (default):**
```
advertised.listeners=PLAINTEXT://kafka-0.kafka-headless.k.svc.cluster.local:9092
```
↓ Clients connecting to `localhost:9092` (via port-forward) get redirected to the internal DNS, which fails.

**With externalBootstrapHost set:**
```
advertised.listeners=PLAINTEXT://localhost:9092
```
↓ Clients connecting to `localhost:9092` work correctly because the broker advertises the same address.

## Environment Variables Used

In the StatefulSet pod, these environment variables control the behavior:

- `EXTERNAL_BOOTSTRAP_HOST` - The hostname/IP to advertise for external clients
- `EXTERNAL_BOOTSTRAP_PORT` - The port for external access (default: 9092)

These are automatically injected from `values.yaml`:
```yaml
kafka:
  externalBootstrapHost: "localhost"      # Set this based on your access method
  externalBootstrapPort: 9092
```

## Updating an Existing Deployment

To change the external bootstrap host on an existing deployment:

```bash
helm upgrade kafka . -n k \
  --set kafka.externalBootstrapHost="your-new-host"
```

The pods will automatically restart and use the new configuration.

## Troubleshooting

### "Connection refused" or "Could not establish connection"
- Ensure port-forward is running: `kubectl port-forward -n k service/kafka-headless 9092:9092`
- Check pod logs: `kubectl logs -n k kafka-0`
- Verify the external bootstrap host matches your connection method

### "Temporary failure in name resolution"
- If using /etc/hosts method, verify: `cat /etc/hosts | grep kafka`
- If using LoadBalancer, wait for IP assignment: `kubectl get svc -n k kafka-external -w`

### Broker still advertises internal DNS
- Check pod environment: `kubectl exec -n k kafka-0 -- env | grep EXTERNAL`
- Verify values were set: `helm get values kafka -n k | grep externalBootstrap`
- Check broker logs for the advertised listeners: `kubectl logs -n k kafka-0 | grep advertised`
