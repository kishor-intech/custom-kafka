#!/bin/bash
# Kafka External Connection Helper Script
# Quickly set up Kafka for external access with different methods

set -e

NAMESPACE=${NAMESPACE:-k}
KAFKA_RELEASE=${KAFKA_RELEASE:-kafka}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
  echo -e "${BLUE}==== $1 ====${NC}"
}

print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
  echo -e "${RED}✗ $1${NC}"
}

show_menu() {
  print_header "Kafka External Access Setup"
  echo ""
  echo "Choose your connection method:"
  echo ""
  echo "1) Local Testing with Port-Forward (localhost:9092)"
  echo "2) Kubernetes DNS (kafka-0.kafka-headless.k.svc.cluster.local)"
  echo "3) Kind NodePort (10.X.X.X:30092)"
  echo "4) AWS/Cloud LoadBalancer (auto-detect)"
  echo "5) Custom Host"
  echo "6) View current configuration"
  echo "7) Port-forward only (don't change config)"
  echo ""
}

setup_localhost() {
  print_header "Setting up for Local Testing"
  print_warning "This requires port-forwarding to be active"
  
  echo ""
  echo "Running: helm upgrade $KAFKA_RELEASE . -n $NAMESPACE \\"
  echo "  --set kafka.externalBootstrapHost=localhost"
  echo ""
  
  helm upgrade "$KAFKA_RELEASE" . -n "$NAMESPACE" \
    --set kafka.externalBootstrapHost="localhost" \
    --set kafka.externalBootstrapPort="9092"
  
  print_success "Updated Kafka configuration"
  print_warning "Now run this in another terminal:"
  echo ""
  echo "  kubectl port-forward -n $NAMESPACE service/kafka-headless 9092:9092"
  echo ""
  echo "Then use bootstrap server: localhost:9092"
}

setup_k8s_dns() {
  print_header "Setting up for Kubernetes DNS"
  
  echo ""
  echo "Running: helm upgrade $KAFKA_RELEASE . -n $NAMESPACE \\"
  echo "  --set kafka.externalBootstrapHost=\"\""
  echo ""
  
  helm upgrade "$KAFKA_RELEASE" . -n "$NAMESPACE" \
    --set kafka.externalBootstrapHost=""
  
  print_success "Updated to use internal Kubernetes DNS"
  print_warning "Now run this in another terminal:"
  echo ""
  echo "  kubectl port-forward -n $NAMESPACE service/kafka-headless 9092:9092"
  echo ""
  echo "Then add to your /etc/hosts:"
  echo "  127.0.0.1 kafka-0.kafka-headless.$NAMESPACE.svc.cluster.local"
  echo ""
  echo "Use bootstrap server: kafka-0.kafka-headless.$NAMESPACE.svc.cluster.local:9092"
}

setup_nodeport() {
  print_header "Setting up for Kind/NodePort"
  
  # Get node IP
  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "10.X.X.X")
  
  echo ""
  echo "Node IP detected: $NODE_IP"
  echo ""
  echo "Running: helm upgrade $KAFKA_RELEASE . -n $NAMESPACE \\"
  echo "  --set kafka.externalBootstrapHost=$NODE_IP"
  echo ""
  
  helm upgrade "$KAFKA_RELEASE" . -n "$NAMESPACE" \
    --set kafka.externalBootstrapHost="$NODE_IP"
  
  print_success "Updated Kafka configuration for NodePort"
  echo ""
  echo "Use bootstrap server: $NODE_IP:9092"
}

setup_loadbalancer() {
  print_header "Detecting Cloud LoadBalancer"
  
  # Wait for LoadBalancer IP
  echo "Waiting for LoadBalancer IP assignment..."
  
  LB_IP=$(kubectl get svc -n "$NAMESPACE" kafka-external \
    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || \
    kubectl get svc -n "$NAMESPACE" kafka-external \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  
  if [ -z "$LB_IP" ]; then
    print_error "LoadBalancer IP not assigned yet"
    echo ""
    print_warning "This can take a few minutes on cloud providers"
    echo "Check status with:"
    echo "  kubectl get svc -n $NAMESPACE kafka-external -w"
    return 1
  fi
  
  print_success "LoadBalancer IP detected: $LB_IP"
  echo ""
  echo "Running: helm upgrade $KAFKA_RELEASE . -n $NAMESPACE \\"
  echo "  --set kafka.externalBootstrapHost=$LB_IP"
  echo ""
  
  helm upgrade "$KAFKA_RELEASE" . -n "$NAMESPACE" \
    --set kafka.externalBootstrapHost="$LB_IP"
  
  print_success "Updated Kafka configuration for LoadBalancer"
  echo ""
  echo "Use bootstrap server: $LB_IP:9092"
}

setup_custom() {
  print_header "Custom Host Setup"
  echo ""
  read -p "Enter the hostname/IP to use as bootstrap: " CUSTOM_HOST
  read -p "Enter the port [9092]: " CUSTOM_PORT
  CUSTOM_PORT=${CUSTOM_PORT:-9092}
  
  echo ""
  echo "Running: helm upgrade $KAFKA_RELEASE . -n $NAMESPACE \\"
  echo "  --set kafka.externalBootstrapHost=$CUSTOM_HOST \\"
  echo "  --set kafka.externalBootstrapPort=$CUSTOM_PORT"
  echo ""
  
  helm upgrade "$KAFKA_RELEASE" . -n "$NAMESPACE" \
    --set kafka.externalBootstrapHost="$CUSTOM_HOST" \
    --set kafka.externalBootstrapPort="$CUSTOM_PORT"
  
  print_success "Updated Kafka configuration"
  echo ""
  echo "Use bootstrap server: $CUSTOM_HOST:$CUSTOM_PORT"
}

show_config() {
  print_header "Current Configuration"
  echo ""
  helm get values "$KAFKA_RELEASE" -n "$NAMESPACE" | grep -A 2 externalBootstrap || \
    echo "No external bootstrap host configured"
  echo ""
  echo "Pod advertised listeners:"
  kubectl logs -n "$NAMESPACE" "$KAFKA_RELEASE"-0 2>/dev/null | grep "Advertised:" | tail -1 || \
    echo "Pod not yet started or logs not available"
}

setup_portforward_only() {
  print_header "Port-Forward Setup"
  echo ""
  print_warning "This doesn't change Kafka config, just sets up port forwarding"
  echo ""
  echo "Starting: kubectl port-forward -n $NAMESPACE service/kafka-headless 9092:9092"
  echo ""
  
  kubectl port-forward -n "$NAMESPACE" service/kafka-headless 9092:9092
}

# Main menu loop
while true; do
  show_menu
  read -p "Select option [1-7]: " choice
  echo ""
  
  case $choice in
    1) setup_localhost ;;
    2) setup_k8s_dns ;;
    3) setup_nodeport ;;
    4) setup_loadbalancer ;;
    5) setup_custom ;;
    6) show_config ;;
    7) setup_portforward_only ;;
    *)
      print_error "Invalid option"
      ;;
  esac
  
  echo ""
  read -p "Press Enter to continue..."
  clear
done
