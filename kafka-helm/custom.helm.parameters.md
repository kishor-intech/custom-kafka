testing from Inside
-------------------------

./kafka-console-producer.sh   --bootstrap-server kafka-0.kafka-headless.k5.svc.cluster.local:9092   --topic harish   --producer-property acducer-property debug=true --request-required-acks 1

./kafka-topics.sh --bootstrap-server kafka-0.kafka-headless.k3.svc.cluster.local:9092 --list

./kafka-console-consumer.sh --bootstrap-server kafka-0.kafka-headless.k5.svc.cluster.local:9092 --topic harish --from-beginning

------------------------------------------------------

10.227.252.10:8082/custom-test/kafka-test:v5

helm uninstall kfk -n kfk
kubectl delete clusterrole kfk-kafka
kubectl delete clusterrolebinding kfk-kafka
-----------------------------------------------------------


kind load docker-image test-kfk:latest --name k8s


kubectl port-forward svc/kafka-ui 8080:8080 -n k5


k delete clusterrole kafka
k delete clusterrole kafka-kafka
k delete clusterrolebinding kafka
k delete clusterrolebinding kafka-kafka


--set healthChecks.readiness.enabled=false \

Test local:
helm install t1 . -n t1 --create-namespace \
  --set namespace=t1 \
  --set image.registry="" \
  --set image.repository="t1" \
  --set image.tag="v1" \
  --set kafka.replicas=1 \
  --set kafka.storage.size=20Gi \
  --set kafka.replication.offsetsTopicReplicationFactor=1 \
  --set kafka.replication.minInSyncReplicas=1 \
  --set kafka.resources.requests.cpu=500m \
  --set kafka.resources.requests.memory=1Gi \
  --set kafka.resources.limits.cpu=1000m \
  --set kafka.resources.limits.memory=2Gi \
  --set kafka.storage.className=standard



For Dev Environment

helm install kfk . -n kfk --create-namespace \
  --set namespace=kfk \
  --set image.registry="10.227.252.10:8082" \
  --set image.repository="custom-test/kafka-test" \
  --set image.tag="v4" \
  --set kafka.replicas=3 \
  --set kafka.storage.size=20Gi \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2 \
  --set kafka.resources.requests.cpu=500m \
  --set kafka.resources.requests.memory=1Gi \
  --set kafka.resources.limits.cpu=1000m \
  --set kafka.resources.limits.memory=2Gi \
  --set kafka.storage.className=longhorn
  
  
  
For Prod Environment
  
  
  helm install kafka . -n kafka-ns --create-namespace \
  --set image.registry="docker.io" \
  --set image.repository="kafka" \
  --set image.tag="kraft-kraft-v1" \
  --set kafka.replicas=3 \
  --set kafka.storage.size=50Gi \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2 \
  --set kafka.performance.networkThreads=8 \
  --set kafka.performance.ioThreads=8 \
  --set kafka.jvmHeap.min=1G \
  --set kafka.jvmHeap.max=3G \
  --set kafka.resources.requests.cpu=1000m \
  --set kafka.resources.requests.memory=2Gi \
  --set kafka.resources.limits.cpu=4000m \
  --set kafka.resources.limits.memory=6Gi
  

helm install kfk . -n kfk --create-namespace \
  --set namespace=kfk \
  --set image.registry="10.227.252.10:8082" \
  --set image.repository="custom-test/kafka-test" \
  --set image.tag="v5" \
  --set kafka.replicas=3 \
  --set healthChecks.readiness.enabled=false \
  --set kafka.storage.size=20Gi \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2 \
  --set kafka.resources.requests.cpu=500m \
  --set kafka.resources.requests.memory=1Gi \
  --set kafka.resources.limits.cpu=1000m \
  --set kafka.resources.limits.memory=2Gi \
  --set kafka.storage.className=longhorn
  
  


helm install kfk ./my-kafka -n kfk --create-namespace \
  --set namespace=kfk \
  --set image.registry="" \
  --set image.repository="k1" \
  --set image.tag="v1" \
  --set kafka.replicas=3 \
  --set healthChecks.readiness.enabled=false \
  --set kafka.storage.size=20Gi \
  --set kafka.replication.offsetsTopicReplicationFactor=3 \
  --set kafka.replication.minInSyncReplicas=2 \
  --set kafka.resources.requests.cpu=300m \
  --set kafka.resources.requests.memory=512Mi \
  --set kafka.resources.limits.cpu=1000m \
  --set kafka.resources.limits.memory=2Gi \
  --set kafka.storage.className=standard




export ip="10.227.252.43:9094"

./kafka-console-producer.sh   --bootstrap-server $ip  --topic harish1   --producer-property acducer-property debug=true --request-required-acks 1

./kafka-topics.sh --bootstrap-server $ip --list

./kafka-console-consumer.sh --bootstrap-server $ip --topic harish1 --from-beginning

