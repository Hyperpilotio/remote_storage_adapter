package model

type ClusterMetrics struct {
	ClusterId string   `bson:"clusterId" json:"clusterId"`
	Keys      []string `bson:"keys" json:"keys"`
	Size      int      `bson:"-" json:"-"`
}

func (cm *ClusterMetrics) Add(namespace string) {
	cm.Keys = appendIfMissing(cm.Keys, namespace)
}

func (cm *ClusterMetrics) Namespaces() []string {
	return cm.Keys
}

func appendIfMissing(keys []string, namespace string) []string {
	for _, key := range keys {
		if namespace == key {
			return keys
		}
	}
	return append(keys, namespace)
}
