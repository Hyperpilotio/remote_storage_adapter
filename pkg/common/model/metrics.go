package model

type MetricCatalog struct {
	Keys []string `json:"namespaces"`
}

func (mc *MetricCatalog) Add(namespace string) {
	mc.Keys = appendIfMissing(mc.Keys, namespace)
}

func (mc *MetricCatalog) Namespaces() []string {
	return mc.Keys
}

func appendIfMissing(keys []string, namespace string) []string {
	for _, key := range keys {
		if namespace == key {
			return keys
		}
	}
	return append(keys, namespace)
}
