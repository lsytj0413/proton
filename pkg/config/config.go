// Package config contains the configuration of proton
package config

// Config ...
type Config struct {
	Etcd   *EtcdConfig
	NodeID int
}

// EtcdConfig ...
type EtcdConfig struct {
	Endpoints []string
}
