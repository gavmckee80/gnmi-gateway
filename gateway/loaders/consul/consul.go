package consul

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	_ "github.com/hashicorp/consul/api/watch"
	"github.com/openconfig/gnmi-gateway/gateway/configuration"
	"github.com/openconfig/gnmi-gateway/gateway/connections"
	"github.com/openconfig/gnmi-gateway/gateway/loaders"
	_ "github.com/openconfig/gnmi/target"

	"strconv"

	"github.com/google/gnxi/utils/xpath"
	"github.com/hashicorp/consul/api"
	"github.com/openconfig/gnmi/proto/gnmi"
	targetpb "github.com/openconfig/gnmi/proto/target"
)

const Name = "consul"

var _ loaders.TargetLoader = new(ConsulTargetLoader)

type ConsulTargetLoader struct {
	client    *api.Client
	config    *configuration.GatewayConfig
	last      *targetpb.Configuration
	host      string
	prefix    string
	token     string
	tlsConfig configuration.TLSConfig
	interval  time.Duration
}

// consul represents the consul distributed key-value storage.

type consulConfig struct {
	Address string
	Prefix  string

	TLSConfig configuration.TLSConfig
}

type GNMIDevice struct {
	Hostname     string   `json:"hostname"`
	Os           string   `json:"os"`
	IPAddress    string   `json:"ip-address"`
	Port         int      `json:"port"`
	Subscription []string `json:"subscription"`
	Tags         []string `json:"tags"`
}

func init() {
	loaders.Register(Name, NewConsulTargetLoader)
}

func NewConsulTargetLoader(config *configuration.GatewayConfig) loaders.TargetLoader {
	return &ConsulTargetLoader{
		config: config,
		host:   config.TargetLoaders.ConsulServer,
		prefix: config.TargetLoaders.ConsulPrefix,
		token:  config.TargetLoaders.ConsulToken,
	}
}

func (m *ConsulTargetLoader) GetConfiguration() (*targetpb.Configuration, error) {

	client := m.client

	kv := client.KV()

	keys, _, err := kv.List(m.config.TargetLoaders.ConsulPrefix, nil)
	if err != nil {
		err = fmt.Errorf("Unable to load configuration from Consul prefix '%s': %v", m.config.TargetLoaders.ConsulPrefix, err)
		m.config.Log.Error().Msg(err.Error())
		return nil, err
	}

	configs := &targetpb.Configuration{
		Target:  make(map[string]*targetpb.Target),
		Request: make(map[string]*gnmi.SubscribeRequest),
	}

	for _, v := range keys {

		var devices []GNMIDevice

		err := json.Unmarshal([]byte(v.Value), &devices)

		if err != nil {
			err = fmt.Errorf("Unable unmashal data into GNMI Device struct %s", err)
			m.config.Log.Error().Msg(err.Error())
			return nil, err
		}

		for _, device := range devices {

			var subs []*gnmi.Subscription

			for _, path := range device.Subscription {
				s, err := xpath.ToGNMIPath(path)

				if err != nil {
					err = fmt.Errorf("Error parsing subscriptions to GNMI Subscription object %s", err)
					m.config.Log.Error().Msg(err.Error())
					return nil, err
				}

				subs = append(subs, &gnmi.Subscription{Path: s})
			}

			ip := net.ParseIP(device.IPAddress)

			ipBytes, _ := ip.MarshalText()

			address := string(ipBytes) + ":" + strconv.Itoa(device.Port)

			configs.Request["default"] = &gnmi.SubscribeRequest{
				Request: &gnmi.SubscribeRequest_Subscribe{
					Subscribe: &gnmi.SubscriptionList{
						Prefix:       &gnmi.Path{},
						Subscription: subs,
					},
				},
			}

			configs.Target[device.Hostname] = &targetpb.Target{
				Addresses: []string{address},
				Request:   "default",
				Credentials: &targetpb.Credentials{
					Username: "",
					Password: "",
				},
			}
		}
	}

	return configs, nil
}

func (m *ConsulTargetLoader) Start() error {

	apiConfig := api.DefaultConfig()
	apiConfig.Address = m.host
	apiConfig.TLSConfig.InsecureSkipVerify = true
	apiConfig.Token = m.token

	client, err := api.NewClient(apiConfig)

	if err != nil {
		err = fmt.Errorf("Error creating a new Consul client connection %s", err)
		m.config.Log.Error().Msg(err.Error())
		return err

	}

	m.client = client

	return err
}

func (m *ConsulTargetLoader) WatchConfiguration(targetChan chan<- *connections.TargetConnectionControl) error {
	return nil
}
