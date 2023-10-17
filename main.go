package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type watcherFlags struct {
	Prefix    string
	CertFile  string
	KeyFile   string
	CaFile    string
	Endpoints EndpointsFlag
}
type EtcdClient struct {
	*clientv3.Client
}

type EndpointsFlag []string

func (l *EndpointsFlag) String() string {
	return fmt.Sprintf("%v", *l)
}

func (l *EndpointsFlag) Set(value string) error {
	*l = append(*l, strings.Split(value, ",")...)
	return nil
}

func newEtcdClient(f *watcherFlags) (*EtcdClient, error) {
	var tlsConfig *tls.Config
	if len(f.CertFile) == 0 && len(f.KeyFile) == 0 && len(f.CaFile) == 0 {
		tlsConfig = nil
	} else {
		cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
		if err != nil {
			fmt.Println("Failed to load cert and key:", err)
			return nil, fmt.Errorf("failed to load cert and key: %s", err)
		}

		// 加载CA
		caCert, err := ioutil.ReadFile(f.CaFile)
		if err != nil {
			fmt.Println("Failed to load CA:", err)
			return nil, fmt.Errorf("failed to load CA: %s", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// 创建TLS配置
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	}
	config := &clientv3.Config{
		DialTimeout: 5 * time.Second,
		Endpoints:   f.Endpoints,
		TLS:         tlsConfig,
	}
	c, err := clientv3.New(*config)
	if err != nil {
		return nil, err
	}
	return &EtcdClient{Client: c}, nil
}

func main() {
	var f watcherFlags
	flag.StringVar(&f.Prefix, "prefix", "/prefix", "prefix to watch")
	flag.StringVar(&f.CertFile, "cert-file", "", "path to etcd cert file")
	flag.StringVar(&f.KeyFile, "key-file", "", "path to etcd key file")
	flag.StringVar(&f.CaFile, "ca-file", "", "path to etcd ca file")
	flag.Var(&f.Endpoints, "endpoints", "etcd endpoints, comma split if more than one")

	// 解析命令行参数
	flag.Parse()

	if !strings.HasPrefix(f.Prefix, "/") {
		f.Prefix = fmt.Sprintf("/%s", f.Prefix)
	}

	cli, err := newEtcdClient(&f)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建一个 Watcher
	watcher := clientv3.NewWatcher(cli.Client)

	// 监控某个 key 的变化
	watchKey := f.Prefix
	watchRespChan := watcher.Watch(ctx, watchKey, clientv3.WithPrefix())
	defer watcher.Close()
	for {
		select {
		case watchResp := <-watchRespChan:
			// 打印监控到的变化事件
			for _, ev := range watchResp.Events {
				fmt.Printf("%s %q : %s\n", ev.Type, ev.Kv.Key, string(ev.Kv.Value))
			}

		case <-ctx.Done():
			return
		}
	}
}
