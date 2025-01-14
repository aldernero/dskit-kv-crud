package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

const (
  defaultBindPort = 7946
  defaultBindAddr = "127.0.0.1"
  messageBufferBytes = 10*1024*1024 // 10 MiB
)

var (
  logger log.Logger
  bindAddr string
  bindPort int
  joinMember string
  client *memberlist.Client
)


func ProtoDescFactory() proto.Message {
  return NewDesc()
}

func NewDesc() *Desc {
  return &Desc{}
}

// GetCodec returns the codec used to encode and decode data being put by ring.
func GetCodec() codec.Codec {
	return codec.NewProtoCodec("exampleDesc", ProtoDescFactory)
}

// make Desc mergeable
func (d *Desc) Merge(other memberlist.Mergeable, _ bool) (change memberlist.Mergeable, error error) {
	return d.mergeWithTime(other)
}

func (d *Desc) mergeWithTime(mergeable memberlist.Mergeable) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*Desc)
	if !ok {
		return nil, fmt.Errorf("expected *Desc, got %T", mergeable)
	}

	if other == nil {
		return nil, nil
	}

	changed := false
	if other.Data == d.Data {
		if other.UpdatedAt > d.UpdatedAt {
			*d = *other
			changed = true
		} else if d.UpdatedAt == other.UpdatedAt && d.CreatedAt == 0 && other.CreatedAt != 0 {
			*d = *other
			changed = true
		}
	}

	if !changed {
		return nil, nil
	}

	out := NewDesc()
	*out = *d
	return out, nil
}

func (d *Desc) MergeContent() []string {
	result := []string(nil)
	if len(d.Data) != 0 {
		result = append(result, d.String())
	}
	return result
}

func (d *Desc) RemoveTombstones(_ time.Time) (total, removed int) {
	return
}

func (d *Desc) Clone() memberlist.Mergeable {
  return proto.Clone(d).(*Desc)
}

func main() {
	flag.StringVar(&bindAddr, "bindaddr", "127.0.0.1", "bindaddr for this specific peer")
	flag.IntVar(&bindPort, "bindport", 7946, "bindport for this specific peer")
	flag.StringVar(&joinMember, "join-member", "", "peer addr that is part of existing cluster")

  flag.Parse()

  logger = log.NewLogfmtLogger(os.Stdout)
  logger = level.NewFilter(logger, level.AllowDebug())
  logger = log.With(logger, "ts", log.DefaultTimestampUTC)

  ctx := context.Background()

	joinmembers := make([]string, 0)
	if joinMember != "" {
		joinmembers = append(joinmembers, joinMember)
	}

	// start memberlist service.
	memberlistsvc := SimpleMemberlistKV(bindAddr, bindPort, joinmembers)
	if err := services.StartAndAwaitRunning(ctx, memberlistsvc); err != nil {
		panic(err)
	}
	defer services.StopAndAwaitTerminated(ctx, memberlistsvc)


	store, err := memberlistsvc.GetMemberlistKV()
	if err != nil {
		panic(err)
	}

	client, err = memberlist.NewClient(store, GetCodec())
	if err != nil {
		panic(err)
	}

	listener, err := net.Listen("tcp", net.JoinHostPort(bindAddr, "8100"))
	if err != nil {
		panic(err)
	}

	fmt.Println("listening on ", listener.Addr())

	mux := http.NewServeMux()
	mux.Handle("/memberlist", memberlistsvc)
  mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
    level.Debug(logger).Log("get", "<all>")
    data, err := client.List(context.Background(), "")
    if err != nil {
      level.Error(logger).Log("err", err)
      w.WriteHeader(http.StatusServiceUnavailable)
      return
    }
    w.Header().Set("Content-Type", "application/json")
    err = json.NewEncoder(w).Encode(data)
    if err != nil {
      level.Error(logger).Log("err", err)
    }
  })
  mux.HandleFunc("/kv/{id}", func(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    level.Debug(logger).Log("get", id)
    desc, err := client.Get(context.Background(), id)
    if err != nil {
      level.Error(logger).Log("err", err)
      w.WriteHeader(http.StatusNotFound)
      return
    }
    if desc == nil {
      level.Error(logger).Log("err", err)
      w.WriteHeader(http.StatusNoContent)
      return
    }
    w.Header().Set("Content-Type", "application/json")
    err = json.NewEncoder(w).Encode(desc)
    if err != nil {
      level.Error(logger).Log("err", err)
    }
  })
  mux.HandleFunc("POST /kv/{id}/", func(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    level.Debug(logger).Log("add", id)
    now := time.Now().UnixMilli()
    var desc *Desc
    err = client.CAS(context.Background(), id, func(in interface{}) (out interface{}, retry bool, err error) {
      var ok bool
      if desc, ok = in.(*Desc); ok {
        if now - desc.UpdatedAt < 1000 {
          return nil, false, nil
        }
      }
      desc = &Desc{
        Data: id,
        UpdatedAt: now,
        CreatedAt: 0,
      }
      return desc, true, nil
    })
  })
  mux.HandleFunc("DELETE /kv/{id}/", func(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    level.Debug(logger).Log("delete", id)
    err = client.Delete(context.Background(), id)
    if err != nil {
      level.Debug(logger).Log("err", err)
    }
  })

	panic(http.Serve(listener, mux))
}


func SimpleMemberlistKV(bindaddr string, bindport int, joinmembers []string) *memberlist.KVInitService {
	var config memberlist.KVConfig
	flagext.DefaultValues(&config)
  config.MessageHistoryBufferBytes = messageBufferBytes 

	// Codecs is used to tell memberlist library how to serialize/de-serialize the messages between peers.
	config.Codecs = []codec.Codec{GetCodec()}

	// TCPTransport defines what addr and port this particular peer should listen on.
	config.TCPTransport = memberlist.TCPTransportConfig{
		BindPort:  bindport,
		BindAddrs: []string{bindaddr},
	}

	// joinmembers are the addresses of peers who are already in the memberlist group.
	// Usually provided if this peer is trying to join an existing cluster.
	// Generally you start the very first peer without `joinmembers`, but start all
	// other peers with at least one `joinmembers`.
	if len(joinmembers) > 0 {
		config.JoinMembers = joinmembers
	}

	// resolver defines how each peers IP address should be resolved.
	// We use default resolver comes with Go.
	resolver := dns.NewProvider(log.With(logger, "component", "dns"), prometheus.NewPedanticRegistry(), dns.GolangResolverType)

	config.NodeName = bindaddr
	config.StreamTimeout = 5 * time.Second

	return memberlist.NewKVInitService(
		&config,
		log.With(logger, "component", "memberlist"),
		resolver,
		prometheus.NewPedanticRegistry(),
	)

}
