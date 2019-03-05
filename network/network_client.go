package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/rpcx-ecosystem/rpcx-benchmark/rpcx/model"
	"log"
	"net/http"
	_ "net/http/pprof"
	"qingcloud.com/qing-cloud-mq/common"
	"qingcloud.com/qing-cloud-mq/common/proto"
	"qingcloud.com/qing-cloud-mq/network"
	"qingcloud.com/qing-cloud-mq/network/remote"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/montanaflynn/stats"
	rlog "github.com/smallnest/rpcx/log"
)

var concurrency = flag.Int("c", 1, "concurrency")
var total = flag.Int("n", 1, "total requests for all clients")
var host = flag.String("s", "127.0.0.1:8972", "server ip and port")
var debugAddr = flag.String("d", "127.0.0.1:9982", "server ip and port")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	rlog.SetDummyLogger()

	go func() {
		log.Println(http.ListenAndServe(*debugAddr, nil))
	}()

	conc, tn, err := checkArgs(*concurrency, *total)
	if err != nil {
		log.Printf("err: %v", err)
		return
	}
	n := conc
	m := tn / n

	log.Printf("concurrency: %d\nrequests per client: %d\n\n", n, m)

	//servicePath := "Hello"
	//serviceMethod := "Say"

	args := prepareArgs()

	b := make([]byte, 1024*1024)
	i, _ := args.MarshalTo(b)
	log.Printf("message size: %d bytes\n\n", i)

	var wg sync.WaitGroup
	wg.Add(n * m)

	log.Printf("sent total %d messages, %d message per client", n*m, m)

	var startWg sync.WaitGroup
	startWg.Add(n)

	var trans uint64
	var transOK uint64

	d := make([][]int64, n, n)

	opts := network.NewNetworkOptions()
	opts.Name = "Echo client"
	opts.ReadBuffer = 4096
	opts.WriteBuffer = 4096
	opts.RpcTimeout = 3 * time.Second
	opts.CommandTable = common.CommandTable
	opts.RpcTimeout = 10 * time.Second
	opts.Events.Serving = func(server *network.Server) {
	}
	opts.Events.SessionActive = func(session network.Session) {
	}
	opts.Events.SessionInactive = func(session network.Session) {
		session.Close()
	}
	opts.Events.SessionRead = func(session network.Session, command *remote.RemotingCommand) {

	}

	client := network.NewClient(opts)

	//it contains warmup time but we can ignore it
	totalT := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		dt := make([]int64, 0, m)
		d = append(d, dt)

		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
					log.Print("Recovered in f", r)
				}
			}()

			session, err := client.Connect("localhost:8280")
			if err != nil {
				panic(err)
			}
			defer session.Close()

			//var reply model.BenchmarkMessage

			//warmup
			for j := 0; j < 5; j++ {
				msg := &proto.TestMessage{Type: int64(88), Index: int64(i), Id: fmt.Sprintf("Id:%d", i)}
				cmdSend := remote.NewRemotingCommand(msg)
				_, err := session.SendRpcSync(cmdSend)
				if err != nil {
					log.Println(err)
					continue
				}
			}

			startWg.Done()
			startWg.Wait()

			for j := 0; j < m; j++ {
				t := time.Now().UnixNano()

				msg := &proto.TestMessage{Type: int64(88), Index: int64(i), Id: fmt.Sprintf("Id:%d撒的发生的士大夫撒旦法撒旦法士大夫撒旦法撒旦法萨芬撒旦法撒旦法撒旦飞洒发顺丰大是大非", i)}
				cmdSend := remote.NewRemotingCommand(msg)
				_, err := session.SendRpcSync(cmdSend)
				if err != nil {
					log.Println(err)
					continue
				}

				t = time.Now().UnixNano() - t

				d[i] = append(d[i], t)

				if err == nil {
					atomic.AddUint64(&transOK, 1)
				}

				if err != nil {
					log.Print(err.Error())
				}

				atomic.AddUint64(&trans, 1)
				wg.Done()
			}
		}(i)

	}

	wg.Wait()

	totalT = time.Now().UnixNano() - totalT
	log.Printf("took %f ms for %d requests\n", float64(totalT)/1000000, n*m)

	totalD := make([]int64, 0, n*m)
	for _, k := range d {
		totalD = append(totalD, k...)
	}
	totalD2 := make([]float64, 0, n*m)
	for _, k := range totalD {
		totalD2 = append(totalD2, float64(k))
	}

	mean, _ := stats.Mean(totalD2)
	median, _ := stats.Median(totalD2)
	max, _ := stats.Max(totalD2)
	min, _ := stats.Min(totalD2)
	p99, _ := stats.Percentile(totalD2, 99.9)

	log.Printf("sent     requests    : %d\n", n*m)
	log.Printf("received requests    : %d\n", atomic.LoadUint64(&trans))
	log.Printf("received requests_OK : %d\n", atomic.LoadUint64(&transOK))
	log.Printf("throughput  (TPS)    : %d\n", int64(n*m)*1000000000/totalT)
	log.Printf("mean: %.f ns, median: %.f ns, max: %.f ns, min: %.f ns, p99.9: %.f ns\n", mean, median, max, min, p99)
	log.Printf("mean: %d ms, median: %d ms, max: %d ms, min: %d ms, p99: %d ms\n", int64(mean/1000000), int64(median/1000000), int64(max/1000000), int64(min/1000000), int64(p99/1000000))
}

// checkArgs check concurrency and total request count.
func checkArgs(c, n int) (int, int, error) {
	if c < 1 {
		log.Printf("c < 1 and reset c = 1")
		c = 1
	}
	if n < 1 {
		log.Printf("n < 1 and reset n = 1")
		n = 1
	}
	if c > n {
		return c, n, errors.New("c must be set <= n")
	}
	return c, n, nil
}

func prepareArgs() *model.BenchmarkMessage {
	b := true
	var i int32 = 100000
	var s = "许多往事在眼前一幕一幕，变的那麼模糊"

	var args model.BenchmarkMessage

	v := reflect.ValueOf(&args).Elem()
	num := v.NumField()
	for k := 0; k < num; k++ {
		field := v.Field(k)
		if field.Type().Kind() == reflect.Ptr {
			switch v.Field(k).Type().Elem().Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64:
				field.Set(reflect.ValueOf(&i))
			case reflect.Bool:
				field.Set(reflect.ValueOf(&b))
			case reflect.String:
				field.Set(reflect.ValueOf(&s))
			}
		} else {
			switch field.Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64:
				field.SetInt(100000)
			case reflect.Bool:
				field.SetBool(true)
			case reflect.String:
				field.SetString(s)
			}
		}

	}
	return &args
}
