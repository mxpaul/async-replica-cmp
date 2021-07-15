package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kylelemons/godebug/pretty"
	flag "github.com/spf13/pflag"
	"github.com/valyala/fasthttp"
	"golang.org/x/sync/errgroup"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type CmdLineOptions struct {
	MaxParallelWorkers uint
	SrcHost            string
	DstHost            string
	ShardNumber        int
	ShardCapasity      int
	NoLogDifference    bool
}

func NewCmdLineOptionsOrDie() *CmdLineOptions {
	opt := CmdLineOptions{}
	flag.UintVar(&opt.MaxParallelWorkers, "workers", uint(10), "work on this number of products in parallel")
	flag.StringVar(&opt.SrcHost, "src-host", "http://127.0.0.1:2222", "replica host 1 for comparation")
	flag.StringVar(&opt.DstHost, "dst-host", "http://127.0.0.1:3333", "replica host 2 for comparation")
	flag.IntVar(&opt.ShardNumber, "shard", int(32), "master-part number for product id range construction")
	flag.IntVar(&opt.ShardCapasity, "shard-capasity", int(1e6), "master-part number for product id range construction")
	flag.BoolVar(&opt.NoLogDifference, "no-log-diff", false, "log entire products and diff when found")
	flag.Parse()
	if opt.MaxParallelWorkers == 0 {
		log.Fatalf("workers should be positive")
	}
	return &opt
}

func (opt *CmdLineOptions) String() string {
	log_diff := "logdiff"
	if opt.NoLogDifference {
		log_diff = "nologdiff"
	}
	return fmt.Sprintf("[%s] <-> [%s] workers: %d; %s",
		opt.SrcHost,
		opt.DstHost,
		opt.MaxParallelWorkers,
		log_diff,
	)
}

type StatCounter struct {
	mu               sync.Mutex
	productsFound    int
	productsStarted  int
	productsCompared int
	productsDiffers  int
}

func (stat *StatCounter) IncProductsFound() {
	stat.mu.Lock()
	stat.productsFound++
	stat.mu.Unlock()
}

func (stat *StatCounter) IncProductsStarted() {
	stat.mu.Lock()
	stat.productsStarted++
	stat.mu.Unlock()
}
func (stat *StatCounter) IncProductsCompared() {
	stat.mu.Lock()
	stat.productsCompared++
	stat.mu.Unlock()
}
func (stat *StatCounter) IncProductsDiffers() {
	stat.mu.Lock()
	stat.productsDiffers++
	stat.mu.Unlock()
}

func (stat *StatCounter) String() string {
	return fmt.Sprintf("found: %d; proc: %d;  cmp: %d; diff: %d",
		stat.productsFound,
		stat.productsStarted,
		stat.productsCompared,
		stat.productsDiffers,
	)
}

func (scope *Scope) GetProductFromHost(host string, productId string) (map[string]interface{}, error) {
	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseResponse(response)
		fasthttp.ReleaseRequest(request)
	}()

	var urlReq []byte
	urlReq = append(urlReq, host...)
	urlReq = append(urlReq, `/product?locale=ru&id=`...)
	urlReq = append(urlReq, productId...)
	request.SetRequestURIBytes(urlReq)
	request.Header.SetMethod("GET")
	//request.SetBody(filterOptions)
	if err := scope.HttpClient.DoTimeout(request, response, 1*time.Second); err != nil {
		log.Println("Request error:", err.Error())
		return nil, err
	}
	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("req %q status code: %v", urlReq, response.StatusCode())
	}
	contLenBytes := response.Header.Peek("Content-Length")
	if len(contLenBytes) > 0 {
		length, err := strconv.Atoi(string(contLenBytes))
		if err != nil {
			return nil, fmt.Errorf("req %q content-length invalid value: %v", urlReq, contLenBytes)
		}
		if length == 0 {
			return nil, nil
		}
	} else {
		return nil, fmt.Errorf("req %q content-length header is missing", urlReq)
	}
	product := map[string]interface{}{}
	if err := json.Unmarshal(response.Body(), &product); err != nil {
		return nil, fmt.Errorf("req %q unmarshal error: %v", urlReq, err)
	}
	return product, nil
}

func CompareProducts(src, dst map[string]interface{}) string {
	shouldNormalize := true
	if src != nil && dst != nil {
		ksortSrcInterface, srcHasKSort := src["KSort"]
		ksortDstInterface, dstHasKSort := dst["KSort"]
		if srcHasKSort && dstHasKSort {
			ksortSrc, okSrc := ksortSrcInterface.(float64)
			ksortDst, okDst := ksortDstInterface.(float64)
			if okSrc && okDst && ksortSrc != ksortDst {
				log.Printf("ksort differs, skip normalize")
				shouldNormalize = false
			}
		}
	}
	if shouldNormalize {
		NormalizeProductData(src)
		NormalizeProductData(dst)
	}
	diff := pretty.Compare(src, dst)
	if diff != "" {
		return fmt.Sprintf("SrcProduct: %s\nDstProduct: %s\nDiff: %s", src, dst, diff)
	}
	return diff
}

type StockSortable []interface{}

func (s StockSortable) Len() int      { return len(s) }
func (s StockSortable) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s StockSortable) Less(i, j int) bool {
	a, convertOk := s[i].(map[string]interface{})
	if !convertOk {
		return false
	}
	b, convertOk := s[j].(map[string]interface{})
	if !convertOk {
		return false
	}
	A := a["id"].(float64)
	B := b["id"].(float64)
	return A < B
}

func NormalizeProductData(product map[string]interface{}) {
	if product == nil {
		return
	}

	delete(product, "StockUpdateTimestamp")

	if sizes, haveSizes := product["sizes"]; haveSizes {
		if sizes, convertOk := sizes.([]interface{}); convertOk {
			for _, size := range sizes {
				if size, convertOk := size.(map[string]interface{}); convertOk {
					if stocks, haveStocks := size["stocks"]; haveStocks {
						if stocks, convertOk := stocks.([]interface{}); convertOk {
							sort.Sort(StockSortable(stocks))
						}
					}
				}
			}
		}
	}

}

type CheckResult struct {
	Error     error
	ProductID int
	Diff      string
}

type Scope struct {
	HttpClient  *fasthttp.Client
	SrcHost     string
	DstHost     string
	FanOutChan  chan int
	FanInChan   chan *CheckResult
	WorkerCount uint
	Stats       *StatCounter
}

func NewScope(opt *CmdLineOptions) *Scope {
	return &Scope{
		HttpClient:  &fasthttp.Client{MaxConnsPerHost: 100},
		SrcHost:     opt.SrcHost,
		DstHost:     opt.DstHost,
		WorkerCount: opt.MaxParallelWorkers,
		FanOutChan:  make(chan int, opt.MaxParallelWorkers*2),
		FanInChan:   make(chan *CheckResult, opt.MaxParallelWorkers*2),
		Stats:       &StatCounter{},
	}
}

func (scope *Scope) GetProductDIff(productId string) (string, error) {
	requests := errgroup.Group{}

	var productSrc, productDst map[string]interface{}
	requests.Go(func() (err error) {
		productSrc, err = scope.GetProductFromHost(scope.SrcHost, productId)
		return err
	})
	requests.Go(func() (err error) {
		productDst, err = scope.GetProductFromHost(scope.DstHost, productId)
		return err
	})
	err := requests.Wait()
	if err != nil {
		return "", err
	}
	return CompareProducts(productSrc, productDst), nil
}

func Worker(scope *Scope, wg *sync.WaitGroup) {
	wg.Add(1)
	for productId := range scope.FanOutChan {
		scope.Stats.IncProductsStarted()
		productIdString := strconv.Itoa(productId)
		diff, err := scope.GetProductDIff(productIdString)
		if err != nil {
			log.Fatalf("GetProductDIff(%q) error: %v", productIdString, err)
		}
		checkResult := CheckResult{
			ProductID: productId,
			Error:     err,
			Diff:      diff,
		}
		scope.FanInChan <- &checkResult
	}
	wg.Done()
}

func Aggregator(scope *Scope, opt *CmdLineOptions, done chan<- struct{}) {
	for checkResult := range scope.FanInChan {
		if checkResult.Error != nil {
			log.Fatalf("GetProductDIff(%v) error: %v", checkResult.ProductID, checkResult.Error)
		}
		scope.Stats.IncProductsCompared()
		if checkResult.Diff != "" {
			scope.Stats.IncProductsDiffers()
			if !opt.NoLogDifference {
				log.Printf("Diff: %+v", checkResult.Diff)
			}
		}
		if checkResult.ProductID%100000 == 0 {
			log.Printf("Progress: %v; Stat: %s", checkResult.ProductID, scope.Stats.String())
		}
	}
	close(done)
}

func main() {
	opt := NewCmdLineOptionsOrDie()
	log.Printf("Start: %s", opt.String())

	problematicID := 32961880

	scope := NewScope(opt)

	var wg sync.WaitGroup
	for i := uint(0); i < opt.MaxParallelWorkers; i++ {
		go Worker(scope, &wg)
	}
	done := make(chan struct{}, 0)
	go Aggregator(scope, opt, done)

	startTime := time.Now()
	for productId := opt.ShardNumber * opt.ShardCapasity; productId < (opt.ShardNumber+1)*opt.ShardCapasity; productId++ {
		scope.Stats.IncProductsFound()
		if productId == problematicID {
			continue
		}
		scope.FanOutChan <- productId
	}

	close(scope.FanOutChan)
	wg.Wait()
	close(scope.FanInChan)
	<-done
	log.Printf("Finished after %.02fs; Stat: %s", time.Since(startTime).Seconds(), scope.Stats.String())
}
