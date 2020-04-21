package binance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/common"
	"github.com/adshao/go-binance/futures"
	"github.com/bitly/go-simplejson"
)

// SideType define side type of order
type SideType string

// OrderType define order type
type OrderType string

// TimeInForceType define time in force type of order
type TimeInForceType string

// NewOrderRespType define response JSON verbosity
type NewOrderRespType string

// OrderStatusType define order status type
type OrderStatusType string

// SymbolType define symbol type
type SymbolType string

// SymbolStatusType define symbol status type
type SymbolStatusType string

// SymbolFilterType define symbol filter type
type SymbolFilterType string

// MarginTransferType define margin transfer type
type MarginTransferType int

// MarginLoanStatusType define margin loan status type
type MarginLoanStatusType string

// MarginRepayStatusType define margin repay status type
type MarginRepayStatusType string

// FuturesTransferStatusType define futures transfer status type
type FuturesTransferStatusType string

// SideEffectType define side effect type for orders
type SideEffectType string

// FuturesTransferType define futures transfer type
type FuturesTransferType int

// Global enums
const (
	SideTypeBuy  SideType = "BUY"
	SideTypeSell SideType = "SELL"

	OrderTypeLimit           OrderType = "LIMIT"
	OrderTypeMarket          OrderType = "MARKET"
	OrderTypeLimitMaker      OrderType = "LIMIT_MAKER"
	OrderTypeStopLoss        OrderType = "STOP_LOSS"
	OrderTypeStopLossLimit   OrderType = "STOP_LOSS_LIMIT"
	OrderTypeTakeProfit      OrderType = "TAKE_PROFIT"
	OrderTypeTakeProfitLimit OrderType = "TAKE_PROFIT_LIMIT"

	TimeInForceTypeGTC TimeInForceType = "GTC"
	TimeInForceTypeIOC TimeInForceType = "IOC"
	TimeInForceTypeFOK TimeInForceType = "FOK"

	NewOrderRespTypeACK    NewOrderRespType = "ACK"
	NewOrderRespTypeRESULT NewOrderRespType = "RESULT"
	NewOrderRespTypeFULL   NewOrderRespType = "FULL"

	OrderStatusTypeNew             OrderStatusType = "NEW"
	OrderStatusTypePartiallyFilled OrderStatusType = "PARTIALLY_FILLED"
	OrderStatusTypeFilled          OrderStatusType = "FILLED"
	OrderStatusTypeCanceled        OrderStatusType = "CANCELED"
	OrderStatusTypePendingCancel   OrderStatusType = "PENDING_CANCEL"
	OrderStatusTypeRejected        OrderStatusType = "REJECTED"
	OrderStatusTypeExpired         OrderStatusType = "EXPIRED"

	SymbolTypeSpot SymbolType = "SPOT"

	SymbolStatusTypePreTrading   SymbolStatusType = "PRE_TRADING"
	SymbolStatusTypeTrading      SymbolStatusType = "TRADING"
	SymbolStatusTypePostTrading  SymbolStatusType = "POST_TRADING"
	SymbolStatusTypeEndOfDay     SymbolStatusType = "END_OF_DAY"
	SymbolStatusTypeHalt         SymbolStatusType = "HALT"
	SymbolStatusTypeAuctionMatch SymbolStatusType = "AUCTION_MATCH"
	SymbolStatusTypeBreak        SymbolStatusType = "BREAK"

	SymbolFilterTypeLotSize          SymbolFilterType = "LOT_SIZE"
	SymbolFilterTypePriceFilter      SymbolFilterType = "PRICE_FILTER"
	SymbolFilterTypePercentPrice     SymbolFilterType = "PERCENT_PRICE"
	SymbolFilterTypeMinNotional      SymbolFilterType = "MIN_NOTIONAL"
	SymbolFilterTypeIcebergParts     SymbolFilterType = "ICEBERG_PARTS"
	SymbolFilterTypeMarketLotSize    SymbolFilterType = "MARKET_LOT_SIZE"
	SymbolFilterTypeMaxNumAlgoOrders SymbolFilterType = "MAX_NUM_ALGO_ORDERS"

	MarginTransferTypeToMargin MarginTransferType = 1
	MarginTransferTypeToMain   MarginTransferType = 2

	FuturesTransferTypeToFutures FuturesTransferType = 1
	FuturesTransferTypeToMain    FuturesTransferType = 2

	MarginLoanStatusTypePending   MarginLoanStatusType = "PENDING"
	MarginLoanStatusTypeConfirmed MarginLoanStatusType = "CONFIRMED"
	MarginLoanStatusTypeFailed    MarginLoanStatusType = "FAILED"

	MarginRepayStatusTypePending   MarginRepayStatusType = "PENDING"
	MarginRepayStatusTypeConfirmed MarginRepayStatusType = "CONFIRMED"
	MarginRepayStatusTypeFailed    MarginRepayStatusType = "FAILED"

	FuturesTransferStatusTypePending   FuturesTransferStatusType = "PENDING"
	FuturesTransferStatusTypeConfirmed FuturesTransferStatusType = "CONFIRMED"
	FuturesTransferStatusTypeFailed    FuturesTransferStatusType = "FAILED"

	SideEffectTypeNoSideEffect SideEffectType = "NO_SIDE_EFFECT"
	SideEffectTypeMarginBuy    SideEffectType = "MARGIN_BUY"
	SideEffectTypeAutoRepay    SideEffectType = "AUTO_REPAY"

	timestampKey  = "timestamp"
	signatureKey  = "signature"
	recvWindowKey = "recvWindow"
)

func currentTimestamp() int64 {
	return int64(time.Nanosecond) * time.Now().UnixNano() / int64(time.Millisecond)
}

func newJSON(data []byte) (j *simplejson.Json, err error) {
	j, err = simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// NewClient initialize an API client instance with API key and secret key.
// You should always call this function before using this SDK.
// Services will be created by the form client.NewXXXService().
// Call Close() after you are done with the Client.
func NewClient(apiKey, secretKey string) *Client {
	c := &Client{
		APIKey:     apiKey,
		SecretKey:  secretKey,
		BaseURL:    "https://api.binance.com",
		UserAgent:  "Binance/golang",
		HTTPClient: http.DefaultClient,
		Logger:     log.New(os.Stderr, "Binance-golang ", log.LstdFlags),
		doneC:      make(chan struct{}),
		getLeaseC:  make(chan leaseRequest),
		backOffC:   make(chan int),
	}
	go c.runWeightLeaser()
	return c
}

// NewFuturesClient initialize client for futures API
func NewFuturesClient(apiKey, secretKey string) *futures.Client {
	return futures.NewClient(apiKey, secretKey)
}

type doFunc func(req *http.Request) (*http.Response, error)

// Client define API client
type Client struct {
	APIKey     string
	SecretKey  string
	BaseURL    string
	UserAgent  string
	HTTPClient *http.Client
	Debug      bool
	Logger     *log.Logger
	do         doFunc

	doneC     chan struct{}
	getLeaseC chan leaseRequest
	backOffC  chan int
}

func (c *Client) debug(format string, v ...interface{}) {
	if c.Debug {
		c.Logger.Printf(format, v...)
	}
}

// Close stops the background goroutine which keeps the client from exceeding the API limits.
func (c *Client) Close() {
	c.doneC <- struct{}{}
}

type leaseRequest struct {
	weight int
	respC  chan int
}

func (c *Client) runWeightLeaser() {
	const maxWeightPerMinute = 1000 // It's actually 1200 but I'm being cautious.

	var currentMinute time.Time
	var currentWeight int
	var waitingRequests []leaseRequest
	var durationUntilReset time.Duration

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	drainRequests := func() {
		var stillWaiting []leaseRequest
		drained := 0
		for _, req := range waitingRequests {
			if currentWeight+req.weight > maxWeightPerMinute {
				stillWaiting = append(stillWaiting, req)
				continue
			}
			drained++
			currentWeight += req.weight
			req.respC <- currentWeight
		}
		if drained > 0 || len(stillWaiting) > 0 {
			c.debug("runWeightLeaser: drainRequests drained %d; %d still remain", drained, len(stillWaiting))
		}
		waitingRequests = stillWaiting
	}

	maybeAdvanceTime := func() {
		now := time.Now()
		nowMinute := now.Truncate(time.Minute)
		durationUntilReset = nowMinute.Add(time.Minute).Sub(now)
		if currentMinute.Equal(nowMinute) {
			return
		}
		c.debug("runWeightLeaser: maybeAdvanceTime came upon a new minute (%v) with %d waiting requests", nowMinute, len(waitingRequests))
		currentMinute = nowMinute
		currentWeight = 0
		if len(waitingRequests) > 0 {
			drainRequests()
		}
	}

	for {
		select {
		case <-c.doneC:
			c.debug("runWeightLeaser exiting: doneC message")
			return

		case <-tick.C:
			maybeAdvanceTime()

		case req, ok := <-c.getLeaseC:
			if !ok {
				c.debug("runWeightLeaser exiting: getLeaseC is closed")
				return
			}

			maybeAdvanceTime()
			if currentWeight+req.weight > maxWeightPerMinute {
				// Can't handle the request now - it must wait.
				if len(waitingRequests) == 0 {
					c.Logger.Printf("You have exceeded the IP limits of the API. Waiting %v for more quota", durationUntilReset)
					c.debug("runWeightLeaser: starting to queue requests")
				}
				waitingRequests = append(waitingRequests, req)
				continue
			}
			currentWeight += req.weight
			c.debug("runWeightLeaser: currentWeight: %d", currentWeight)
			req.respC <- currentWeight

		case wait, ok := <-c.backOffC:
			if !ok {
				c.debug("runWeightLeaser exiting: backOffC is closed")
				return
			}

			c.Logger.Printf("You have exceeded the IP limits of the API. Waiting %d seconds before proceeding.", wait)
			time.Sleep(time.Duration(wait) * time.Second)

		}
	}
}

func (c *Client) waitForWeight(weight int) int {
	req := leaseRequest{
		weight: weight,
		respC:  make(chan int, 1),
	}
	c.getLeaseC <- req
	return <-req.respC
}

func (c *Client) CurrentWeight() int {
	return c.waitForWeight(0)
}

func (c *Client) parseRequest(r *request, opts ...RequestOption) (err error) {
	// set request options from user
	for _, opt := range opts {
		opt(r)
	}
	err = r.validate()
	if err != nil {
		return err
	}

	fullURL := fmt.Sprintf("%s%s", c.BaseURL, r.endpoint)
	if r.recvWindow > 0 {
		r.setParam(recvWindowKey, r.recvWindow)
	}
	if r.secType == secTypeSigned {
		r.setParam(timestampKey, currentTimestamp())
	}
	queryString := r.query.Encode()
	body := &bytes.Buffer{}
	bodyString := r.form.Encode()
	header := http.Header{}
	if bodyString != "" {
		header.Set("Content-Type", "application/x-www-form-urlencoded")
		body = bytes.NewBufferString(bodyString)
	}
	if r.secType == secTypeAPIKey || r.secType == secTypeSigned {
		header.Set("X-MBX-APIKEY", c.APIKey)
	}

	if r.secType == secTypeSigned {
		raw := fmt.Sprintf("%s%s", queryString, bodyString)
		mac := hmac.New(sha256.New, []byte(c.SecretKey))
		_, err = mac.Write([]byte(raw))
		if err != nil {
			return err
		}
		v := url.Values{}
		v.Set(signatureKey, fmt.Sprintf("%x", (mac.Sum(nil))))
		if queryString == "" {
			queryString = v.Encode()
		} else {
			queryString = fmt.Sprintf("%s&%s", queryString, v.Encode())
		}
	}
	if queryString != "" {
		fullURL = fmt.Sprintf("%s?%s", fullURL, queryString)
	}
	c.debug("full url: %s, body: %s", fullURL, bodyString)

	r.fullURL = fullURL
	r.header = header
	r.body = body
	return nil
}

func (c *Client) callAPI(ctx context.Context, r *request, opts ...RequestOption) (data []byte, err error) {
	if r.weight == 0 {
		r.weight = 1
	}
	c.waitForWeight(r.weight)

	err = c.parseRequest(r, opts...)
	if err != nil {
		return []byte{}, err
	}
	req, err := http.NewRequest(r.method, r.fullURL, r.body)
	if err != nil {
		return []byte{}, err
	}

	req = req.WithContext(ctx)
	req.Header = r.header
	c.debug("request: %#v", req)
	f := c.do
	if f == nil {
		f = c.HTTPClient.Do
	}
	res, err := f(req)
	if err != nil {
		return []byte{}, err
	}
	data, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return []byte{}, err
	}
	defer func() {
		cerr := res.Body.Close()
		// Only overwrite the retured error if the original error was nil and an
		// error occurred while closing the body.
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
	c.debug("response: %#v", res)
	c.debug("response body: %s", string(data))
	c.debug("response status code: %d", res.StatusCode)

	const mbxPrefix = "X-MBX-USED-WEIGHT-"
	for key, values := range res.Header {
		ukey := strings.ToUpper(key)
		if !strings.HasPrefix(ukey, mbxPrefix) {
			continue
		}
		interval := strings.TrimPrefix(ukey, mbxPrefix)
		c.debug("weight %s for interval %s", values[0], interval)
	}

	if res.StatusCode >= 400 {
		if res.StatusCode == 429 {
			// IP Limit exceeded; need to back off.
			if retryAfter := res.Header.Get("Retry-After"); retryAfter != "" {
				i, err := strconv.Atoi(retryAfter)
				if err != nil {
					c.debug("retryAfter %d seconds", i)

				}
			}
		} else if res.StatusCode == 418 {
			retryAfter := res.Header.Get("Retry-After")
			c.Logger.Printf("Ban response: %#v", res)
			panic(fmt.Sprintf("Your IP has been banned for %q", retryAfter))
		}

		apiErr := new(common.APIError)
		e := json.Unmarshal(data, apiErr)
		if e != nil {
			c.debug("failed to unmarshal json: %s", e)
		}
		return nil, apiErr
	}
	return data, nil
}

// NewPingService init ping service
func (c *Client) NewPingService() *PingService {
	return &PingService{c: c}
}

// NewServerTimeService init server time service
func (c *Client) NewServerTimeService() *ServerTimeService {
	return &ServerTimeService{c: c}
}

// NewDepthService init depth service
func (c *Client) NewDepthService() *DepthService {
	return &DepthService{c: c}
}

// NewAggTradesService init aggregate trades service
func (c *Client) NewAggTradesService() *AggTradesService {
	return &AggTradesService{c: c}
}

// NewRecentTradesService init recent trades service
func (c *Client) NewRecentTradesService() *RecentTradesService {
	return &RecentTradesService{c: c}
}

// NewKlinesService init klines service
func (c *Client) NewKlinesService() *KlinesService {
	return &KlinesService{c: c}
}

// NewListPriceChangeStatsService init list prices change stats service
func (c *Client) NewListPriceChangeStatsService() *ListPriceChangeStatsService {
	return &ListPriceChangeStatsService{c: c}
}

// NewListPricesService init listing prices service
func (c *Client) NewListPricesService() *ListPricesService {
	return &ListPricesService{c: c}
}

// NewListBookTickersService init listing booking tickers service
func (c *Client) NewListBookTickersService() *ListBookTickersService {
	return &ListBookTickersService{c: c}
}

// NewCreateOrderService init creating order service
func (c *Client) NewCreateOrderService() *CreateOrderService {
	return &CreateOrderService{c: c}
}

// NewGetOrderService init get order service
func (c *Client) NewGetOrderService() *GetOrderService {
	return &GetOrderService{c: c}
}

// NewCancelOrderService init cancel order service
func (c *Client) NewCancelOrderService() *CancelOrderService {
	return &CancelOrderService{c: c}
}

// NewListOpenOrdersService init list open orders service
func (c *Client) NewListOpenOrdersService() *ListOpenOrdersService {
	return &ListOpenOrdersService{c: c}
}

// NewListOrdersService init listing orders service
func (c *Client) NewListOrdersService() *ListOrdersService {
	return &ListOrdersService{c: c}
}

// NewGetAccountService init getting account service
func (c *Client) NewGetAccountService() *GetAccountService {
	return &GetAccountService{c: c}
}

// NewListTradesService init listing trades service
func (c *Client) NewListTradesService() *ListTradesService {
	return &ListTradesService{c: c}
}

// NewHistoricalTradesService init listing trades service
func (c *Client) NewHistoricalTradesService() *HistoricalTradesService {
	return &HistoricalTradesService{c: c}
}

// NewListDepositsService init listing deposits service
func (c *Client) NewListDepositsService() *ListDepositsService {
	return &ListDepositsService{c: c}
}

// NewCreateWithdrawService init creating withdraw service
func (c *Client) NewCreateWithdrawService() *CreateWithdrawService {
	return &CreateWithdrawService{c: c}
}

// NewListWithdrawsService init listing withdraw service
func (c *Client) NewListWithdrawsService() *ListWithdrawsService {
	return &ListWithdrawsService{c: c}
}

// NewStartUserStreamService init starting user stream service
func (c *Client) NewStartUserStreamService() *StartUserStreamService {
	return &StartUserStreamService{c: c}
}

// NewKeepaliveUserStreamService init keep alive user stream service
func (c *Client) NewKeepaliveUserStreamService() *KeepaliveUserStreamService {
	return &KeepaliveUserStreamService{c: c}
}

// NewCloseUserStreamService init closing user stream service
func (c *Client) NewCloseUserStreamService() *CloseUserStreamService {
	return &CloseUserStreamService{c: c}
}

// NewExchangeInfoService init exchange info service
func (c *Client) NewExchangeInfoService() *ExchangeInfoService {
	return &ExchangeInfoService{c: c}
}

// NewGetWithdrawFeeService init get withdraw fee service
func (c *Client) NewGetWithdrawFeeService() *GetWithdrawFeeService {
	return &GetWithdrawFeeService{c: c}
}

// NewAveragePriceService init average price service
func (c *Client) NewAveragePriceService() *AveragePriceService {
	return &AveragePriceService{c: c}
}

// NewMarginTransferService init margin account transfer service
func (c *Client) NewMarginTransferService() *MarginTransferService {
	return &MarginTransferService{c: c}
}

// NewMarginLoanService init margin account loan service
func (c *Client) NewMarginLoanService() *MarginLoanService {
	return &MarginLoanService{c: c}
}

// NewMarginRepayService init margin account repay service
func (c *Client) NewMarginRepayService() *MarginRepayService {
	return &MarginRepayService{c: c}
}

// NewCreateMarginOrderService init creating margin order service
func (c *Client) NewCreateMarginOrderService() *CreateMarginOrderService {
	return &CreateMarginOrderService{c: c}
}

// NewCancelMarginOrderService init cancel order service
func (c *Client) NewCancelMarginOrderService() *CancelMarginOrderService {
	return &CancelMarginOrderService{c: c}
}

// NewGetMarginOrderService init get order service
func (c *Client) NewGetMarginOrderService() *GetMarginOrderService {
	return &GetMarginOrderService{c: c}
}

// NewListMarginLoansService init list margin loan service
func (c *Client) NewListMarginLoansService() *ListMarginLoansService {
	return &ListMarginLoansService{c: c}
}

// NewListMarginRepaysService init list margin repay service
func (c *Client) NewListMarginRepaysService() *ListMarginRepaysService {
	return &ListMarginRepaysService{c: c}
}

// NewGetMarginAccountService init get margin account service
func (c *Client) NewGetMarginAccountService() *GetMarginAccountService {
	return &GetMarginAccountService{c: c}
}

// NewGetMarginAssetService init get margin asset service
func (c *Client) NewGetMarginAssetService() *GetMarginAssetService {
	return &GetMarginAssetService{c: c}
}

// NewGetMarginPairService init get margin pair service
func (c *Client) NewGetMarginPairService() *GetMarginPairService {
	return &GetMarginPairService{c: c}
}

// NewGetMarginAllPairsService init get margin all pairs service
func (c *Client) NewGetMarginAllPairsService() *GetMarginAllPairsService {
	return &GetMarginAllPairsService{c: c}
}

// NewGetMarginPriceIndexService init get margin price index service
func (c *Client) NewGetMarginPriceIndexService() *GetMarginPriceIndexService {
	return &GetMarginPriceIndexService{c: c}
}

// NewListMarginOpenOrdersService init list margin open orders service
func (c *Client) NewListMarginOpenOrdersService() *ListMarginOpenOrdersService {
	return &ListMarginOpenOrdersService{c: c}
}

// NewListMarginOrdersService init list margin all orders service
func (c *Client) NewListMarginOrdersService() *ListMarginOrdersService {
	return &ListMarginOrdersService{c: c}
}

// NewListMarginTradesService init list margin trades service
func (c *Client) NewListMarginTradesService() *ListMarginTradesService {
	return &ListMarginTradesService{c: c}
}

// NewGetMaxBorrowableService init get max borrowable service
func (c *Client) NewGetMaxBorrowableService() *GetMaxBorrowableService {
	return &GetMaxBorrowableService{c: c}
}

// NewGetMaxTransferableService init get max transferable service
func (c *Client) NewGetMaxTransferableService() *GetMaxTransferableService {
	return &GetMaxTransferableService{c: c}
}

// NewStartMarginUserStreamService init starting margin user stream service
func (c *Client) NewStartMarginUserStreamService() *StartMarginUserStreamService {
	return &StartMarginUserStreamService{c: c}
}

// NewKeepaliveMarginUserStreamService init keep alive margin user stream service
func (c *Client) NewKeepaliveMarginUserStreamService() *KeepaliveMarginUserStreamService {
	return &KeepaliveMarginUserStreamService{c: c}
}

// NewCloseMarginUserStreamService init closing margin user stream service
func (c *Client) NewCloseMarginUserStreamService() *CloseMarginUserStreamService {
	return &CloseMarginUserStreamService{c: c}
}

// NewFuturesTransferService init futures transfer service
func (c *Client) NewFuturesTransferService() *FuturesTransferService {
	return &FuturesTransferService{c: c}
}

// NewListFuturesTransferService init list futures transfer service
func (c *Client) NewListFuturesTransferService() *ListFuturesTransferService {
	return &ListFuturesTransferService{c: c}
}
