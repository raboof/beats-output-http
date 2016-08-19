package http

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/transport"
)

type Client struct {
	Connection
	params map[string]string

	// additional configs
	compressionLevel int
	proxyURL         *url.URL
}

type Connection struct {
	URL      string
	Username string
	Password string

	http      *http.Client
	connected bool

	encoder bodyEncoder
}

// Metrics that can retrieved through the expvar web interface.
var (
	ackedEvents            = expvar.NewInt("libbeatHttpPublishedAndAckedEvents")
	eventsNotAcked         = expvar.NewInt("libbeatHttpPublishedButNotAckedEvents")
	publishEventsCallCount = expvar.NewInt("libbeatHttpPublishEventsCallCount")

	statReadBytes   = expvar.NewInt("libbeatHttpPublishReadBytes")
	statWriteBytes  = expvar.NewInt("libbeatHttpPublishWriteBytes")
	statReadErrors  = expvar.NewInt("libbeatHttpPublishReadErrors")
	statWriteErrors = expvar.NewInt("libbeatHttpPublishWriteErrors")
)

func NewClient(
	hostURL string, proxyURL *url.URL, tls *tls.Config,
	username, password string,
	params map[string]string,
	timeout time.Duration,
	compression int,
) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if proxyURL != nil {
		proxy = http.ProxyURL(proxyURL)
	}

	logp.Info("Http url: %s", hostURL)

	dialer := transport.NetDialer(timeout)
	dialer = transport.StatsDialer(dialer, &transport.IOStats{
		Read:        statReadBytes,
		Write:       statWriteBytes,
		ReadErrors:  statReadErrors,
		WriteErrors: statWriteErrors,
	})

	var err error
	var encoder bodyEncoder
	if compression == 0 {
		encoder = newJSONEncoder(nil)
	} else {
		encoder, err = newGzipEncoder(compression, nil)
		if err != nil {
			return nil, err
		}
	}

	client := &Client{
		Connection: Connection{
			URL:      hostURL,
			Username: username,
			Password: password,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:            dialer.Dial,
					TLSClientConfig: tls,
					Proxy:           proxy,
				},
				Timeout: timeout,
			},
			encoder: encoder,
		},
		params: params,

		proxyURL: proxyURL,
	}

	return client, nil
}

func (client *Client) Clone() *Client {
	// when cloning the connection callback and params are not copied. A
	// client's close is for example generated for topology-map support. With params
	// most likely containing the ingest node pipeline and default callback trying to
	// create install a template, we don't want these to be included in the clone.

	transport := client.http.Transport.(*http.Transport)
	c, _ := NewClient(
		client.URL,
		client.proxyURL,
		transport.TLSClientConfig,
		client.Username,
		client.Password,
		nil, // XXX: do not pass params?
		client.http.Timeout,
		client.compressionLevel,
	)
	return c
}

func (conn *Connection) Connect(timeout time.Duration) error {
	conn.connected = true
	return nil
}

func (conn *Connection) IsConnected() bool {
	return conn.connected
}

func (conn *Connection) Close() error {
	conn.connected = false
	return nil
}

// PublishEvents posts all events to the http endpoint. On error a slice with all
// events not published will be returned.
func (client *Client) PublishEvents(
	data []outputs.Data,
) ([]outputs.Data, error) {
	begin := time.Now()
	publishEventsCallCount.Add(1)

	if len(data) == 0 {
		return nil, nil
	}

	if !client.connected {
		return data, ErrNotConnected
	}

	var failedEvents []outputs.Data

	sendErr := error(nil)
	for _, event := range data {
		sendErr = client.PublishEvent(event)
		// TODO more gracefully handle failures return the failed events
		// below instead of bailing out directly here:
		if sendErr != nil {
			return nil, sendErr
		}
	}

	debugf("PublishEvents: %d metrics have been published over HTTP in %v.",
		len(data),
		time.Now().Sub(begin))

	ackedEvents.Add(int64(len(data) - len(failedEvents)))
	eventsNotAcked.Add(int64(len(failedEvents)))
	if len(failedEvents) > 0 {
		return failedEvents, sendErr
	}

	return nil, nil
}

func (client *Client) PublishEvent(data outputs.Data) error {
	if !client.connected {
		return ErrNotConnected
	}

	event := data.Event

	debugf("Publish event: %s", event)

	status, _, err := client.request("POST", "", client.params, event)
	if err != nil {
		logp.Warn("Fail to insert a single event: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 0: // event was not send yet
		return nil
	case status >= 500 || status == 429: // server error, retry
		return err
	case status >= 300 && status < 500:
		// other error => don't retry
		return nil
	}

	return nil
}

func (conn *Connection) request(
	method, path string,
	params map[string]string,
	body interface{},
) (int, []byte, error) {
	url := makeURL(conn.URL, path, "", params)
	debugf("%s %s %v", method, url, body)

	if body == nil {
		return conn.execRequest(method, url, nil)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logp.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, url, conn.encoder.Reader())
}

func (conn *Connection) execRequest(
	method, url string,
	body io.Reader,
) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		logp.Warn("Failed to create request", err)
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header)
	}
	return conn.execHTTPRequest(req)
}

func (conn *Connection) execHTTPRequest(req *http.Request) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}

	resp, err := conn.http.Do(req)
	if err != nil {
		conn.connected = false
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	if status >= 300 {
		conn.connected = false
		return status, nil, fmt.Errorf("%v", resp.Status)
	}

	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		conn.connected = false
		return status, nil, err
	}
	return status, obj, nil
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
		logp.Warn("Close failed with: %v", err)
	}
}
