package datascope

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	BaseURL = "https://selectapi.datascope.refinitiv.com/RestApi/v1"
)

type Client struct {
	Username string
	Password string
	Token    string
	Debug    bool

	http.Client
}

type Credentials struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
}

type AuthRequest struct {
	Credentials Credentials `json:"Credentials"`
}

type AuthResponse struct {
	Token string `json:"value"`
}

type SearchRequest struct {
	Identifier              string   `json:"Identifier"`
	IdentifierType          string   `json:"IdentifierType"`
	PreferredIdentifierType string   `json:"PreferredIdentifierType,omitzero"`
	InstrumentTypeGroups    []string `json:"InstrumentTypeGroups,omitzero"`
}

type SearchRequestBody struct {
	SearchRequest SearchRequest `json:"SearchRequest"`
}

type SearchResponse struct {
	Value []map[string]any `json:"value"`
}

type Condition struct {
	ReportDateRangeType   string `json:"ReportDateRangeType"`
	IncludeDividendEvents bool   `json:"IncludeDividendEvents,omitzero"`
	QueryStartDate        string `json:"QueryStartDate,omitempty"`
	QueryEndDate          string `json:"QueryEndDate,omitempty"`
	RelativeStartDaysAgo  int    `json:"RelativeStartDaysAgo,omitzero"`
	RelativeEndDaysAgo    int    `json:"RelativeEndDaysAgo,omitzero"`
	PreviousDays          int    `json:"PreviousDays,omitzero"`
	NextDays              int    `json:"NextDays,omitzero"`
	DaysAgo               int    `json:"DaysAgo,omitzero"`
}

type ExtractionRequest struct {
	DataType          string         `json:"@odata.type"`
	ContentFieldNames []string       `json:"ContentFieldNames"`
	IdentifierList    IdentifierList `json:"IdentifierList"`
	Condition         Condition      `json:"Condition,omitzero"`
}

type ExtractionRequestBody struct {
	ExtractionRequest ExtractionRequest `json:"ExtractionRequest"`
}

type InstrumentIdentifier struct {
	IdentifierType        string `json:"IdentifierType"`
	Identifier            string `json:"Identifier"`
	Source                string `json:"Source,omitempty"`
	UserDefinedIdentifier string `json:"UserDefinedIdentifier,omitempty"`
}

type IdentifierList struct {
	DataType              string                 `json:"@odata.type"`
	InstrumentIdentifiers []InstrumentIdentifier `json:"InstrumentIdentifiers"`
}

type EntityIdentifierList struct {
	DataType          string                 `json:"@odata.type"`
	EntityIdentifiers []InstrumentIdentifier `json:"EntityIdentifiers"`
}

type ExtractionResponse struct {
	Contents []map[string]any `json:"Contents"`
	Notes    []string         `json:"Notes"`
}

func NewClient(username, password string) *Client {
	return &Client{
		Username: username,
		Password: password,
	}
}

func tokenPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "datascope", "token")
}

func LoadToken() string {
	if token, err := os.ReadFile(tokenPath()); err == nil {
		return string(token)
	}
	return ""
}

func SaveToken(token string) error {
	path := tokenPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(token), 0o600)
}

func (c *Client) Login() error {
	authReq := AuthRequest{
		Credentials: Credentials{
			Username: c.Username,
			Password: c.Password,
		},
	}

	body, err := json.Marshal(authReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", BaseURL+"/Authentication/RequestToken", bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	if c.Debug {
		b, _ := httputil.DumpRequestOut(req, true)
		fmt.Println(string(b))
	}

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var authResp AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return err
	}

	err = SaveToken(authResp.Token)
	if err != nil {
		return err
	}

	c.Token = authResp.Token

	return nil
}

func (c *Client) DoRequest(method, path string, body any) ([]byte, error) {
	tryReqeust := func(method, path string, body any) ([]byte, error) {
		c.Token = LoadToken()
		if c.Token == "" {
			if err := c.Login(); err != nil {
				return nil, err
			}
		}

		url, _ := url.JoinPath(BaseURL, []string{path}...)

		b := bytes.Buffer{}
		err := json.NewEncoder(&b).Encode(body)
		if err != nil {
			return nil, err
		}

		req, err := http.NewRequest(method, url, &b)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Authorization", "Token "+c.Token)
		req.Header.Set("Content-Type", "application/json")

		if c.Debug {
			fmt.Println("token", c.Token)
			b, err := httputil.DumpRequestOut(req, true)
			if err != nil {
				return nil, err
			}
			fmt.Println(string(b))
		}

		resp, err := c.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
		}
		return io.ReadAll(resp.Body)
	}

	resp, err := tryReqeust(method, path, body)
	if err != nil && strings.Contains(err.Error(), "invalid token") {
		if err := c.Login(); err != nil {
			return nil, err
		}
		return tryReqeust(method, path, body)
	}
	return resp, err
}

func builtinField(f string) bool {
	return f == "Identifier" || f == "IdentifierType"
}

func (c *Client) DoExtract(req ExtractionRequest) (*ExtractionResponse, error) {
	req.ContentFieldNames = slices.DeleteFunc(req.ContentFieldNames, builtinField)
	resp, err := c.DoRequest("POST", "/Extractions/ExtractWithNotes", ExtractionRequestBody{req})
	if err != nil {
		return nil, err
	}
	er := &ExtractionResponse{}
	if err := json.Unmarshal(resp, er); err != nil {
		return nil, fmt.Errorf("%w: %q", err, resp)
	}
	return er, nil
}

func identifierType(s string) string {
	switch strings.ToLower(s) {
	case "ric":
		return "Ric"
	case "chain":
		return "ChainRIC"
	case "isin":
		return "Isin"
	case "cusip":
		return "Cusip"
	default:
		return "Ric"
	}
}

func ParseIdentifier(s string) InstrumentIdentifier {
	iid := InstrumentIdentifier{
		IdentifierType: "Ric",
	}

	parts := strings.SplitN(s, ":", 3)
	if len(parts) > 1 {
		if len(parts[0]) > 0 {
			iid.IdentifierType = identifierType(parts[0])
		}
		iid.Identifier = parts[1]
		if len(parts) == 3 {
			iid.Source = parts[2]
		}
	} else {
		iid.Identifier = parts[0]
	}
	return iid
}

func ParseIdentifiers(identifiers []string) IdentifierList {
	identifierList := []InstrumentIdentifier{}
	for _, id := range identifiers {
		identifierList = append(identifierList, ParseIdentifier(id))
	}

	return IdentifierList{
		DataType:              "#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
		InstrumentIdentifiers: identifierList,
	}
}

func (c *Client) ExtractEndOfDay(identifiers []string, fields []string) (*ExtractionResponse, error) {
	ids := ParseIdentifiers(identifiers)
	req := ExtractionRequest{
		DataType:          "#DataScope.Select.Api.Extractions.ExtractionRequests.EndOfDayPricingExtractionRequest",
		IdentifierList:    ids,
		ContentFieldNames: fields,
	}

	return c.DoExtract(req)
}

func (c *Client) ExtractComposite(identifiers []string, fields []string) (*ExtractionResponse, error) {
	ids := ParseIdentifiers(identifiers)
	req := ExtractionRequest{
		DataType:          "#DataScope.Select.Api.Extractions.ExtractionRequests.CompositeExtractionRequest",
		IdentifierList:    ids,
		ContentFieldNames: fields,
	}

	return c.DoExtract(req)
}

func (c *Client) ExtractPriceHistory(identifiers []string, fields []string, daysAgo int, startDate, endDate string) (*ExtractionResponse, error) {
	ids := ParseIdentifiers(identifiers)
	condition := Condition{}
	if startDate != "" || endDate != "" {
		condition.ReportDateRangeType = "Range"
		condition.QueryStartDate = startDate
		condition.QueryEndDate = endDate
		if condition.QueryStartDate != "" && condition.QueryEndDate == "" {
			condition.QueryEndDate = time.Now().Format(time.DateOnly)
		}
	} else if daysAgo > 0 {
		condition.ReportDateRangeType = "Delta"
		condition.DaysAgo = daysAgo
	}
	req := ExtractionRequest{
		DataType:          "#DataScope.Select.Api.Extractions.ExtractionRequests.PriceHistoryExtractionRequest",
		IdentifierList:    ids,
		ContentFieldNames: fields,
		Condition:         condition,
	}

	return c.DoExtract(req)
}

func (c *Client) ExtractIntraday(identifiers []string, fields []string) (*ExtractionResponse, error) {
	ids := ParseIdentifiers(identifiers)
	req := ExtractionRequest{
		DataType:          "#DataScope.Select.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest",
		IdentifierList:    ids,
		ContentFieldNames: fields,
	}

	return c.DoExtract(req)

}

func (c *Client) ExtractCorporateActions(identifiers []string, fields []string) (*ExtractionResponse, error) {
	ids := ParseIdentifiers(identifiers)
	condition := Condition{
		ReportDateRangeType:   "Delta",
		IncludeDividendEvents: true,
		PreviousDays:          100,
	}
	req := ExtractionRequest{
		DataType:          "#DataScope.Select.Api.Extractions.ExtractionRequests.CorporateActionsStandardExtractionRequest",
		IdentifierList:    ids,
		ContentFieldNames: fields,
		Condition:         condition,
	}

	return c.DoExtract(req)
}

func (c *Client) Search(identifier string) (*SearchResponse, error) {
	id := ParseIdentifier(identifier)
	req := SearchRequestBody{
		SearchRequest: SearchRequest{
			Identifier:              id.Identifier,
			IdentifierType:          id.IdentifierType,
			PreferredIdentifierType: "Ric",
		},
	}

	resp, err := c.DoRequest("POST", "/Search/InstrumentSearch", req)
	if err != nil {
		return nil, err
	}

	sr := &SearchResponse{}
	if err := json.Unmarshal(resp, sr); err != nil {
		return nil, fmt.Errorf("unexpected response: %q %w", resp, err)
	}
	return sr, err
}
