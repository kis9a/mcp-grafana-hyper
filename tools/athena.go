package tools

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/DataDog/zstd"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	mcpgrafana "github.com/grafana/mcp-grafana"
	"github.com/mark3labs/mcp-go/server"
)

type athenaQueryPayload struct {
	Queries []athenaInnerQuery `json:"queries"`
	From    string             `json:"from"`
	To      string             `json:"to"`
}

type athenaInnerQuery struct {
	RefID      string            `json:"refId"`
	Datasource map[string]string `json:"datasource"`
	RawSQL     string            `json:"rawSql"`
	RawQuery   bool              `json:"rawQuery"`
}

type athenaQueryResponse struct {
	Results map[string]struct {
		Error       string `json:"error,omitempty"`
		ErrorSource string `json:"errorSource,omitempty"`
		Status      int    `json:"status,omitempty"`
		Frames      []struct {
			Schema any             `json:"schema"`
			Data   json.RawMessage `json:"data"`
		} `json:"frames,omitempty"`
	} `json:"results"`
}

type athenaClient struct {
	baseURL    string
	httpClient *http.Client
	uid        string
}

func newAthenaClient(ctx context.Context, uid string) (*athenaClient, error) {
	if _, err := getDatasourceByUID(ctx, GetDatasourceByUIDParams{UID: uid}); err != nil {
		return nil, err
	}

	cfg := mcpgrafana.GrafanaConfigFromContext(ctx)
	grafanaURL := strings.TrimRight(cfg.URL, "/")
	base := fmt.Sprintf("%s/api/ds/query?ds_type=athena", grafanaURL)

	return &athenaClient{
		baseURL: base,
		uid:     uid,
		httpClient: &http.Client{
			Transport: &authRoundTripper{
				accessToken: cfg.AccessToken,
				idToken:     cfg.IDToken,
				apiKey:      cfg.APIKey,
				underlying:  http.DefaultTransport,
			},
		},
	}, nil
}

func (c *athenaClient) query(ctx context.Context, sql string) ([]map[string]any, error) {
	now := time.Now().UnixMilli()
	hrAgo := now - 60*60*1000

	payload := athenaQueryPayload{
		From: fmt.Sprintf("%d", hrAgo),
		To:   fmt.Sprintf("%d", now),
		Queries: []athenaInnerQuery{{
			RefID: "A",
			Datasource: map[string]string{
				"type": "athena",
				"uid":  c.uid,
			},
			RawSQL:   sql,
			RawQuery: true,
		}},
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Grafana /api/ds/query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		var dj athenaQueryResponse
		if err := json.Unmarshal(raw, &dj); err == nil {
			if ref, ok := dj.Results["A"]; ok && ref.Error != "" {
				return []map[string]any{{
					"error":        ref.Error,
					"error_source": ref.ErrorSource,
					"status":       ref.Status,
				}}, nil
			}
		}
		return []map[string]any{{"error": strings.TrimSpace(string(raw)), "status": resp.StatusCode}}, nil
	}

	var parsed athenaQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode response JSON: %w", err)
	}

	ref, ok := parsed.Results["A"]
	if !ok {
		return nil, fmt.Errorf("no result for refId A")
	}
	if ref.Error != "" {
		return []map[string]any{{"error": ref.Error, "error_source": ref.ErrorSource, "status": ref.Status}}, nil
	}
	if len(ref.Frames) == 0 {
		return []map[string]any{}, nil
	}

	var dataStr string
	if err := json.Unmarshal(ref.Frames[0].Data, &dataStr); err == nil {
		dec, err := base64.StdEncoding.DecodeString(dataStr)
		if err != nil {
			return nil, fmt.Errorf("base64 decode frame: %w", err)
		}
		arrowBytes, err := zstd.Decompress(nil, dec)
		if err != nil {
			return nil, fmt.Errorf("zstd decompress: %w", err)
		}
		frames, err := data.UnmarshalArrowFrames([][]byte{arrowBytes})
		if err != nil {
			return nil, fmt.Errorf("unmarshal arrow frame: %w", err)
		}
		if len(frames) == 0 {
			return []map[string]any{}, nil
		}
		frame := frames[0]
		rows := make([]map[string]any, frame.Rows())
		for r := 0; r < frame.Rows(); r++ {
			row := map[string]any{}
			for _, f := range frame.Fields {
				row[f.Name] = f.At(r)
			}
			rows[r] = row
		}
		return rows, nil
	}

	var obj struct {
		Values [][]any `json:"values"`
	}
	if err := json.Unmarshal(ref.Frames[0].Data, &obj); err != nil {
		return nil, fmt.Errorf("unknown data format: %w", err)
	}
	return valuesMatrixToJSON(obj.Values, ref.Frames[0].Schema), nil
}

type QueryAthenaSQLParams struct {
	DatasourceUID string `json:"datasourceUid" jsonschema:"required,description=Athena datasource UID"`
	SQL           string `json:"sql"           jsonschema:"required,description=SQL statement to execute"`
}

func queryAthenaSQL(ctx context.Context, args QueryAthenaSQLParams) ([]map[string]any, error) {
	client, err := newAthenaClient(ctx, args.DatasourceUID)
	if err != nil {
		return nil, err
	}
	return client.query(ctx, args.SQL)
}

var QueryAthenaSQL = mcpgrafana.MustTool(
	"query_athena_sql",
	"Athena datasource: Executes arbitrary SQL and returns the results as an array of JSON objects, one per row.",
	queryAthenaSQL,
)

func AddAthenaTools(mcp *server.MCPServer) {
	QueryAthenaSQL.Register(mcp)
}
