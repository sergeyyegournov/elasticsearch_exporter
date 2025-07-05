// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"testing"

	"bytes"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/promslog"
)

func TestShards(t *testing.T) {
	// Testcases created using:
	// docker run --rm -d -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:$VERSION
	// curl -XPUT http://localhost:9200/testindex
	// curl -XPUT http://localhost:9200/otherindex
	// curl http://localhost:9200/_cat/shards?format=json > fixtures/shards/$VERSION.json

	tests := []struct {
		name string
		file string
	}{
		{
			name: "7.15.0",
			file: "7.15.0.json",
		},
		{
			name: "assignment_status",
			file: "assignment_status.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(path.Join("../fixtures/shards/", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			// Read fixture data for DRY expected output
			var fixtureData []map[string]interface{}
			if err := json.NewDecoder(bytes.NewReader(readAll(f))).Decode(&fixtureData); err != nil {
				t.Fatal(err)
			}

			var wantBuf bytes.Buffer
			wantBuf.WriteString(`# TYPE elasticsearch_index_shard_assignment gauge
`)
			for _, shard := range fixtureData {
				index := shard["index"].(string)
				shardNum := shard["shard"].(string)
				node, _ := shard["node"].(string)
				assigned := "false"
				if node != "" && node != "null" {
					assigned = "true"
				}
				// Match encoder's label order: assigned, index, shard
				wantBuf.WriteString(
					fmt.Sprintf("elasticsearch_index_shard_assignment{assigned=\"%s\",index=\"%s\",shard=\"%s\"} %d\n",
						assigned, index, shardNum, map[bool]int{true: 1, false: 0}[assigned == "true"],
					),
				)
			}
			wantBuf.WriteString(`# TYPE elasticsearch_node_shards_total gauge
`)
			// For node_shards_total, only count STARTED shards and group by node
			nodeShards := make(map[string]int)
			for _, shard := range fixtureData {
				if state, ok := shard["state"].(string); ok && state == "STARTED" {
					node := shard["node"].(string)
					nodeShards[node]++
				}
			}
			for node, count := range nodeShards {
				wantBuf.WriteString(
					fmt.Sprintf("elasticsearch_node_shards_total{cluster=\"unknown_cluster\",node=\"%s\"} %d\n", node, count),
				)
			}
			wantBuf.WriteString(`# TYPE elasticsearch_node_shards_json_parse_failures counter
`)
			wantBuf.WriteString(`elasticsearch_node_shards_json_parse_failures 0
`)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				f.Seek(0, 0)
				io.Copy(w, f)
			}))
			defer ts.Close()

			u, err := url.Parse(ts.URL)
			if err != nil {
				t.Fatalf("Failed to parse URL: %s", err)
			}

			s := NewShards(promslog.NewNopLogger(), http.DefaultClient, u)
			if err != nil {
				t.Fatal(err)
			}

			// Collect metrics directly and encode to text format
			ch := make(chan prometheus.Metric)
			var actualBuf bytes.Buffer
			go func() {
				s.Collect(ch)
				close(ch)
			}()
			// Collect all metrics into a slice
			var metrics []prometheus.Metric
			for m := range ch {
				metrics = append(metrics, m)
			}
			// Group metrics by desc to form MetricFamily
			families := make(map[string]*io_prometheus_client.MetricFamily)
			// Helper to assign correct metric type
			metricType := func(name string) io_prometheus_client.MetricType {
				switch name {
				case "elasticsearch_index_shard_assignment":
					return io_prometheus_client.MetricType_GAUGE
				case "elasticsearch_node_shards_total":
					return io_prometheus_client.MetricType_GAUGE
				case "elasticsearch_node_shards_json_parse_failures":
					return io_prometheus_client.MetricType_COUNTER
				default:
					return io_prometheus_client.MetricType_UNTYPED
				}
			}
			for _, m := range metrics {
				pb := &io_prometheus_client.Metric{}
				if err := m.Write(pb); err != nil {
					t.Fatalf("failed to write metric: %v", err)
				}
				desc := m.Desc().String()
				name := metricNameFromDesc(desc)
				if families[desc] == nil {
					families[desc] = &io_prometheus_client.MetricFamily{
						Name: protoString(name),
						Type: metricType(name).Enum(),
					}
				}
				families[desc].Metric = append(families[desc].Metric, pb)
			}
			for _, mf := range families {
				if _, err := expfmt.MetricFamilyToText(&actualBuf, mf); err != nil {
					t.Fatalf("failed to encode metric family: %v", err)
				}
			}
			actual := actualBuf.String()
			expected := wantBuf.String()
			if !compareSortedLines(actual, expected) {
				t.Logf("Actual output:\n%s", actual)
				t.Logf("Expected output:\n%s", expected)
				t.Fatalf("metrics output does not match expected output (even after sorting)")
			}
		})
	}
}

// Helper to read all from file
func readAll(f *os.File) []byte {
	b, _ := io.ReadAll(f)
	return b
}

// Helper to compare sorted lines
func compareSortedLines(a, b string) bool {
	aLines := filterNonEmpty(strings.Split(a, "\n"))
	bLines := filterNonEmpty(strings.Split(b, "\n"))
	sort.Strings(aLines)
	sort.Strings(bLines)
	if len(aLines) != len(bLines) {
		return false
	}
	for i := range aLines {
		if aLines[i] != bLines[i] {
			return false
		}
	}
	return true
}

func filterNonEmpty(lines []string) []string {
	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// Helper to extract metric name from desc string
func metricNameFromDesc(desc string) string {
	// desc string format: Desc{fqName: "elasticsearch_index_shard_assignment", ...}
	start := strings.Index(desc, "fqName: \"")
	if start == -1 {
		return ""
	}
	start += len("fqName: \"")
	end := strings.Index(desc[start:], "\"")
	if end == -1 {
		return ""
	}
	return desc[start : start+end]
}

func protoString(s string) *string { return &s }
