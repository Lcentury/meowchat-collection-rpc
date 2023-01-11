package model

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/xh-polaris/meowchat-collection-rpc/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const CatCollectionName = "cat"
const CatIndexName = "meowchat_collection.cat-alias"

var _ CatModel = (*customCatModel)(nil)

type (
	// CatModel is an interface to be customized, add more methods here,
	// and implement the added methods in customCatModel.
	CatModel interface {
		catModel
		FindManyByCommunityId(ctx context.Context, communityId string, skip int64, count int64) ([]*Cat, int64, error)
		Search(ctx context.Context, communityId, keyword string, skip, count int64) ([]*Cat, error)
		SearchNumber(ctx context.Context, communityId, keyword string) (int64, error)
	}

	customCatModel struct {
		*defaultCatModel
		es *elasticsearch.Client
	}
)

// NewCatModel returns a model for the mongo.
func NewCatModel(url, db string, c cache.CacheConf, es config.ElasticsearchConf) CatModel {
	conn := monc.MustNewModel(url, db, CatCollectionName, c)
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Username:  es.Username,
		Password:  es.Password,
		Addresses: es.Addresses,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return &customCatModel{
		defaultCatModel: newDefaultCatModel(conn),
		es:              esClient,
	}
}

func (m *customCatModel) FindManyByCommunityId(ctx context.Context, communityId string, skip int64, count int64) ([]*Cat, int64, error) {
	data := make([]*Cat, 0, 20)
	err := m.conn.Find(ctx, &data, bson.M{"communityId": communityId}, &options.FindOptions{Skip: &skip, Limit: &count})
	if err != nil {
		return nil, -1, err
	}
	countx, err := m.conn.CountDocuments(ctx, bson.M{"communityId": communityId})
	if err != nil {
		return nil, -1, err
	}
	return data, countx, nil
}

func (m *customCatModel) Search(ctx context.Context, communityId, keyword string, skip, count int64) ([]*Cat, error) {
	search := m.es.Search
	query := map[string]any{
		"from": skip,
		"size": count,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"term": map[string]any{
							"communityId": communityId,
						},
					},
					map[string]any{
						"multi_match": map[string]any{
							"query":  keyword,
							"fields": []string{"details", "name", "area", "color"},
						},
					},
				},
				"filter": []any{
					map[string]any{
						"term": map[string]any{
							"isDeleted": false,
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}
	res, err := search(
		search.WithIndex(CatIndexName),
		search.WithContext(ctx),
		search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, err
		} else {
			logx.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}
	var r map[string]any
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}
	hits := r["hits"].(map[string]any)["hits"].([]any)
	cats := make([]*Cat, 0, 10)
	for i := range hits {
		hit := hits[i].(map[string]any)
		cat := &Cat{}
		source := hit["_source"].(map[string]any)
		if source["createAt"], err = time.Parse("2006-01-02T15:04:05Z07:00", source["createAt"].(string)); err != nil {
			return nil, err
		}
		if source["updateAt"], err = time.Parse("2006-01-02T15:04:05Z07:00", source["updateAt"].(string)); err != nil {
			return nil, err
		}
		hit["_source"] = source
		err := mapstructure.Decode(hit["_source"], cat)
		if err != nil {
			return nil, err
		}
		oid := hit["_id"].(string)
		id, err := primitive.ObjectIDFromHex(oid)
		if err != nil {
			return nil, err
		}
		cat.ID = id
		cats = append(cats, cat)
	}
	return cats, nil
}

func (m *customCatModel) SearchNumber(ctx context.Context, communityId, keyword string) (int64, error) {
	search := m.es.Count
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"term": map[string]any{
							"communityId": communityId,
						},
					},
					map[string]any{
						"multi_match": map[string]any{
							"query":  keyword,
							"fields": []string{"details", "name", "area", "color"},
						},
					},
				},
				"filter": []any{
					map[string]any{
						"term": map[string]any{
							"isDeleted": false,
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return -1, err
	}
	res, err := search(
		search.WithIndex(CatIndexName),
		search.WithContext(ctx),
		search.WithBody(&buf),
	)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return -1, err
		} else {
			logx.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}
	var r map[string]any
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return -1, err
	}
	counts := fmt.Sprint(r["count"])
	num, err := strconv.ParseInt(counts, 10, 64)
	if err != nil {
		return -1, err
	}
	return num, nil
}
