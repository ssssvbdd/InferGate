package main

import (
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"

	"github.com/ssssvbdd/InferGate/services/gateway/internal/model"
)

// redisClient 是 registry/selector/limiter 所需的 *redis.Client 别名。
type redisClient = *redis.Client

// modelsFile 是 models.yaml 的结构。
type modelsFile struct {
	Models []model.ModelConfig `yaml:"models"`
}

// loadModels 从 YAML 文件加载模型配置列表。
func loadModels(path string) ([]model.ModelConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read models file: %w", err)
	}
	var mf modelsFile
	if err := yaml.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("unmarshal models: %w", err)
	}
	// 填充默认值。
	for i := range mf.Models {
		m := &mf.Models[i]
		if m.Version == "" {
			m.Version = "default"
		}
		if m.ModelType == "" {
			m.ModelType = model.ModelTypeLLM
		}
		if m.Discovery == "" {
			m.Discovery = model.DiscoveryStatic
		}
		if m.LoadBalance == "" {
			m.LoadBalance = model.LBWeighted
		}
	}
	return mf.Models, nil
}
