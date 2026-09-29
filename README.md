# InferGate

InferGate 是面向 vLLM 与 OpenAI 兼容模型服务的推理网关。Go 数据面负责鉴权、租户限流、并发控制、模型路由、熔断、SSE 流式转发、用量记录和 Prometheus 指标；Python 控制面只读取指标并生成观察性优化建议，不参与请求转发。

## 目录

- `services/gateway`：Go 网关与 OpenAI/Anthropic 适配器。
- `services/optimizer`：只读优化控制面。
- `tools/perf-lab`：可复现的离线性能比较工具。
- `config`：网关、模型和优化策略示例配置。
- `contracts`：优化策略与实验清单的 JSON Schema。
- `deploy`：systemd 与 Prometheus 部署示例。

## 快速开始

```bash
cp config/keys.json.example config/keys.json
cp config/optimizer-policy.example.json config/optimizer-policy.json
python3 scripts/create_key.py local
head -c 32 /dev/urandom > config/cache-salt.key
mkdir -p run
make build
./bin/infergate -conf config/gateway.yaml -models config/models.yaml
```

将 `create_key.py` 输出的条目写入 `config/keys.json`，并按实际模型服务修改 `config/models.yaml`。默认监听 `127.0.0.1:8080`，指标监听 `127.0.0.1:6060`。

业务接口包括 `/v1/models`、`/v1/chat/completions` 和 `/v1/embeddings`；健康检查为 `/healthz` 与 `/readyz`。

## 许可证

本项目采用 MIT License，版权归 `ssssvbdd` 所有。完整条款请参阅 [LICENSE](LICENSE)。
