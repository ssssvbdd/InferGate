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

将 `create_key.py` 输出的条目写入 `config/keys.json`，并按实际模型服务修改 `config/models.yaml`。示例配置中的 InferGate 监听 `127.0.0.1:8082`，指标监听 `127.0.0.1:6062`，vLLM 上游为 `127.0.0.1:8001`；使用前核对当前服务器的实际端口。

业务接口包括 `/v1/models`、`/v1/chat/completions` 和 `/v1/embeddings`；健康检查为 `/healthz` 与 `/readyz`。

模型交付脚本位于 `scripts/model_delivery.py` 和 `scripts/accept_model.py`；
性能工具位于 `tools/perf-lab`，容量报告位于 `scripts/capacity_report.py`。
运行前将 `config/model-deliveries/qwen3-4b.json` 中的模型目录、服务地址
和资源参数改为实际环境配置。运行数据、实验报告和密钥保存在仓库之外。

## 双副本流量治理

入口配置见 `deploy/envoy/envoy.yaml`，双网关与双上游配置示例见
`config/gateway-ha.example.yaml`、`config/models-multi.example.yaml`。
两个网关进程应使用不同的监听地址、指标端口和本地用量日志，使用同一 Redis、
`quota_namespace` 和租户密钥。模型与实例的 `max_in_flight` 是每个网关副本
的本地保护；`auth.max_in_flight` 在 `auth.distributed: true` 时是跨副本
共享上限。SSE 使用独立的 `stream_idle_timeout`，不受普通请求总超时约束。

`make test-resilience` 检查路由与故障处理；
`make test-multireplica` 需要隔离的 Redis，用于检查共享配额与集中用量。
示例端口和 SLO 门槛应根据目标环境调整。

## 许可证

本项目采用 MIT License，版权归 `ssssvbdd` 所有。完整条款请参阅 [LICENSE](LICENSE)。
