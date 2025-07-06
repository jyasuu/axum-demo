# axum-demo


```bash
curl localhost:3000/poll?client_id=123 | jq
curl -X POST localhost:3000/push -H 'Content-Type: application/json' -d '{"client_id":"123","message":"hello"}'
```