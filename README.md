The codebase implements the algorithm presented in the ICDCN 2026 Conference titled "A Single-Order Mixed-Consistency Replication Scheme" (https://dl.acm.org/doi/10.1145/3772290.3772304).

## Running Creek Nodes
1. Make sure `docker` and `docker-compose` are installed in the system.
2. Run the following commands
  ```bash
  docker compose build
  docker compose up -d
  ```

## Sending Requests to the Creek Nodes

### To send PUT request to creek node 1 with Weak Operation
  
  ```bash
  curl -X POST http://localhost:8001/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": false}'
  ```

### To send PUT request to creek node 1 with Strong Operation

```bash
  curl -X POST http://localhost:8001/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": true}'
  ```
