The codebase implements the algorithm presented in the ICDCN 2026 Conference titled "A Single-Order Mixed-Consistency Replication Scheme" (https://dl.acm.org/doi/10.1145/3772290.3772304).

## Brief Overview of the Paper
Creek is a Geo-replicated Key-Value store where one can achieve Linearizability for the Strong Operations and Fluctuating Eventual Consistency for the Weak Operations with the fault tolerance of $f < \frac{N}{2}$ nodes.

## Building & Running the System
1. Make sure `docker` and `docker-compose` are installed in the system.
2. Run the following commands
  ```bash
  docker compose build
  docker compose up -d
  ```

## Sending Requests to the Creek Nodes

The system is currently set up with 2 creek nodes for learning purposes. However, it can be generalized by modification in docker compose file for the N creek nodes architecture.

### To send a PUT request to Creek Node 1 with Weak Operation
  
  ```bash
  curl -X POST http://localhost:8001/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": false}'
  ```

### To send a PUT request to Creek Node 1 with Strong Operation

```bash
  curl -X POST http://localhost:8001/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": true}'
  ```


### To send a PUT request to Creek Node 2 with Weak Operation
  
  ```bash
  curl -X POST http://localhost:8002/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": false}'
  ```

### To send a PUT request to Creek Node 2 with Strong Operation

```bash
  curl -X POST http://localhost:8002/invoke \                                                                                
  -H "Content-Type: application/json" \
  -d '{"op": ["PUT", "key", "value"], "strong_op": true}'
  ```
