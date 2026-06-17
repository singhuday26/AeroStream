import asyncio
import json
import time
import sys
import httpx

async def send_request(client, semaphore, payload, stats):
    async with semaphore:
        start_time = time.perf_counter()
        try:
            # POST event to the ingestion endpoint
            response = await client.post("/api/v1/stream-event", json=payload)
            latency = time.perf_counter() - start_time
            stats['latencies'].append(latency)
            if response.status_code == 202 or response.status_code == 200:
                stats['success'] += 1
            else:
                stats['failures'] += 1
        except Exception:
            stats['errors'] += 1

async def main():
    # Read payload
    try:
        with open("test_payload.json", "r") as f:
            payload = json.load(f)
    except FileNotFoundError:
        # Create a default valid event payload if not exists
        payload = {
            "event_type": "ad_click",
            "user_context": {
                "user_id": "usr_benchmark_99",
                "device_type": "mobile",
                "segment_tags": ["high_value"]
            },
            "payload": {
                "campaign_id": "camp_99"
            }
        }
        with open("test_payload.json", "w") as f:
            json.dump(payload, f, indent=2)

    url = "http://127.0.0.1:8000"
    total_requests = 10000
    concurrency = 50

    print("=" * 60)
    print(f" AeroStream Python Stress Test ")
    print(f" Target:      {url}/api/v1/stream-event")
    print(f" Requests:    {total_requests}")
    print(f" Concurrency: {concurrency}")
    print("=" * 60)
    print("Benchmarking in progress...")

    stats = {
        'latencies': [],
        'success': 0,
        'failures': 0,
        'errors': 0
    }

    semaphore = asyncio.Semaphore(concurrency)
    # Configure client connection pool matching our concurrency
    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    
    start_time = time.perf_counter()
    async with httpx.AsyncClient(base_url=url, limits=limits, timeout=10.0) as client:
        tasks = [
            send_request(client, semaphore, payload, stats)
            for _ in range(total_requests)
        ]
        await asyncio.gather(*tasks)
    
    total_time = time.perf_counter() - start_time
    
    # Calculate statistics
    latencies = stats['latencies']
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    latencies.sort()
    
    p50 = latencies[int(len(latencies) * 0.5)] if latencies else 0
    p90 = latencies[int(len(latencies) * 0.9)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    
    print("\n" + "=" * 60)
    print(" BENCHMARK RESULTS")
    print("=" * 60)
    print(f" Total Time Taken:   {total_time:.3f} seconds")
    print(f" Successful Req:     {stats['success']}")
    print(f" Failed Req:         {stats['failures']}")
    print(f" Network Errors:     {stats['errors']}")
    print(f" Throughput:         {len(latencies) / total_time:.2f} req/sec")
    print(f" Latency Metrics:")
    print(f"   Average:          {avg_latency * 1000:.2f} ms")
    print(f"   50% (Median):     {p50 * 1000:.2f} ms")
    print(f"   90%:              {p90 * 1000:.2f} ms")
    print(f"   95%:              {p95 * 1000:.2f} ms")
    print(f"   99%:              {p99 * 1000:.2f} ms")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
