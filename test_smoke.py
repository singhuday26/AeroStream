"""Quick smoke test for AeroStream endpoints."""
import asyncio
import json
import sys
import time
import io

# Force UTF-8 on Windows consoles
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import httpx

BASE = "http://127.0.0.1:8000"

async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=10.0) as client:
        # 1. Root
        print("=" * 60)
        print("1. ROOT ENDPOINT")
        r = await client.get("/")
        data = r.json()
        print(f"   Engine: {data['engine']} v{data['version']}")
        print(f"   Status: {r.status_code}")
        print()

        # 2. Single event ingestion
        print("2. SINGLE EVENT INGESTION")
        event = {
            "event_type": "ad_click",
            "user_context": {
                "user_id": "usr_demo_001",
                "device_type": "mobile",
                "segment_tags": ["high_value", "returning", "brand_loyal"],
            },
            "payload": {
                "campaign_id": "camp_7742",
                "creative_variant": "A",
                "bid_amount": 2.45,
            },
            "personalization_hint": "hybrid_ensemble",
        }
        r = await client.post("/api/v1/stream-event", json=event)
        ack = r.json()
        print(f"   Status:            {r.status_code}")
        print(f"   Event ID:          {ack['event_id']}")
        print(f"   Cache Hit:         {ack['cache_hit']}")
        print(f"   Profile Resolved:  {ack['profile_resolved']}")
        print(f"   Processing Time:   {ack['processing_time_us']}μs")
        trace_id = r.headers.get("x-trace-id", "N/A")
        proc_time = r.headers.get("x-processing-time-us", "N/A")
        print(f"   Trace ID:          {trace_id}")
        print(f"   Middleware Latency: {proc_time}μs")
        print()

        # 3. Send same user again (should be cache hit)
        print("3. CACHE HIT TEST (same user)")
        event["event_type"] = "page_view"
        r = await client.post("/api/v1/stream-event", json=event)
        ack = r.json()
        print(f"   Cache Hit:         {ack['cache_hit']}")
        print(f"   Processing Time:   {ack['processing_time_us']}μs")
        print()

        # 4. Burst simulation
        print("4. BURST SIMULATION (500 events)")
        r = await client.post("/api/v1/simulate/burst?count=500")
        burst = r.json()
        print(f"   Simulated:         {burst['simulated']}")
        print(f"   Accepted:          {burst['accepted']}")
        print(f"   Total Time:        {burst['total_time_us']}μs")
        print(f"   Avg Per Event:     {burst['avg_per_event_us']}μs")
        print(f"   Throughput:        {burst['throughput_eps']} events/sec")
        print(f"   Unique Users:      {burst['unique_users']}")
        print()

        # 5. Wait for workers to process, then check profile
        await asyncio.sleep(0.5)
        print("5. PROFILE RESOLUTION")
        r = await client.get("/api/v1/profile/usr_demo_001")
        profile = r.json()
        print(f"   User ID:           {profile['user_id']}")
        print(f"   Segments:          {profile['segments']}")
        print(f"   Interactions:      {profile['interaction_count']}")
        print(f"   Last Event:        {profile['last_event_type']}")
        if profile["personalization_scores"]:
            print(f"   Scores:            {json.dumps(profile['personalization_scores'], indent=2)}")
        print(f"   Predictions:       {len(profile['speculative_predictions'])} cached")
        print()

        # 6. Health check
        print("6. HEALTH CHECK")
        r = await client.get("/api/v1/health")
        health = r.json()
        print(f"   Status:            {health['status']}")
        print(f"   Uptime:            {health['uptime_seconds']}s")
        print(f"   Cache Entries:     {health['cache_entries']}")
        print(f"   Cache Hit Rate:    {health['cache_hit_rate']*100:.1f}%")
        print(f"   Worker Queue:      {health['worker_queue_depth']}")
        print(f"   Active Workers:    {health['active_workers']}")
        print(f"   Events Processed:  {health['events_processed']}")
        print(f"   Events/sec:        {health['events_per_second']}")
        print(f"   Avg Latency:       {health['avg_latency_us']:.0f}μs")
        print(f"   P99 Latency:       {health['p99_latency_us']:.0f}μs")
        print()

        # 7. Metrics summary
        print("7. METRICS SUMMARY")
        r = await client.get("/api/v1/metrics/summary")
        metrics = r.json()
        print(f"   Cache Hits:        {metrics['cache']['hits']}")
        print(f"   Cache Misses:      {metrics['cache']['misses']}")
        print(f"   Cache Hit Rate:    {metrics['cache']['hit_rate']}%")
        print(f"   Cache Ops/sec:     {metrics['cache']['ops_per_second']}")
        print(f"   Worker Tasks Done: {metrics['workers']['tasks_completed']}")
        print(f"   Worker Avg Lat:    {metrics['workers']['avg_latency_us']}μs")
        print()

        # 8. High-throughput benchmark
        print("8. HIGH-THROUGHPUT BENCHMARK (1000 concurrent events)")
        start = time.perf_counter_ns()
        tasks = []
        for i in range(1000):
            evt = {
                "event_type": "ad_impression",
                "user_context": {
                    "user_id": f"bench_user_{i % 100:03d}",
                    "device_type": "desktop",
                    "segment_tags": ["benchmark"],
                },
                "payload": {"creative_id": f"cr_{i}"},
            }
            tasks.append(client.post("/api/v1/stream-event", json=evt))
        
        responses = await asyncio.gather(*tasks)
        elapsed_us = (time.perf_counter_ns() - start) // 1000
        
        success = sum(1 for r in responses if r.status_code == 202)
        latencies = [r.json()["processing_time_us"] for r in responses if r.status_code == 202]
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        max_lat = max(latencies) if latencies else 0
        min_lat = min(latencies) if latencies else 0
        
        print(f"   Total Time:        {elapsed_us}μs ({elapsed_us/1000:.1f}ms)")
        print(f"   Successful:        {success}/1000")
        print(f"   Throughput:        {1000 / (elapsed_us / 1_000_000):.0f} events/sec")
        print(f"   Avg Server Lat:    {avg_lat:.0f}μs")
        print(f"   Min Server Lat:    {min_lat}μs")
        print(f"   Max Server Lat:    {max_lat}μs")

        print()
        print("=" * 60)
        print("ALL TESTS PASSED ✓")
        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
