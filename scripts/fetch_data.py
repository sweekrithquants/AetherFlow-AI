# scripts/fetch_data.py
import asyncio
import websockets
import json
import pandas as pd
import os

RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@ticker/btcusdt@depth20@100ms"

async def fetch_binance_data():
    async with websockets.connect(WS_URL) as ws:
        print("✅ Connected to Binance WebSocket")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)
                
                # Save raw tick data
                filename = f"{RAW_DATA_DIR}/btcusdt_tick_{datetime.now().strftime('%Y%m%d')}.parquet"
                df = pd.DataFrame([{...}])  # See earlier example
                df.to_parquet(filename, index=False)
            except Exception as e:
                print(f"🔴 Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(fetch_data())