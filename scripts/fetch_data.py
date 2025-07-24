# scripts/fetch_data.py
import asyncio
import websockets
import json
import pandas as pd
import os
from datetime import datetime  # ✅ Added missing import

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
                
                df = pd.DataFrame([{
                    "timestamp": int(data.get("E", 0)),  # Event time
                    "price": float(data.get("c", 0)),    # Close price
                    "volume": float(data.get("v", 0)),   # Volume
                    "bid_price": float(data.get("b", 0)),
                    "ask_price": float(data.get("a", 0)),
                    "open_price": float(data.get("o", 0)),
                    "high_price": float(data.get("h", 0)),
                    "low_price": float(data.get("l", 0))
                }])
                
                if os.path.exists(filename):
                    old_df = pd.read_parquet(filename)
                    df = pd.concat([old_df, df], ignore_index=True)
                
                df.to_parquet(filename, index=False)
                print(f"📊 Saved tick: {data['c']} @ {datetime.now()}")

            except Exception as e:
                print(f"🔴 Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(fetch_binance_data())  # ✅ Fixed function name