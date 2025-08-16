#!/usr/bin/env python3
"""
WebSocket客户端测试脚本
用于测试Redis WebSocket服务器

使用方法:
python3 websocket_test_client.py

依赖:
pip install websockets asyncio
"""

import asyncio
import websockets
import json
import sys

async def websocket_client():
    # WebSocket连接配置
    host = "localhost"
    port = 8080
    token = "secret_token_123"  # 默认认证令牌
    
    uri = f"ws://{host}:{port}/ws?token={token}"
    
    print(f"正在连接到WebSocket服务器: {uri}")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket连接成功！")
            print("正在等待Redis消息...\n")
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    print(f"📨 收到消息:")
                    print(f"   频道: {data.get('channel', 'N/A')}")
                    print(f"   时间: {data.get('timestamp', 'N/A')}")
                    print(f"   数据: {data.get('payload', 'N/A')}")
                    print("-" * 50)
                except json.JSONDecodeError:
                    print(f"收到非JSON消息: {message}")
                    
    except websockets.exceptions.ConnectionClosed:
        print("❌ WebSocket连接已关闭")
    except websockets.exceptions.InvalidStatusCode as e:
        if e.status_code == 401:
            print("❌ 认证失败！请检查token是否正确")
        else:
            print(f"❌ 连接失败，状态码: {e.status_code}")
    except ConnectionRefusedError:
        print("❌ 连接被拒绝！请确保WebSocket服务器正在运行")
        print(f"   启动命令: cargo run --bin redis_listener_example")
    except Exception as e:
        print(f"❌ 连接错误: {e}")

if __name__ == "__main__":
    print("Redis WebSocket客户端测试")
    print("=" * 50)
    
    try:
        asyncio.run(websocket_client())
    except KeyboardInterrupt:
        print("\n👋 客户端已退出")
