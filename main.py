import os

os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

import asyncio
import json
import httpx
import websockets
import uvicorn
from typing import Dict, Set
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

try:
    import message_pb2
except ImportError:
    print("Помилка: Файл message_pb2.py не знайдено. Скомпілюйте його: ./protoc --python_out=. message.proto")

active_subscriptions: Dict[WebSocket, Set[str]] = {}


async def binance_connector():
    """Фонове завдання для отримання даних із зовнішньої системи Binance за протоколом WebSocket """
    uri = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    while True:
        try:
            async with websockets.connect(uri) as binance_ws:
                print("Підключено до Binance WebSocket API")
                while True:
                    data = await binance_ws.recv()
                    tickers = json.loads(data)

                    for ws, subscribed_symbols in active_subscriptions.items():
                        for ticker in tickers:
                            symbol = ticker['s']
                            if symbol in subscribed_symbols:
                                update = message_pb2.PriceUpdate(
                                    symbol=symbol,
                                    price=ticker['c'],
                                    timestamp=int(ticker['E'])
                                )
                                try:
                                    await ws.send_bytes(update.SerializeToString())
                                except Exception:
                                    pass
        except Exception as e:
            print(f"Помилка з'єднання з Binance: {e}. Повторна спроба за 5 секунд...")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(binance_connector())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="."), name="static")

CASDOOR_URL = "https://localhost"
CLIENT_ID = "871ada625da12022e6f2"
CLIENT_SECRET = "89a38ed3e9b321cbd3e4323ec2f3f539328695da"
REDIRECT_URI = "http://localhost:8080/callback"


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/login")
async def login():
    auth_url = (
        f"{CASDOOR_URL}/login/oauth/authorize?"
        f"client_id={CLIENT_ID}&"
        f"response_type=code&"
        f"redirect_uri={REDIRECT_URI}&"
        f"scope=read&"
        f"state=casdoor"
    )
    return RedirectResponse(auth_url)


@app.get("/callback")
async def callback(code: str):
    async with httpx.AsyncClient(verify=False) as client:
        token_res = await client.post(
            f"{CASDOOR_URL}/api/login/oauth/access_token",
            data={
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "code": code,
            }
        )
    if token_res.status_code != 200:
        raise HTTPException(status_code=400, detail="Помилка отримання токена")

    token_data = token_res.json()
    access_token = token_data.get("access_token")

    response = RedirectResponse(url="/")
    response.set_cookie(key="access_token", value=access_token)
    return response


@app.get("/user-info")
async def get_user_info(request: Request):
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Ви не авторизовані")
    async with httpx.AsyncClient(verify=False) as client:
        user_res = await client.get(
            f"{CASDOOR_URL}/api/userinfo",
            params={"access_token": token}
        )
    return user_res.json()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.cookies.get("access_token")
    if not token:
        print("Відхилено: Відсутній токен авторизації")
        await websocket.close(code=4001)
        return

    await websocket.accept()
    active_subscriptions[websocket] = set()
    print(f"Нова сесія: {websocket.client}")

    try:
        while True:
            binary_data = await websocket.receive_bytes()
            sub_request = message_pb2.SubscribeRequest()
            sub_request.ParseFromString(binary_data)

            for sym in sub_request.symbols:
                active_subscriptions[websocket].add(sym.upper())

            print(f"Сесія {websocket.client} підписалась на: {sub_request.symbols}")

    except WebSocketDisconnect:
        if websocket in active_subscriptions:
            del active_subscriptions[websocket]
        print(f"Сесія завершена: {websocket.client}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)