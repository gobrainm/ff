import asyncio
import os
import logging
from uuid import uuid4
from urllib.parse import urlencode
import hashlib
import base64
from io import BytesIO
import requests
import psycopg2.pool
import qrcode
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
from config import load_bot_configs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
logger.info("Starting subscription bot service")

# Constants
PAYMENT_ENDPOINT = "/store_payment"
YOOMONEY_ENDPOINT = "/yoomoney_notify"
HEALTH_ENDPOINT = "/health"
WEBHOOK_ENDPOINT = "/telegram_webhook"
DATABASE_URL = "postgresql://postgres.iylthyqzwovudjcyfubg:Alex4382!@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
BASE_URL = os.environ.get("BASE_URL", "https://short-blinnie-bakibakikun-a88f041b.koyeb.app")
ENVIRONMENT = "vercel"
logger.info(f"Running on platform: {ENVIRONMENT}")

# Initialize database connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DATABASE_URL)

# Currency conversion
def convert_rub_to_usd(amount_rub):
    try:
        # Placeholder for real API: response = requests.get("https://api.exchangerate-api.com/v4/latest/RUB")
        rate_usd = 100.0  # Static rate: 1 USD = 100 RUB
        return amount_rub / rate_usd
    except Exception as e:
        logger.error(f"USD conversion error: {e}")
        return amount_rub / 100.0

# Fetch crypto prices
def fetch_crypto_rates():
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network,bitcoin,tether&vs_currencies=usd"
        )
        data = response.json()
        return (
            data["the-open-network"]["usd"],
            data["bitcoin"]["usd"],
            data["tether"]["usd"]
        )
    except Exception as e:
        logger.error(f"Crypto price fetch error: {e}")
        return 5.0, 60000.0, 1.0  # Fallback values

# Generate QR code
def create_qr_image(data):
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        return base64.b64encode(buffer.getvalue()).decode()
    except Exception as e:
        logger.error(f"QR code generation error: {e}")
        return None

# Bot initialization
CONFIGS = load_bot_configs()
logger.info(f"Initializing {len(CONFIGS)} bot instances")
bots = {}
dispatchers = {}

for bot_id, config in CONFIGS.items():
    try:
        logger.info(f"Setting up bot {bot_id}")
        bots[bot_id] = Bot(token=config["TOKEN"])
        dispatchers[bot_id] = Dispatcher(bots[bot_id])
        logger.info(f"Bot {bot_id} ready")
    except Exception as e:
        logger.error(f"Bot {bot_id} initialization failed: {e}")
        raise SystemExit(1)

# Database setup
def initialize_database():
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        for bot_id in CONFIGS:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS payments_{bot_id} (
                    payment_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payment_method TEXT
                )
                """
            )
            cursor.execute(
                f"ALTER TABLE payments_{bot_id} ADD COLUMN IF NOT EXISTS payment_method TEXT"
            )
        conn.commit()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Database setup error: {e}")
        raise SystemExit(1)
    finally:
        cursor.close()
        db_pool.putconn(conn)

initialize_database()

# Payment options keyboard
def get_payment_options(user_id):
    keyboard = InlineKeyboardMarkup()
    buttons = [
        ("ЮMoney", f"yoomoney_{user_id}"),
        ("TON", f"ton_{user_id}"),
        ("BTC", f"btc_{user_id}"),
        ("USDT TRC20", f"usdt_{user_id}")
    ]
    for text, callback in buttons:
        keyboard.add(InlineKeyboardButton(text, callback_data=callback))
    return keyboard

# Bot command handlers
for bot_id, dp in dispatchers.items():
    @dp.message_handler(commands=["start"])
    async def start_command(msg: types.Message, bot_id=bot_id):
        try:
            user_id = str(msg.from_user.id)
            chat_id = msg.chat.id
            bot = bots[bot_id]
            config = CONFIGS[bot_id]
            logger.info(f"[{bot_id}] /start command from user {user_id}")

            keyboard = get_payment_options(user_id)
            message = config["DESCRIPTION"].format(price=config["PRICE"])
            await bot.send_message(
                chat_id,
                f"{message}\n\nSelect payment method for {config['PRICE']} RUB:",
                reply_markup=keyboard
            )
            logger.info(f"[{bot_id}] Payment options sent to user {user_id}")
        except Exception as e:
            logger.error(f"[{bot_id}] /start error: {e}")
            await bots[bot_id].send_message(chat_id, "An error occurred. Please try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("yoomoney_"))
    async def process_yoomoney(cb: types.CallbackQuery, bot_id=bot_id):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bots[bot_id]
            config = CONFIGS[bot_id]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_id}] YooMoney selected by user {user_id}")

            payment_id = str(uuid4())
            payment_params = {
                "quickpay-form": "shop",
                "paymentType": "AC",
                "targets": f"Subscription for user {user_id}",
                "sum": config["PRICE"],
                "label": payment_id,
                "receiver": config["YOOMONEY_WALLET"],
                "successURL": f"https://t.me/{(await bot.get_me()).username}"
            }
            payment_url = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(payment_params)}"

            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_id} (payment_id, user_id, status, payment_method) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "yoomoney")
            )
            conn.commit()
            logger.info(f"[{bot_id}] Payment {payment_id} stored for user {user_id}")

            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("Pay Now", url=payment_url))
            await bot.send_message(chat_id, "Proceed to YooMoney payment:", reply_markup=keyboard)
            logger.info(f"[{bot_id}] YooMoney link sent to user {user_id}")
        except Exception as e:
            logger.error(f"[{bot_id}] YooMoney error: {e}")
            await bots[bot_id].send_message(chat_id, "Payment error. Please try again.")
        finally:
            cursor.close()
            db_pool.putconn(conn)

    @dp.callback_query_handler(lambda c: c.data.startswith("ton_"))
    async def process_ton(cb: types.CallbackQuery, bot_id=bot_id):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bots[bot_id]
            config = CONFIGS[bot_id]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_id}] TON selected by user {user_id}")

            payment_id = str(uuid4())
            ton_rate, _, _ = fetch_crypto_rates()
            usd_amount = convert_rub_to_usd(config["PRICE"])
            ton_amount = round(usd_amount / ton_rate, 4)
            nano_ton = int(ton_amount * 1e9)

            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_id} (payment_id, user_id, status, payment_method) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "ton")
            )
            conn.commit()
            logger.info(f"[{bot_id}] TON payment {payment_id} stored for user {user_id}")

            qr_data = f"ton://transfer/{config['CRYPTO_ADDRESSES']['TON']}?amount={nano_ton}"
            qr_code = create_qr_image(qr_data)
            if qr_code:
                qr_bytes = base64.b64decode(qr_code)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=config["CRYPTO_ADDRESSES"]["TON"])
            else:
                await bot.send_message(chat_id, config["CRYPTO_ADDRESSES"]["TON"])

            await bot.send_message(chat_id, f"Send: {ton_amount:.4f} TON")
            logger.info(f"[{bot_id}] TON address and amount sent to user {user_id}")
        except Exception as e:
            logger.error(f"[{bot_id}] TON error: {e}")
            await bots[bot_id].send_message(chat_id, "Payment error. Please try again.")
        finally:
            cursor.close()
            db_pool.putconn(conn)

    @dp.callback_query_handler(lambda c: c.data.startswith("btc_"))
    async def process_btc(cb: types.CallbackQuery, bot_id=bot_id):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bots[bot_id]
            config = CONFIGS[bot_id]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_id}] BTC selected by user {user_id}")

            payment_id = str(uuid4())
            _, btc_rate, _ = fetch_crypto_rates()
            usd_amount = convert_rub_to_usd(config["PRICE"])
            btc_amount = f"{usd_amount / btc_rate:.8f}".rstrip("0")

            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_id} (payment_id, user_id, status, payment_method) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "btc")
            )
            conn.commit()
            logger.info(f"[{bot_id}] BTC payment {payment_id} stored for user {user_id}")

            qr_data = f"bitcoin:{config['CRYPTO_ADDRESSES']['BTC']}?amount={btc_amount}"
            qr_code = create_qr_image(qr_data)
            if qr_code:
                qr_bytes = base64.b64decode(qr_code)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=config["CRYPTO_ADDRESSES"]["BTC"])
            else:
                await bot.send_message(chat_id, config["CRYPTO_ADDRESSES"]["BTC"])

            await bot.send_message(chat_id, f"Send: {btc_amount} BTC")
            logger.info(f"[{bot_id}] BTC address and amount sent to user {user_id}")
        except Exception as e:
            logger.error(f"[{bot_id}] BTC error: {e}")
            await bots[bot_id].send_message(chat_id, "Payment error. Please try again.")
        finally:
            cursor.close()
            db_pool.putconn(conn)

    @dp.callback_query_handler(lambda c: c.data.startswith("usdt_"))
    async def process_usdt(cb: types.CallbackQuery, bot_id=bot_id):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bots[bot_id]
            config = CONFIGS[bot_id]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_id}] USDT TRC20 selected by user {user_id}")

            payment_id = str(uuid4())
            _, _, usdt_rate = fetch_crypto_rates()
            usd_amount = convert_rub_to_usd(config["PRICE"])
            usdt_amount = round(usd_amount / usdt_rate, 2)

            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_id} (payment_id, user_id, status, payment_method) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "usdt")
            )
            conn.commit()
            logger.info(f"[{bot_id}] USDT payment {payment_id} stored for user {user_id}")

            qr_code = create_qr_image(config["CRYPTO_ADDRESSES"]["USDT"])
            if qr_code:
                qr_bytes = base64.b64decode(qr_code)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=config["CRYPTO_ADDRESSES"]["USDT"])
            else:
                await bot.send_message(chat_id, config["CRYPTO_ADDRESSES"]["USDT"])

            await bot.send_message(chat_id, f"Send: {usdt_amount:.2f} USDT TRC20")
            logger.info(f"[{bot_id}] USDT address and amount sent to user {user_id}")
        except Exception as e:
            logger.error(f"[{bot_id}] USDT error: {e}")
            await bots[bot_id].send_message(chat_id, "Payment error. Please try again.")
        finally:
            cursor.close()
            db_pool.putconn(conn)

# Create channel invite
async def create_invite_link(bot_id, user_id):
    try:
        config = CONFIGS[bot_id]
        bot = bots[bot_id]
        bot_member = await bot.get_chat_member(chat_id=config["PRIVATE_CHANNEL_ID"], user_id=(await bot.get_me()).id)
        if not bot_member.can_invite_users:
            logger.error(f"[{bot_id}] Bot lacks invite permissions for channel {config['PRIVATE_CHANNEL_ID']}")
            return None

        for attempt in range(3):
            try:
                invite = await bot.create_chat_invite_link(
                    chat_id=config["PRIVATE_CHANNEL_ID"],
                    member_limit=1,
                    name=f"user_{user_id}_access"
                )
                logger.info(f"[{bot_id}] Invite created for user {user_id}: {invite.invite_link}")
                return invite.invite_link
            except Exception as e:
                logger.warning(f"[{bot_id}] Invite creation attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(1)
        logger.error(f"[{bot_id}] Failed to create invite for user {user_id}")
        return None
    except Exception as e:
        logger.error(f"[{bot_id}] Invite creation error: {e}")
        return None

# Verify YooMoney webhook
def verify_yoomoney_signature(data, bot_id):
    try:
        config = CONFIGS[bot_id]
        params = [
            data.get("notification_type", ""),
            data.get("operation_id", ""),
            data.get("amount", ""),
            data.get("currency", ""),
            data.get("datetime", ""),
            data.get("sender", ""),
            data.get("codepro", ""),
            config["NOTIFICATION_SECRET"],
            data.get("label", "")
        ]
        computed = hashlib.sha1("&".join(params).encode()).hexdigest()
        return computed == data.get("sha1_hash")
    except Exception as e:
        logger.error(f"[{bot_id}] YooMoney signature verification error: {e}")
        return False

# Find bot by payment ID
def find_bot_by_payment(payment_id):
    try:
        for bot_id in CONFIGS:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT user_id FROM payments_{bot_id} WHERE payment_id = %s", (payment_id,))
            result = cursor.fetchone()
            cursor.close()
            db_pool.putconn(conn)
            if result:
                logger.info(f"[{bot_id}] Payment {payment_id} found")
                return bot_id
        logger.warning(f"Payment {payment_id} not found")
        return None
    except Exception as e:
        logger.error(f"Payment lookup error: {e}")
        return None

# YooMoney webhook handler
async def handle_yoomoney_webhook(request):
    try:
        data = await request.post()
        logger.info(f"[{ENVIRONMENT}] YooMoney webhook received: {dict(data)}")
        payment_id = data.get("label")
        if not payment_id:
            logger.error(f"[{ENVIRONMENT}] Missing payment ID")
            return web.Response(status=400, text="Missing payment ID")

        bot_id = find_bot_by_payment(payment_id)
        if not bot_id:
            logger.error(f"[{ENVIRONMENT}] Bot not found for payment {payment_id}")
            return web.Response(status=400, text="Bot not found")

        if not verify_yoomoney_signature(data, bot_id):
            logger.error(f"[{bot_id}] Invalid YooMoney signature")
            return web.Response(status=400, text="Invalid signature")

        if data.get("notification_type") in ["p2p-incoming", "card-incoming"]:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(f"SELECT user_id FROM payments_{bot_id} WHERE payment_id = %s", (payment_id,))
            result = cursor.fetchone()
            if result:
                user_id = result[0]
                cursor.execute(
                    f"UPDATE payments_{bot_id} SET status = %s WHERE payment_id = %s",
                    ("success", payment_id)
                )
                conn.commit()
                bot = bots[bot_id]
                await bot.send_message(user_id, "Payment confirmed!")
                invite = await create_invite_link(bot_id, user_id)
                if invite:
                    await bot.send_message(user_id, f"Join the channel: {invite}")
                    logger.info(f"[{bot_id}] Payment {payment_id} processed for user {user_id}")
                else:
                    await bot.send_message(user_id, "Invite error. Contact @YourSupportHandle.")
                    logger.error(f"[{bot_id}] Invite creation failed for user {user_id}")
            else:
                logger.error(f"[{bot_id}] Payment {payment_id} not found")
            cursor.close()
            db_pool.putconn(conn)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[{ENVIRONMENT}] YooMoney webhook error: {e}")
        return web.Response(status=500)

# Store payment handler
async def save_payment(request, bot_id):
    try:
        data = await request.json()
        payment_id = data.get("label")
        user_id = data.get("user_id")
        method = data.get("payment_method", "unknown")
        logger.info(f"[{bot_id}] Storing payment: {payment_id} for user {user_id}")
        if not payment_id or not user_id:
            logger.error(f"[{bot_id}] Incomplete payment data")
            return web.Response(status=400, text="Incomplete data")

        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO payments_{bot_id} (payment_id, user_id, status, payment_method)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (payment_id) DO UPDATE SET user_id = %s, status = %s
            """,
            (payment_id, user_id, "pending", method, user_id, "pending")
        )
        conn.commit()
        logger.info(f"[{bot_id}] Payment {payment_id} stored")
        cursor.close()
        db_pool.putconn(conn)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[{bot_id}] Payment storage error: {e}")
        return web.Response(status=500)

# Health check handler
async def health_check(request):
    logger.info(f"[{ENVIRONMENT}] Health check requested")
    return web.Response(status=200, text=f"Running with {len(CONFIGS)} bots")

# Bot webhook handler
async def handle_bot_webhook(request, bot_id):
    try:
        if bot_id not in dispatchers:
            logger.error(f"[{bot_id}] Invalid bot ID")
            return web.Response(status=400, text="Invalid bot")

        bot = bots[bot_id]
        dp = dispatchers[bot_id]
        Bot.set_current(bot)
        dp.set_current(dp)

        update = await request.json()
        logger.debug(f"[{bot_id}] Webhook data: {update}")
        update_obj = types.Update(**update)
        asyncio.create_task(dp.process_update(update_obj))
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[{bot_id}] Webhook error: {e}")
        return web.Response(status=500)

# Setup webhooks
async def setup_webhooks():
    logger.info(f"Configuring webhooks for {len(CONFIGS)} bots")
    for bot_id in bots:
        try:
            bot = bots[bot_id]
            webhook_url = f"{BASE_URL}{WEBHOOK_ENDPOINT}/{bot_id}"
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(webhook_url)
            logger.info(f"[{bot_id}] Webhook set: {webhook_url}")
        except Exception as e:
            logger.error(f"[{bot_id}] Webhook setup error: {e}")
            raise SystemExit(1)

# Start server
async def start_server():
    try:
        await setup_webhooks()
        logger.info("Starting web server")
        app = web.Application()
        app.router.add_post("/", lambda req: web.Response(status=200, text="OK"))
        app.router.add_post(YOOMONEY_ENDPOINT, handle_yoomoney_webhook)
        app.router.add_get(HEALTH_ENDPOINT, health_check)
        app.router.add_post(HEALTH_ENDPOINT, health_check)
        for bot_id in CONFIGS:
            app.router.add_post(f"{YOOMONEY_ENDPOINT}/{bot_id}", lambda req, bot_id=bot_id: handle_yoomoney_webhook(req))
            app.router.add_post(f"{PAYMENT_ENDPOINT}/{bot_id}", lambda req, bot_id=bot_id: save_payment(req, bot_id))
            app.router.add_post(f"{WEBHOOK_ENDPOINT}/{bot_id}", lambda req, bot_id=bot_id: handle_bot_webhook(req, bot_id))
        logger.info(f"Routes active: {HEALTH_ENDPOINT}, {YOOMONEY_ENDPOINT}, {PAYMENT_ENDPOINT}, {WEBHOOK_ENDPOINT}, /")

        port = int(os.environ.get("PORT", 8000))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"Server running on port {port}")

        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise SystemExit(1)

if __name__ == "__main__":
    asyncio.run(start_server())
