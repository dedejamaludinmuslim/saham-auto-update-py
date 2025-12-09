import os
import logging
from datetime import datetime, time

import pytz
from dotenv import load_dotenv
import yfinance as yf
from supabase import create_client

# ------------------------------------------------------------------
# Load .env
# ------------------------------------------------------------------
load_dotenv()

ID_TZ = pytz.timezone("Asia/Jakarta")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY belum di-set di .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

# ------------------------------------------------------------------
# Helper: cek jam bursa
# ------------------------------------------------------------------
def is_market_hours(now_jkt: datetime) -> bool:
    """
    Contoh sederhana: Senin–Jumat, 09:00–15:30 WIB.
    """
    if now_jkt.weekday() >= 5:  # 5 = Sabtu, 6 = Ahad
        return False

    start = time(9, 0)
    end = time(15, 30)
    return start <= now_jkt.time() <= end


# ------------------------------------------------------------------
# Ambil daftar saham yang mau di-track dari Supabase
# ------------------------------------------------------------------
def get_tracked_stocks():
    """
    Ambil saham yang:
      - is_tracked = true
      - yahoo_symbol TIDAK kosong
    Kolom yang dipakai: id, kode, yahoo_symbol
    """
    resp = (
        supabase
        .table("saham")
        .select("id, kode, yahoo_symbol, is_tracked")
        .eq("is_tracked", True)
        .execute()
    )

    data = getattr(resp, "data", resp)

    # Filter lagi di sisi Python: yahoo_symbol harus ada & tidak string kosong
    stocks = [
        row for row in data
        if row.get("yahoo_symbol") not in (None, "", "null")
    ]

    logging.info(
        f"Ditemukan {len(stocks)} saham is_tracked=true dengan yahoo_symbol terisi"
    )
    return stocks


# ------------------------------------------------------------------
# Fetch harga terakhir dari yfinance
# ------------------------------------------------------------------
def fetch_price(symbol: str):
    """
    Fetch data dari yfinance.
    Mengembalikan dict:
      { date (YYYY-MM-DD), close (float) }
    atau None kalau gagal.
    """
    try:
        ticker = yf.Ticker(symbol)
        # Ambil 5 hari ke belakang, ambil baris terakhir
        hist = ticker.history(period="5d", interval="1d")
        if hist.empty:
            logging.warning(f"yfinance: data kosong untuk {symbol}")
            return None

        last_row = hist.iloc[-1]
        date_str = last_row.name.date().isoformat()
        close_price = float(last_row["Close"])

        return {
            "date": date_str,
            "close": close_price,
        }
    except Exception as e:
        logging.warning(f"yfinance gagal {symbol}: {e}")
        return None


# ------------------------------------------------------------------
# Simpan ke tabel harga_saham_harian
# ------------------------------------------------------------------
def save_price_to_supabase(stock: dict, data: dict):
    """
    stock: row dari tabel saham (id, kode, yahoo_symbol)
    data:  hasil fetch_price (date, close)

    Logika:
      - cek dulu apakah sudah ada baris dengan (saham_id, close_date)
      - kalau ada -> update close_price
      - kalau belum -> insert baris baru
    """
    saham_id = stock["id"]
    close_date = data["date"]
    close_price = data["close"]

    # cek apakah sudah ada data untuk tanggal itu
    check_resp = (
        supabase
        .table("harga_saham_harian")
        .select("id")
        .eq("saham_id", saham_id)
        .eq("close_date", close_date)
        .limit(1)
        .execute()
    )
    existing = getattr(check_resp, "data", check_resp)

    if existing:
        harga_id = existing[0]["id"]
        supabase.table("harga_saham_harian").update(
            {"close_price": close_price}
        ).eq("id", harga_id).execute()
        action = "update"
    else:
        supabase.table("harga_saham_harian").insert(
            {
                "saham_id": saham_id,
                "close_date": close_date,
                "close_price": close_price,
            }
        ).execute()
        action = "insert"

    return action


# ------------------------------------------------------------------
# Update tabel system_status
# ------------------------------------------------------------------
def update_system_status(now_jkt: datetime, market_hours: bool):
    """
    Tabel: system_status
    Kolom:
      - id (smallint, kita pakai nilai tetap 1)
      - last_auto_update_at
      - last_out_of_hours_run
    """
    ts_iso = now_jkt.isoformat()
    field = "last_auto_update_at" if market_hours else "last_out_of_hours_run"

    resp = (
        supabase.table("system_status")
        .update({field: ts_iso})
        .eq("id", 1)
        .execute()
    )
    data = getattr(resp, "data", resp)

    # kalau belum ada row (id=1), insert
    if not data:
        supabase.table("system_status").insert(
            {
                "id": 1,
                field: ts_iso,
            }
        ).execute()


# ------------------------------------------------------------------
# Main runner
# ------------------------------------------------------------------
def run_once():
    now_jkt = datetime.now(ID_TZ)
    market_hours = is_market_hours(now_jkt)

    logging.info(
        f"runOnce @ Jakarta time = {now_jkt.isoformat()} (marketHours={market_hours})"
    )
    logging.info(
        f"Mulai update @ {now_jkt.isoformat()} (Jakarta date = {now_jkt.date()})"
    )

    # Ambil daftar saham yang di-track
    stocks = get_tracked_stocks()
    logging.info(f"Akan fetch harga untuk {len(stocks)} saham dari yfinance...")

    for stock in stocks:
        kode = stock.get("kode")
        symbol = stock.get("yahoo_symbol")

        if not symbol:
            logging.warning(f"Lewati {kode}: yahoo_symbol kosong")
            continue

        price = fetch_price(symbol)
        if not price:
            logging.warning(f"Gagal mendapatkan data {symbol} ({kode})")
            continue

        try:
            action = save_price_to_supabase(stock, price)
            logging.info(
                f"{kode} ({symbol}) {price['date']} close={price['close']} [{action}]"
            )
        except Exception as e:
            logging.warning(f"Gagal simpan {kode} ({symbol}) ke Supabase: {e}")

    # update system_status
    try:
        update_system_status(now_jkt, market_hours)
    except Exception as e:
        logging.warning(f"Gagal update system_status: {e}")


if __name__ == "__main__":
    run_once()
