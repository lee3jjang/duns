from urllib.parse import urljoin
import ast
import json
import os

import httpx
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TABLE_NAME = "products"
BASE_URL = "https://shopdunssweden.se/"
MENU_LINKS = [
    {"name": "Home", "href": "/"},
    {"name": "Radish", "href": "/collections/radish/radish"},
    {"name": "Dungaree", "href": "/collections/dungaree"},
    {"name": "Long Sleeve Suit", "href": "/collections/long-sleeve-suit"},
    {"name": "Zip Suit", "href": "/collections/zip-suit"},
    {"name": "Summer Suit", "href": "/collections/short-sleeved-suit/Summer-Suit"},
    {"name": "Play suit", "href": "/collections/play-suit/Play-suit"},
    {"name": "Short Sleeved Top", "href": "/collections/short-sleeved-top"},
    {"name": "Short Pants", "href": "/collections/short-pants/Short-pants"},
    {"name": "Skater Dress", "href": "/collections/skater-dress/Skater-Dress"},
    {"name": "Baggy Pants", "href": "/collections/baggy-trousers"},
    {"name": "Long Sleeved Top", "href": "/collections/long-sleeved-top"},
    {"name": "Hood Suit", "href": "/collections/hood-suit"},
    {"name": "Long Sleeve Dress", "href": "/collections/long-sleeved-dress"},
    {
        "name": "LS Dress w. Gathered Skirt",
        "href": "/collections/long-sleeve-dress-with-gathered-skirt/Long-Sleeve-Dress-with-Gathered-Skirt",
    },
    {"name": "Long Sleeve Body", "href": "/collections/body"},
    {"name": "Sun Hat", "href": "/collections/sun-hat"},
    {
        "name": "Sleeveless Dress with Gathered Skirt",
        "href": "/collections/sleeveless-dress-with-gathered-skirt/Sleeveless-Dress-with-Gathered-Skirt",
    },
    {"name": "Babycap", "href": "/collections/babycap"},
]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_product_data(
    client: httpx.AsyncClient,
    href: str,
) -> pd.DataFrame:
    """Fetch product data from a given page URL."""
    url = urljoin(BASE_URL, href)
    response = await client.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    script_tags = soup.find_all(
        "script",
        type="application/json",
        class_="bc-sf-filter-product-script",
    )

    product_data_list = []
    for tag in script_tags:
        json_data = json.loads(tag.string)
        product_data_list.append(json_data)

    df = pd.DataFrame(product_data_list)
    return df


async def fetch_all_product_data(menu_links: list[dict]) -> pd.DataFrame:
    async with httpx.AsyncClient() as client:
        tasks = [fetch_product_data(client, item["href"]) for item in menu_links]
        results = await asyncio.gather(*tasks)
        raw_product_df = pd.concat(results, ignore_index=True)
        product_df = (
            raw_product_df.astype(
                {"variants": "str", "options_with_values": "str", "images": "str"}
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )
        return product_df


def insert_data_to_supabase(product_df: pd.DataFrame) -> None:
    if product_df.empty:
        print("No product data to insert.")
        return

    data = product_df.to_dict(orient="records")
    supabase.table(table_name=TABLE_NAME).insert(data).execute()


def get_product_ids_from_supabase() -> list[int]:
    response = supabase.table(table_name=TABLE_NAME).select("id").execute()
    df = pd.DataFrame.from_records(response.data)
    return df["id"].tolist()


def get_chat_ids_from_supabase() -> list[int]:
    response = supabase.table(table_name="chats").select("id").execute()
    df = pd.DataFrame.from_records(response.data)
    return df["id"].tolist()


def get_bot_updates() -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    with httpx.Client() as httpx_client:
        res = httpx_client.get(url)
    return res.json()


def generate_telegram_message(product_df: pd.DataFrame) -> str:
    message_lines = ["🆕 [신상 입고 알림]\n"]

    for idx, row in product_df.iterrows():
        title = row["title"]
        url = f"https://shopdunssweden.se{row['url']}"

        try:
            options_raw = ast.literal_eval(row["options_with_values"])
        except Exception as e:
            print(f"[Error parsing options] {row['title']}: {e}")
            options_raw = []

        options = []
        for opt in options_raw:
            values = opt.get("values", [])
            options.extend(values)
        options_str = ", ".join(opt.strip() for opt in options)

        msg = f"""**{title}**
옵션: {options_str}  
🔗 [상품보기]({url})\n"""
        message_lines.append(f"{idx + 1}. {msg}")

    return "\n".join(message_lines)


def send_message_to_chat(chat_id: int, message: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    with httpx.Client() as client:
        client.post(url, data=payload)


async def main() -> None:
    product_df = await fetch_all_product_data(MENU_LINKS)
    product_ids = get_product_ids_from_supabase()
    # new_product_df = product_df.loc[lambda x: ~x["id"].isin(product_ids)]
    new_product_df = product_df.head(3)  # For testing, limit to first 3 products

    if not new_product_df.empty:
        print(f"Inserting {len(new_product_df)} new products into Supabase.")
        insert_data_to_supabase(new_product_df)
        message = generate_telegram_message(new_product_df)
        chat_ids = get_chat_ids_from_supabase()
        for chat_id in chat_ids:
            send_message_to_chat(chat_id, message)
    else:
        print("No new products to insert into Supabase.")
