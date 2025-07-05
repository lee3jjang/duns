from urllib.parse import urljoin
import ast
import json
import os

import httpx
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

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


async def fetch_all_product_data(
    menu_links: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    async with httpx.AsyncClient() as client:
        tasks = [fetch_product_data(client, item["href"]) for item in menu_links]
        results = await asyncio.gather(*tasks)

    raw_product_df = pd.concat(results, ignore_index=True)

    PRODUCT_COLUMNS = ["id", "title", "handle", "url"]
    product_df = (
        raw_product_df[PRODUCT_COLUMNS].drop_duplicates().reset_index(drop=True)
    )
    product_df_exploded = raw_product_df.rename(columns={"id": "product_id"}).explode(
        "variants"
    )
    product_variant_product_id_df = (
        pd.json_normalize(product_df_exploded["variants"])[["id"]]
        .set_index(product_df_exploded["product_id"])
        .drop_duplicates()
        .reset_index()
    )

    PRODUCT_VARIANT_COLUMNS = ["id", "title", "name", "available"]
    product_variant_df = (
        pd.DataFrame.from_records(raw_product_df["variants"].sum())[
            PRODUCT_VARIANT_COLUMNS
        ]
        .drop_duplicates()
        .merge(
            product_variant_product_id_df,
            on="id",
            how="left",
        )
    )

    return (product_df, product_variant_df)


def insert_product_to_supabase(product_df: pd.DataFrame) -> None:
    if product_df.empty:
        print("No product data to insert.")
        return

    data = product_df.to_dict(orient="records")
    supabase.table(table_name="products").insert(data).execute()


def get_product_ids_from_supabase() -> list[int]:
    response = supabase.table(table_name="products").select("id").execute()
    df = pd.DataFrame.from_records(response.data)
    return df["id"].tolist()


def get_available_product_variant_ids_from_supabase() -> list[int]:
    response = (
        supabase.table(table_name="product_variants")
        .select("id")
        .eq("available", True)
        .execute()
    )
    df = pd.DataFrame.from_records(response.data)
    return df["id"].tolist()


def update_product_variant_to_supabase(product_variant_df: pd.DataFrame) -> None:
    if product_variant_df.empty:
        print("No product variant data to insert.")
        return

    data = product_variant_df.to_dict(orient="records")
    supabase.table(table_name="product_variants").delete().neq("id", 99999999).execute()
    supabase.table(table_name="product_variants").insert(data).execute()


def get_chat_ids_from_supabase() -> list[int]:
    response = supabase.table(table_name="chats").select("id").execute()
    df = pd.DataFrame.from_records(response.data)
    return df["id"].tolist()


def get_bot_updates() -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    with httpx.Client() as httpx_client:
        res = httpx_client.get(url)
    return res.json()


def send_message_to_chat(chat_id: int, message: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    with httpx.Client() as client:
        client.post(url, data=payload)


def generate_new_product_alert_message(new_product_df: pd.DataFrame) -> str:
    message_lines: list[str] = ["🆕 [신상 입고 알림]\n"]

    for idx, row in new_product_df.iterrows():
        title = row["title"]
        url = f"https://shopdunssweden.se{row['url']}"

        msg = f"""**{title}**
🔗 [상품보기]({url})\n"""
        message_lines.append(f"{idx + 1}. {msg}")

    return "\n".join(message_lines)


def generate_restock_alert_message(
    new_available_product_variant_df: pd.DataFrame,
) -> str:
    message_lines = ["🔔 *[재고 알림]*\n"]

    for idx, row in new_available_product_variant_df.iterrows():
        name = row.get("name", "").strip()
        url = f"https://shopdunssweden.se{row['url'].strip()}"

        msg = f"""*{name}*\n🔗 [상품보기]({url})\n"""
        message_lines.append(f"{idx + 1}. {msg}")

    return "\n".join(message_lines)


async def main() -> None:
    product_df, product_variant_df = await fetch_all_product_data(MENU_LINKS)
    chat_ids = get_chat_ids_from_supabase()
    product_ids = get_product_ids_from_supabase()
    new_product_df = product_df.loc[lambda x: ~x["id"].isin(product_ids)]
    if not new_product_df.empty:
        # message = generate_new_product_alert_message(new_product_df)
        # for chat_id in chat_ids:
        #     send_message_to_chat(chat_id, message)
        insert_product_to_supabase(new_product_df)

    available_product_variant_ids = get_available_product_variant_ids_from_supabase()
    new_available_product_variant_df = product_variant_df.query("available").loc[
        lambda x: ~x["id"].isin(available_product_variant_ids)
    ]

    if not new_available_product_variant_df.empty:
        new_available_product_variant_df_with_product_info = (
            new_available_product_variant_df.merge(
                product_df.drop(columns=["title"]).rename(columns={"id": "product_id"}),
                on="product_id",
                how="left",
            )
        )
        message = generate_restock_alert_message(
            new_available_product_variant_df_with_product_info
        )
        for chat_id in chat_ids:
            send_message_to_chat(chat_id, message)
        update_product_variant_to_supabase(product_variant_df)


if __name__ == "__main__":
    asyncio.run(main())
