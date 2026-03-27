"""
Async competitor sitemap scraper — يقرأ روابط Sitemap من data/competitors_list.json
ويُخرج data/competitors_latest.csv

استخراج JSON-LD أولاً (Salla / Zid) ثم وسوم meta — أقل اعتماداً على CSS.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SCRAPER_LAST_RUN_JSON = os.path.join("data", "scraper_last_run.json")
SCRAPER_PROGRESS_JSON = os.path.join("data", "scraper_progress.json")


def _write_scraper_last_run_meta(payload: Dict[str, Any]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(SCRAPER_LAST_RUN_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _merge_scraper_progress(updates: Dict[str, Any]) -> None:
    prev: Dict[str, Any] = {}
    if os.path.exists(SCRAPER_PROGRESS_JSON):
        try:
            with open(SCRAPER_PROGRESS_JSON, "r", encoding="utf-8") as f:
                prev = json.load(f)
        except Exception:
            pass
    prev.update(updates)
    os.makedirs("data", exist_ok=True)
    with open(SCRAPER_PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(prev, f, ensure_ascii=False, indent=2)


def _save_competitor_csv_rows(rows: List[Dict[str, Any]]) -> int:
    """يكتب competitors_latest.csv من قائمة صفوف. يعيد عدد الصفوف بعد إزالة التكرار."""
    if not rows:
        return 0
    _col_order = ["name", "price", "brand", "image_url", "comp_url", "sku"]
    df = pd.DataFrame(rows).drop_duplicates(subset=["comp_url"])
    for c in _col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[_col_order]
    
    os.makedirs("data", exist_ok=True)
    temp_file = "data/competitors_temp.csv"
    final_file = "data/competitors_latest.csv"
    df_ar = df.rename(
        columns={
            "name": "الاسم",
            "price": "السعر",
            "brand": "الماركة",
            "image_url": "رابط_الصورة",
            "comp_url": "رابط_المنتج",
            "sku": "sku",
        }
    )
    df_ar.to_csv(temp_file, index=False, encoding="utf-8-sig")
    shutil.move(temp_file, final_file)
    return len(df)


def _tag_local(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _stable_sku_from_url(url: str) -> str:
    """معرّف ثابت للمنافس يُطابق عمود sku في المطابقة."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _extract_brand_from_product(data: dict) -> str:
    """brand في schema.org قد يكون نصاً أو Brand { name }."""
    b = data.get("brand")
    if b is None:
        return ""
    if isinstance(b, str):
        return b.strip()
    if isinstance(b, dict):
        n = b.get("name") or b.get("@value")
        if isinstance(n, dict):
            n = n.get("value") or n.get("text")
        return str(n or "").strip()
    if isinstance(b, list) and b:
        x = b[0]
        if isinstance(x, dict):
            return _extract_brand_from_product({"brand": x})
        return str(x).strip()
    return str(b).strip()


def _extract_image_url_from_product(data: dict) -> str:
    """صورة المنتج: نص، أو ImageObject، أو قائمة."""
    img = data.get("image")
    if img is None:
        return ""
    if isinstance(img, str):
        return img.strip()
    if isinstance(img, dict):
        u = img.get("url") or img.get("contentUrl") or img.get("@id")
        return str(u or "").strip()
    if isinstance(img, list) and img:
        first = img[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            u = first.get("url") or first.get("contentUrl")
            return str(u or "").strip()
    return ""


def _filter_salla_like_product_urls(urls: List[str]) -> List[str]:
    """يحتفظ بصفحات منتج سلة/زد النموذجية (.../اسم-المنتج/p123456789) ويستبعد المدونة والأقسام وروابط CDN."""
    out: List[str] = []
    for u in urls:
        try:
            p = urlparse(u)
        except Exception:
            continue
        host = (p.netloc or "").lower()
        if "cdn.salla.sa" in host or host.startswith("cdn."):
            continue
        path = p.path or ""
        if re.search(r"/p\d+$", path):
            out.append(u)
    return list(dict.fromkeys(out))


def _parse_price_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    t = re.sub(r"[^\d.,]", "", str(text).replace(",", ""))
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _price_from_offers(offers: Any) -> Optional[float]:
    """يستخرج السعر من كتلة offers (قائمة، Offer، AggregateOffer)."""
    if offers is None:
        return None
    if isinstance(offers, list):
        if not offers:
            return None
        offers = offers[0]
    if not isinstance(offers, dict):
        return _parse_price_from_text(str(offers))
    otype = offers.get("@type", "")
    if otype == "AggregateOffer":
        p = offers.get("lowPrice") or offers.get("highPrice") or offers.get("price")
    else:
        p = offers.get("price")
    if p is None:
        return None
    if isinstance(p, (int, float)):
        return float(p)
    return _parse_price_from_text(str(p))


def _is_product_type(t: Any) -> bool:
    if isinstance(t, list):
        return any(x in ("Product", "ProductGroup") for x in t)
    return t in ("Product", "ProductGroup")


def _first_product_node(obj: Any) -> Optional[dict]:
    """أول كائن JSON-LD من نوع Product / ProductGroup (يشمل @graph وقوائم)."""
    if isinstance(obj, list):
        for x in obj:
            found = _first_product_node(x)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        tt = obj.get("@type")
        if _is_product_type(tt):
            return obj
        if "@graph" in obj:
            found = _first_product_node(obj["@graph"])
            if found is not None:
                return found
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _first_product_node(v)
                if found is not None:
                    return found
    return None


class AsyncCompetitorScraper:
    """جلب صفحات المنتجات — JSON-LD أولاً، ثم meta، مع حد تزامن وتأخير مهذب."""

    def __init__(self, concurrency_limit: int = 15):
        self.concurrency_limit = max(1, int(concurrency_limit))
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        """رؤوس واقعية لتقليل الحظر (Referer + Accept-Encoding + …)."""
        ref = referer or "https://www.google.com/"
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Referer": ref,
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _referer_for_url(self, url: str) -> str:
        try:
            p = urlparse(url)
            if p.scheme and p.netloc:
                return f"{p.scheme}://{p.netloc}/"
        except Exception:
            pass
        return "https://www.google.com/"

    async def scan_sitemap(
        self, session: aiohttp.ClientSession, sitemap_url: str
    ) -> tuple[List[str], Dict[str, Any]]:
        """يجلب sitemap أو sitemapindex (يتفرع بشكل متكرر لكل sub-sitemap)."""
        collected: List[str] = []
        diag: Dict[str, Any] = {"http_status": None, "fetch_error": None, "parse_error": None}
        ref = self._referer_for_url(sitemap_url)
        try:
            async with session.get(
                sitemap_url,
                timeout=aiohttp.ClientTimeout(total=180),
                headers=self._get_headers(referer=ref),
            ) as resp:
                diag["http_status"] = resp.status
                if resp.status != 200:
                    return [], diag
                text = await resp.text()
        except Exception as e:
            diag["fetch_error"] = str(e)
            return [], diag

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            diag["parse_error"] = str(e)
            return [], diag

        root_local = _tag_local(root.tag)
        if root_local == "sitemapindex":
            child_locs: List[str] = []
            for el in root.iter():
                if _tag_local(el.tag) == "loc" and el.text:
                    u = el.text.strip()
                    if u.startswith("http"):
                        child_locs.append(u)
            for child in child_locs:
                sub, _ = await self.scan_sitemap(session, child)
                collected.extend(sub)
            return list(dict.fromkeys(collected)), diag

        if root_local == "urlset":
            for el in root.iter():
                if _tag_local(el.tag) == "loc" and el.text:
                    u = el.text.strip()
                    if u.startswith("http"):
                        collected.append(u)
            return list(dict.fromkeys(collected)), diag

        return [], diag

    async def fetch_product(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[Dict[str, Any]]:
        """يجلب صفحة منتج ويستخرج البيانات."""
        async with self.semaphore:
            ref = self._referer_for_url(url)
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers=self._get_headers(referer=ref),
                ) as resp:
                    if resp.status != 200:
                        return None
                    html = await resp.text()
            except Exception:
                return None

            soup = BeautifulSoup(html, "html.parser")
            
            # 1. JSON-LD
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    node = _first_product_node(data)
                    if node:
                        name = node.get("name")
                        price = _price_from_offers(node.get("offers"))
                        if name and price:
                            return {
                                "name": str(name).strip(),
                                "price": price,
                                "brand": _extract_brand_from_product(node),
                                "image_url": _extract_image_url_from_product(node),
                                "comp_url": url,
                                "sku": node.get("sku") or _stable_sku_from_url(url),
                            }
                except Exception:
                    continue

            # 2. Meta Tags (Fallback)
            name = (
                soup.find("meta", property="og:title") or
                soup.find("meta", name="twitter:title") or
                soup.find("title")
            )
            if name:
                name_text = name.get("content") if hasattr(name, "get") else name.string
                price_meta = (
                    soup.find("meta", property="product:price:amount") or
                    soup.find("meta", property="og:price:amount")
                )
                if name_text and price_meta:
                    return {
                        "name": str(name_text).strip(),
                        "price": _parse_price_from_text(price_meta.get("content")),
                        "brand": "",
                        "image_url": (soup.find("meta", property="og:image") or {}).get("content", ""),
                        "comp_url": url,
                        "sku": _stable_sku_from_url(url),
                    }

            return None

async def run_scraper(sitemap_urls: List[str], progress_callback=None):
    """دالة رئيسية لتشغيل الكاشط من Streamlit."""
    all_rows = []
    async with aiohttp.ClientSession() as session:
        scraper = AsyncCompetitorScraper(concurrency_limit=10)
        
        all_product_urls = []
        for i, s_url in enumerate(sitemap_urls):
            if progress_callback:
                progress_callback(f"جاري جلب Sitemap: {s_url}...", 0.1)
            urls, _ = await scraper.scan_sitemap(session, s_url)
            product_urls = _filter_salla_like_product_urls(urls)
            all_product_urls.extend(product_urls)
            
        all_product_urls = list(dict.fromkeys(all_product_urls))
        total = len(all_product_urls)
        
        if total == 0:
            return 0
            
        for i, url in enumerate(all_product_urls):
            res = await scraper.fetch_product(session, url)
            if res:
                all_rows.append(res)
            
            if progress_callback and i % 5 == 0:
                progress_callback(f"تم كشط {len(all_rows)} منتج من {total}...", 0.1 + (0.8 * (i/total)))
                
    count = _save_competitor_csv_rows(all_rows)
    return count
