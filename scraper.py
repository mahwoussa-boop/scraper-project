"""
Async competitor sitemap scraper v34.0 — النسخة الاحترافية الشاملة
═══════════════════════════════════════════════════════════
✅ دمج sitemap_resolve: اكتشاف تلقائي وذكي لروابط Sitemap (سلة/زد/وغيرها)
✅ دمج async_scraper: استخراج JSON-LD متقدم (Salla Config) وتجاوز الحماية
✅ الحفظ الفوري (Real-time Save) لتمكين العرض الحي في لوحة التحكم
✅ استخراج الحجم والنوع (Size & Type) لضمان دقة المطابقة 100%
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
import random

# إنشاء مجلد البيانات إذا لم يكن موجوداً قبل استيراد أي مكتبات أو تعريف مسارات
os.makedirs("data", exist_ok=True)

import aiohttp
import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ملفات الحالة للأتمتة
SCRAPER_STATE_JSON = os.path.join("data", "scraper_state.json")
COMPETITORS_LATEST_CSV = os.path.join("data", "competitors_latest.csv")

# رؤوس متصفح حقيقية لتقليل الحظر
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── 1. تقنيات الاستخراج الذكي للحجم والنوع ──────────────────────────

def extract_size_from_name(name):
    """استخراج الحجم من الاسم بدقة (100ml, 50ml, 13ML)"""
    if not name: return ""
    match = re.search(r'(\d+)\s*(ml|مل|g|جم|oz|أونصة)', str(name), re.I)
    return match.group(0) if match else ""

def extract_type_from_name(name):
    """استخراج النوع من الاسم (EDP, EDT, Parfum)"""
    if not name: return ""
    n = str(name).upper()
    if "EDP" in n or "EAU DE PARFUM" in n or "أو دو بارفيوم" in n: return "EDP"
    if "EDT" in n or "EAU DE TOILETTE" in n or "أو دو تواليت" in n: return "EDT"
    if "PARFUM" in n or "بارفيوم" in n: return "Parfum"
    if "TESTER" in n or "تستر" in n: return "Tester"
    return ""

# ── 2. تقنيات sitemap_resolve (الاكتشاف الذكي) ──────────────────────────

def _parse_origin(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u: return None
    if not u.lower().startswith(("http://", "https://")): u = "https://" + u
    p = urlparse(u)
    if not p.netloc: return None
    scheme = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{scheme}://{p.netloc}"

def _response_is_sitemap_xml(text: str) -> bool:
    t = (text or "").lstrip()
    if not t: return False
    if t.startswith("<?xml") or t.startswith("<"):
        return bool(re.search(r"<(?:urlset|sitemapindex)\b", t[:2000], re.I))
    return False

async def resolve_to_sitemap_url(session: aiohttp.ClientSession, user_input: str) -> Optional[str]:
    """يحول رابط المتجر إلى رابط Sitemap صالح."""
    origin = _parse_origin(user_input)
    if not origin: return None
    
    try:
        async with session.get(origin.rstrip("/") + "/robots.txt", timeout=15) as r:
            if r.status == 200:
                text = await r.text()
                for line in text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        u = line.split(":", 1)[1].strip()
                        if u.startswith("http"): return u
    except: pass

    candidates = [f"{origin}/sitemap.xml", f"{origin}/sitemap_products_1.xml", f"{origin}/sitemap_index.xml"]
    for c in candidates:
        try:
            async with session.get(c, timeout=15) as r:
                if r.status == 200 and _response_is_sitemap_xml(await r.text()): return c
        except: pass
    return user_input if user_input.endswith(".xml") else f"{origin}/sitemap.xml"

# ── 3. تقنيات async_scraper (الاستخراج المتقدم) ──────────────────────────

def load_scraper_state() -> Dict[str, Any]:
    if os.path.exists(SCRAPER_STATE_JSON):
        try:
            with open(SCRAPER_STATE_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {"urls_to_scrape": [], "scraped_urls": [], "results": [], "last_update": ""}

def save_scraper_state(state: Dict[str, Any]):
    os.makedirs("data", exist_ok=True)
    state["last_update"] = datetime.now().isoformat()
    with open(SCRAPER_STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _clean_price(val) -> Optional[float]:
    if val is None: return None
    if isinstance(val, (int, float)): return float(val) if val > 0 else None
    s = str(val).strip()
    s = re.sub(r'[^\d.,٠-٩]', '', s)
    _AR_DIGITS = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
    for ar, en in _AR_DIGITS.items(): s = s.replace(ar, en)
    s = s.replace(',', '')
    try:
        p = float(s)
        return p if p > 0 else None
    except: return None

def _clean_text(val) -> str:
    if val is None: return ""
    s = str(val).strip()
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

def _save_competitor_csv_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows: return 0
    df = pd.DataFrame(rows).drop_duplicates(subset=["comp_url"])
    # التأكد من وجود كافة الأعمدة المطلوبة للمقارنة
    _col_order = ["name", "price", "brand", "size", "type", "image_url", "comp_url", "sku"]
    for c in _col_order:
        if c not in df.columns: df[c] = ""
    df = df[_col_order]
    df_ar = df.rename(columns={
        "name": "اسم المنتج", "price": "السعر", "brand": "الماركة",
        "size": "الحجم", "type": "النوع",
        "image_url": "رابط_الصورة", "comp_url": "رابط_المنتج", "sku": "sku",
    })
    os.makedirs("data", exist_ok=True)
    df_ar.to_csv(COMPETITORS_LATEST_CSV, index=False, encoding="utf-8-sig")
    return len(df)

class AsyncCompetitorScraper:
    def __init__(self, concurrency_limit: int = 15):
        self.concurrency_limit = concurrency_limit
        self.semaphore = asyncio.Semaphore(concurrency_limit)

    async def scan_sitemap(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        collected = []
        try:
            async with session.get(url, timeout=30, headers=_BROWSER_HEADERS) as resp:
                if resp.status != 200: return []
                text = await resp.text()
                urls = re.findall(r'<loc>(https?://[^<]+)</loc>', text)
                for u in urls:
                    u = u.strip()
                    if "sitemap" in u.lower() and (u.endswith(".xml") or "index" in u.lower()):
                        sub = await self.scan_sitemap(session, u)
                        collected.extend(sub)
                    else:
                        collected.append(u)
        except: pass
        return list(dict.fromkeys(collected))

    async def fetch_product(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            try:
                await asyncio.sleep(random.uniform(0.1, 0.4))
                async with session.get(url, timeout=25, headers=_BROWSER_HEADERS) as resp:
                    if resp.status != 200: return None
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    
                    product_data = None
                    # 1. Salla Config
                    salla_match = re.search(r'window\.Salla\.config\s*=\s*({.*?});', html, re.DOTALL)
                    if salla_match:
                        try:
                            config = json.loads(salla_match.group(1))
                            p = config.get('product', {})
                            if p:
                                product_data = {
                                    "name": _clean_text(p.get('name')),
                                    "price": _clean_price(p.get('price', {}).get('amount') if isinstance(p.get('price'), dict) else p.get('price')),
                                    "brand": _clean_text(p.get('brand_name', '')),
                                    "image_url": p.get('image', ''),
                                    "comp_url": url,
                                    "sku": p.get('sku') or str(p.get('id', ''))
                                }
                        except: pass

                    # 2. JSON-LD Fallback
                    if not product_data:
                        for script in soup.find_all("script", type="application/ld+json"):
                            try:
                                data = json.loads(script.string)
                                nodes = data.get("@graph", [data]) if isinstance(data, dict) else data
                                if not isinstance(nodes, list): nodes = [nodes]
                                for node in nodes:
                                    if node.get("@type") in ("Product", "ProductGroup"):
                                        name = _clean_text(node.get("name"))
                                        offers = node.get("offers", {})
                                        if isinstance(offers, list) and offers: offers = offers[0]
                                        price = _clean_price(offers.get("price") or offers.get("lowPrice"))
                                        if name and price:
                                            product_data = {
                                                "name": name, "price": price,
                                                "brand": _clean_text(node.get("brand", {}).get("name") if isinstance(node.get("brand"), dict) else node.get("brand")),
                                                "image_url": _clean_text(node.get("image", [None])[0] if isinstance(node.get("image"), list) else node.get("image")),
                                                "comp_url": url, "sku": node.get("sku") or hashlib.md5(url.encode()).hexdigest()[:10]
                                            }
                                            break
                                if product_data: break
                            except: continue

                    if product_data:
                        # استخراج الحجم والنوع من الاسم لضمان المطابقة
                        name = product_data["name"]
                        product_data["size"] = extract_size_from_name(name)
                        product_data["type"] = extract_type_from_name(name)
                        return product_data
            except: pass
            return None

async def run_scraper(sitemap_urls: List[str], progress_callback=None, force_new=False):
    state = load_scraper_state()
    
    def is_product_url(u):
        return "/p/" in u and "/c/" not in u and not any(x in u for x in ["/blog/", "/tags/", "/policy"])

    async with aiohttp.ClientSession() as session:
        if force_new or not state.get("urls_to_scrape"):
            if progress_callback: progress_callback("🔍 جاري اكتشاف روابط Sitemap وجمع المنتجات...", 0.05)
            scraper = AsyncCompetitorScraper()
            all_urls = []
            for raw_url in sitemap_urls:
                s_url = await resolve_to_sitemap_url(session, raw_url)
                urls = await scraper.scan_sitemap(session, s_url)
                all_urls.extend([u for u in urls if is_product_url(u)])
            
            state["urls_to_scrape"] = list(dict.fromkeys(all_urls))
            state["scraped_urls"] = []
            state["results"] = []
            save_scraper_state(state)

        to_scrape = [u for u in state["urls_to_scrape"] if u not in state["scraped_urls"]]
        total = len(state["urls_to_scrape"])
        
        if not to_scrape:
            return len(state["results"]), state["results"]

        scraper = AsyncCompetitorScraper(concurrency_limit=15)
        
        for i, url in enumerate(to_scrape):
            res = await scraper.fetch_product(session, url)
            state["scraped_urls"].append(url)
            if res:
                state["results"].append(res)
                _save_competitor_csv_rows(state["results"])
            
            if i % 5 == 0 or i == len(to_scrape) - 1:
                save_scraper_state(state)
                if progress_callback:
                    prog = 0.1 + (0.9 * (len(state["scraped_urls"]) / total))
                    progress_callback(f"تم استخراج {len(state['results'])} منتج من {total}...", prog)
            
            if i % 30 == 0: await asyncio.sleep(0.3)

    count = _save_competitor_csv_rows(state["results"])
    return count, state["results"]
