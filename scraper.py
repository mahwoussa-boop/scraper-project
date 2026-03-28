"""
Async competitor sitemap scraper v27.0
═══════════════════════════════════════
✅ v27: تنظيف السعر ليصبح float + تنظيف النصوص من المسافات الزائدة
✅ استخراج: اسم المنتج، السعر، الماركة، رابط الصورة، رابط المنتج
✅ JSON-LD أولاً (Salla / Zid) ثم وسوم meta
✅ sitemap_resolve لضمان العثور على روابط Sitemap صالحة
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
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import aiohttp
import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SCRAPER_LAST_RUN_JSON = os.path.join("data", "scraper_last_run.json")
SCRAPER_PROGRESS_JSON = os.path.join("data", "scraper_progress.json")

# ── sitemap_resolve logic ──────────────────────────
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
}

def _parse_origin(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u: return None
    if not u.lower().startswith(("http://", "https://")): u = "https://" + u
    p = urlparse(u)
    if not p.netloc: return None
    scheme = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{scheme}://{p.netloc}"

def _looks_like_direct_sitemap_url(url: str) -> bool:
    p = urlparse(url.strip())
    path = (p.path or "").lower()
    return path.endswith(".xml") and ("sitemap" in path or "blog-" in path)

def _response_is_sitemap_xml(text: str) -> bool:
    t = (text or "").lstrip()
    if not t: return False
    if t.startswith("<?xml") or t.startswith("<"):
        return bool(re.search(r"<(?:urlset|sitemapindex)\b", t[:2000], re.I))
    return False

def _probe_sitemap_url(url: str, timeout: float = 20.0) -> bool:
    try:
        r = requests.get(url, headers=_BROWSER_HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code == 200 and _response_is_sitemap_xml(r.text)
    except: return False

def _sitemap_urls_from_robots(origin: str, timeout: float = 15.0) -> List[str]:
    robots_url = origin.rstrip("/") + "/robots.txt"
    out = []
    try:
        r = requests.get(robots_url, headers=_BROWSER_HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            for line in r.text.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    u = line.split(":", 1)[1].strip()
                    if u.startswith("http"): out.append(u)
    except: pass
    return out

def resolve_store_to_sitemap_url(user_input: str) -> Tuple[Optional[str], str]:
    raw = (user_input or "").strip()
    if not raw: return None, "الرجاء إدخال رابط."
    if not raw.lower().startswith(("http://", "https://")): raw = "https://" + raw
    p = urlparse(raw)
    if not p.netloc: return None, "تعذر قراءة نطاق الرابط."
    if _looks_like_direct_sitemap_url(raw):
        if _probe_sitemap_url(raw): return raw, f"تم اعتماد الرابط مباشرة: `{raw}`"
    origin = _parse_origin(raw)
    if not origin: return None, "رابط المتجر غير صالح."
    from_robots = _sitemap_urls_from_robots(origin)
    for u in from_robots:
        if _probe_sitemap_url(u): return u, f"تم الاستنتاج من robots.txt: `{u}`"
    base = origin.rstrip("/")
    for u in [f"{base}/sitemap.xml", f"{base}/sitemap_products.xml", f"{base}/sitemap_index.xml"]:
        if _probe_sitemap_url(u): return u, f"تم الاستنتاج تلقائياً: `{u}`"
    return None, "لم يُعثر على Sitemap صالح."


# ══════════════════════════════════════════════
#  v27: دوال تنظيف البيانات المحسّنة
# ══════════════════════════════════════════════

def _clean_price(val) -> Optional[float]:
    """تنظيف السعر ليصبح float — يدعم أرقام عربية وصيغ متعددة"""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        p = float(val)
        return p if p > 0 else None
    s = str(val).strip()
    if not s:
        return None
    # إزالة رمز العملة والنصوص
    s = re.sub(r'[^\d.,٠-٩]', '', s)
    # تحويل أرقام عربية
    _AR_DIGITS = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
    for ar, en in _AR_DIGITS.items():
        s = s.replace(ar, en)
    s = s.replace(',', '')
    try:
        p = float(s)
        return p if p > 0 else None
    except (ValueError, TypeError):
        return None


def _clean_text(val) -> str:
    """تنظيف النص من المسافات الزائدة والأحرف غير المرئية"""
    if val is None:
        return ""
    s = str(val).strip()
    # إزالة الأحرف غير المرئية
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', s)
    # توحيد المسافات
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


# ── Scraper helper functions ──────────────────────────

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
        except: pass
    prev.update(updates)
    os.makedirs("data", exist_ok=True)
    with open(SCRAPER_PROGRESS_JSON, "w", encoding="utf-8") as f:
        json.dump(prev, f, ensure_ascii=False, indent=2)

def _save_competitor_csv_rows(rows: List[Dict[str, Any]]) -> int:
    """حفظ منتجات المنافسين في CSV مع تنظيف شامل"""
    if not rows:
        return 0
    # ── v27: تنظيف كل صف قبل الحفظ ──
    clean_rows = []
    for r in rows:
        name = _clean_text(r.get("name", ""))
        price = _clean_price(r.get("price"))
        if not name or price is None:
            continue
        clean_rows.append({
            "name":      name,
            "price":     round(price, 2),
            "brand":     _clean_text(r.get("brand", "")),
            "image_url": _clean_text(r.get("image_url", "")),
            "comp_url":  _clean_text(r.get("comp_url", "")),
            "sku":       _clean_text(r.get("sku", "")),
        })
    if not clean_rows:
        return 0

    _col_order = ["name", "price", "brand", "image_url", "comp_url", "sku"]
    df = pd.DataFrame(clean_rows).drop_duplicates(subset=["comp_url"])
    for c in _col_order:
        if c not in df.columns:
            df[c] = ""
    df = df[_col_order]
    os.makedirs("data", exist_ok=True)
    temp_file = "data/competitors_temp.csv"
    final_file = "data/competitors_latest.csv"
    # ── v27: أعمدة عربية واضحة مع "اسم المنتج" بدل "الاسم" ──
    df_ar = df.rename(columns={
        "name": "اسم المنتج",
        "price": "السعر",
        "brand": "الماركة",
        "image_url": "رابط_الصورة",
        "comp_url": "رابط_المنتج",
        "sku": "sku",
    })
    df_ar.to_csv(temp_file, index=False, encoding="utf-8-sig")
    shutil.move(temp_file, final_file)
    return len(df)

def _tag_local(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag

def _stable_sku_from_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def _extract_brand_from_product(data: dict) -> str:
    b = data.get("brand")
    if b is None: return ""
    if isinstance(b, str): return b.strip()
    if isinstance(b, dict):
        n = b.get("name") or b.get("@value")
        if isinstance(n, dict): n = n.get("value") or n.get("text")
        return str(n or "").strip()
    return str(b).strip()

def _extract_image_url_from_product(data: dict) -> str:
    img = data.get("image")
    if img is None: return ""
    if isinstance(img, str): return img.strip()
    if isinstance(img, dict):
        u = img.get("url") or img.get("contentUrl") or img.get("@id")
        return str(u or "").strip()
    if isinstance(img, list) and img:
        first = img[0]
        if isinstance(first, str): return first.strip()
        if isinstance(first, dict): return str(first.get("url") or first.get("contentUrl") or "").strip()
    return ""

def _filter_salla_like_product_urls(urls: List[str]) -> List[str]:
    out = []
    for u in urls:
        try:
            p = urlparse(u)
            host = (p.netloc or "").lower()
            if "cdn.salla.sa" in host or host.startswith("cdn."): continue
            path = p.path or ""
            if re.search(r"/p\d+$", path) or "/product/" in path or "/products/" in path:
                out.append(u)
        except: continue
    return list(dict.fromkeys(out))

def _parse_price_from_text(text: str) -> Optional[float]:
    """تحليل السعر من نص — v27 محسّن"""
    return _clean_price(text)

def _price_from_offers(offers: Any) -> Optional[float]:
    if offers is None: return None
    if isinstance(offers, list):
        if not offers: return None
        offers = offers[0]
    if not isinstance(offers, dict): return _clean_price(offers)
    p = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
    if p is None: return None
    return _clean_price(p)

def _is_product_type(t: Any) -> bool:
    if isinstance(t, list): return any(x in ("Product", "ProductGroup") for x in t)
    return t in ("Product", "ProductGroup")

def _first_product_node(obj: Any) -> Optional[dict]:
    if isinstance(obj, list):
        for x in obj:
            found = _first_product_node(x)
            if found: return found
    if isinstance(obj, dict):
        if _is_product_type(obj.get("@type")): return obj
        if "@graph" in obj:
            found = _first_product_node(obj["@graph"])
            if found: return found
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _first_product_node(v)
                if found: return found
    return None


class AsyncCompetitorScraper:
    def __init__(self, concurrency_limit: int = 15):
        self.concurrency_limit = max(1, int(concurrency_limit))
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Referer": referer or "https://www.google.com/",
            "Connection": "keep-alive",
        }

    async def scan_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str) -> tuple[List[str], Dict[str, Any]]:
        collected = []
        diag = {"http_status": None, "fetch_error": None}
        try:
            async with session.get(sitemap_url, timeout=180, headers=self._get_headers()) as resp:
                diag["http_status"] = resp.status
                if resp.status != 200: return [], diag
                text = await resp.text()
                root = ET.fromstring(text)
                root_local = _tag_local(root.tag)
                if root_local == "sitemapindex":
                    for el in root.iter():
                        if _tag_local(el.tag) == "loc" and el.text:
                            u = el.text.strip()
                            if u.startswith("http"):
                                sub, _ = await self.scan_sitemap(session, u)
                                collected.extend(sub)
                elif root_local == "urlset":
                    for el in root.iter():
                        if _tag_local(el.tag) == "loc" and el.text:
                            u = el.text.strip()
                            if u.startswith("http"): collected.append(u)
        except Exception as e: diag["fetch_error"] = str(e)
        return list(dict.fromkeys(collected)), diag

    async def fetch_product(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        """كشط منتج واحد — v27: تنظيف شامل للسعر والنصوص"""
        async with self.semaphore:
            try:
                async with session.get(url, timeout=30, headers=self._get_headers(url)) as resp:
                    if resp.status != 200: return None
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")

                    # ── 1. JSON-LD (المصدر الأساسي — Salla/Zid) ──
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            node = _first_product_node(json.loads(script.string))
                            if node:
                                raw_name = node.get("name")
                                raw_price = _price_from_offers(node.get("offers"))
                                if raw_name and raw_price:
                                    # v27: تنظيف فوري
                                    name = _clean_text(raw_name)
                                    price = _clean_price(raw_price)
                                    if not name or price is None or price <= 0:
                                        continue
                                    return {
                                        "name":      name,
                                        "price":     round(price, 2),
                                        "brand":     _clean_text(_extract_brand_from_product(node)),
                                        "image_url": _clean_text(_extract_image_url_from_product(node)),
                                        "comp_url":  url,
                                        "sku":       _clean_text(node.get("sku") or _stable_sku_from_url(url)),
                                    }
                        except: continue

                    # ── 2. Fallback: Meta tags ──
                    name_meta = soup.find("meta", property="og:title") or soup.find("title")
                    price_meta = soup.find("meta", property="product:price:amount") or soup.find("meta", property="og:price:amount")
                    if name_meta and price_meta:
                        name_text = name_meta.get("content") if hasattr(name_meta, "get") else name_meta.string
                        raw_price = price_meta.get("content") if hasattr(price_meta, "get") else None
                        name = _clean_text(name_text)
                        price = _clean_price(raw_price)
                        if name and price and price > 0:
                            img_meta = soup.find("meta", property="og:image")
                            return {
                                "name":      name,
                                "price":     round(price, 2),
                                "brand":     "",
                                "image_url": _clean_text(img_meta.get("content", "") if img_meta else ""),
                                "comp_url":  url,
                                "sku":       _stable_sku_from_url(url),
                            }
            except: pass
            return None


async def run_scraper(sitemap_urls: List[str], progress_callback=None):
    all_rows = []
    async with aiohttp.ClientSession() as session:
        scraper = AsyncCompetitorScraper(concurrency_limit=15)
        all_product_urls = []
        for s_url in sitemap_urls:
            if progress_callback: progress_callback(f"جاري فحص: {s_url}...", 0.05)
            resolved_url, _ = resolve_store_to_sitemap_url(s_url)
            target = resolved_url or s_url
            urls, _ = await scraper.scan_sitemap(session, target)
            all_product_urls.extend(_filter_salla_like_product_urls(urls))

        all_product_urls = list(dict.fromkeys(all_product_urls))
        total = len(all_product_urls)
        if total == 0: return 0

        for i, url in enumerate(all_product_urls):
            res = await scraper.fetch_product(session, url)
            if res: all_rows.append(res)
            if progress_callback and i % 5 == 0:
                progress_callback(f"تم كشط {len(all_rows)} منتج من {total}...", 0.1 + (0.8 * (i/total)))

    count = _save_competitor_csv_rows(all_rows)
    return count, all_rows
