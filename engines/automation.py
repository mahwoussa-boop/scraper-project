"""
engines/automation.py v28.1 — محرك الأتمتة الشامل (24 ساعة)
═══════════════════════════════════════════════════════════
✅ كشط ومقارنة تلقائية كل 24 ساعة
✅ استئناف ذكي للكشط (Resume) وحفظ الحالة
✅ توزيع النتائج على الأقسام (سعر أقل، مفقودة، مراجعة...)
✅ الحفاظ على الدوال القديمة لمنع ImportError
"""
import json
import time
import threading
import os
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# استيراد المكونات اللازمة
from scraper import run_scraper, load_scraper_state
from engines.engine import run_full_analysis, find_missing_products, read_file
from utils.db_manager import upsert_our_catalog, upsert_comp_catalog, save_job_progress

try:
    from config import (AUTOMATION_RULES_DEFAULT, AUTO_DECISION_CONFIDENCE,
                        AUTO_PUSH_TO_MAKE, AUTO_SEARCH_INTERVAL_MINUTES, DB_PATH)
except ImportError:
    AUTOMATION_RULES_DEFAULT = []
    AUTO_DECISION_CONFIDENCE = 92
    AUTO_PUSH_TO_MAKE = False
    AUTO_SEARCH_INTERVAL_MINUTES = 360
    DB_PATH = "perfume_pricing.db"

# ── 1. الدوال القديمة (للتوافق ومنع ImportError) ──────────────────────────

class PricingRule:
    def __init__(self, rule_dict: dict):
        self.name = rule_dict.get("name", "قاعدة بدون اسم")
        self.enabled = rule_dict.get("enabled", True)
        self.action = rule_dict.get("action", "keep")
        self.min_match_score = rule_dict.get("min_match_score", 90)
        self.params = rule_dict

    def evaluate(self, our_price: float, comp_price: float, match_score: float, cost_price: float = 0) -> Optional[Dict]:
        return None

class AutomationEngine:
    def __init__(self, rules: List[dict] = None):
        self.rules = [PricingRule(r) for r in (rules or AUTOMATION_RULES_DEFAULT)]
        self.decisions_log: List[dict] = []
        self._lock = threading.Lock()
    def evaluate_product(self, product_data: dict): return None
    def evaluate_batch(self, products_df: pd.DataFrame): return []
    def get_summary(self): return {"total": 0}

class ScheduledSearchManager:
    def __init__(self, interval_minutes: int = None):
        self.interval = timedelta(minutes=interval_minutes or AUTO_SEARCH_INTERVAL_MINUTES)
        self.last_run = None
    def should_run(self): return False
    def time_until_next(self): return "N/A"

def auto_push_decisions(decisions: List[Dict]) -> Dict:
    return {"success": True, "sent": 0}

def auto_process_review_items(review_df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame()

def log_automation_decision(decision, pushed=False): pass
def get_automation_log(limit=100): return []
def get_automation_stats(): return {"total": 0}

# ── 2. محرك الأتمتة الجديد (24 ساعة) ──────────────────────────

AUTOMATION_STATE_FILE = "data/automation_state.json"
DEFAULT_INTERVAL_HOURS = 24

class GlobalAutomationManager:
    """المدير المسؤول عن تشغيل الكشط والمقارنة في الخلفية"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(GlobalAutomationManager, cls).__new__(cls)
                cls._instance._init_manager()
            return cls._instance

    def _init_manager(self):
        self.is_running = False
        self.last_run_time = None
        self.next_run_time = None
        self.current_status = "خامل"
        self.progress = 0.0
        self.error_msg = None
        self.thread = None
        self.stop_event = threading.Event()
        self.load_state()

    def load_state(self):
        if os.path.exists(AUTOMATION_STATE_FILE):
            try:
                with open(AUTOMATION_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                    self.last_run_time = state.get("last_run")
                    if self.last_run_time:
                        last = datetime.fromisoformat(self.last_run_time)
                        self.next_run_time = (last + timedelta(hours=DEFAULT_INTERVAL_HOURS)).isoformat()
            except: pass

    def save_state(self):
        os.makedirs("data", exist_ok=True)
        state = {"last_run": self.last_run_time, "next_run": self.next_run_time, "status": self.current_status}
        with open(AUTOMATION_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def start_automation(self, sitemap_urls: List[str], our_file_path: str = None):
        with self._lock:
            if self.is_running: return False
            self.is_running = True
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._automation_loop, args=(sitemap_urls, our_file_path), daemon=True)
            self.thread.start()
            return True

    def _automation_loop(self, sitemap_urls, our_file_path):
        while not self.stop_event.is_set():
            now = datetime.now()
            should_run = False
            if not self.last_run_time: should_run = True
            else:
                last = datetime.fromisoformat(self.last_run_time)
                if now - last >= timedelta(hours=DEFAULT_INTERVAL_HOURS): should_run = True
            if should_run:
                try:
                    self._execute_full_cycle(sitemap_urls, our_file_path)
                    self.last_run_time = datetime.now().isoformat()
                    self.next_run_time = (datetime.now() + timedelta(hours=DEFAULT_INTERVAL_HOURS)).isoformat()
                    self.save_state()
                except Exception as e:
                    self.error_msg = str(e)
                    self.current_status = f"خطأ: {str(e)[:100]}"
            self.stop_event.wait(300)

    def _execute_full_cycle(self, sitemap_urls, our_file_path):
        self.current_status = "🕷️ جاري الكشط..."
        self.progress = 0.05
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        def scraper_progress(msg, p):
            self.current_status = msg
            self.progress = p * 0.5
        count, scraped_rows = loop.run_until_complete(run_scraper(sitemap_urls, progress_callback=scraper_progress))
        if not scraped_rows:
            self.current_status = "⚠️ لم يتم العثور على منتجات"
            return
        our_df = pd.DataFrame()
        if our_file_path and os.path.exists(our_file_path):
            self.current_status = "📂 قراءة ملف المتجر..."
            our_df, err = read_file(our_file_path)
        if our_df.empty:
            self.current_status = "✅ اكتمل الكشط (لا يوجد ملف للمقارنة)"
            return
        self.current_status = "🤖 جاري المقارنة..."
        self.progress = 0.6
        comp_df = pd.DataFrame(scraped_rows)
        comp_dfs = {"المنافسين": comp_df}
        upsert_our_catalog(our_df, name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر")
        upsert_comp_catalog(comp_dfs)
        analysis_df = run_full_analysis(our_df, comp_dfs)
        missing_df = find_missing_products(our_df, comp_dfs)
        self.current_status = "💾 حفظ النتائج..."
        self.progress = 0.9
        job_id = f"auto_{datetime.now().strftime('%Y%m%d_%H%M')}"
        from app import _safe_results_for_json
        safe_records = _safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []
        save_job_progress(job_id, len(our_df), len(our_df), safe_records, "done", os.path.basename(our_file_path), "Auto Scraper", missing=safe_missing)
        self.current_status = f"✅ اكتملت الدورة ({len(analysis_df)} منتج)"
        self.progress = 1.0

automation_manager = GlobalAutomationManager()
