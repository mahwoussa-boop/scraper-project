"""
app.py - نظام التسعير الذكي مهووس v42.0
═══════════════════════════════════════════════════════════
✅ المعالج الشامل: بطاقات منتجات ذكية (Product Cards)
✅ عرض حي وتلقائي للأقسام (سعر أقل، مفقودة، إلخ)
✅ مطابقة الحجم والتركيز والنوع بدقة 100%
✅ أتمتة كاملة في الخلفية مع حفظ الحالة
"""
import streamlit as st
import pandas as pd
import threading
import time
import uuid
import os
from datetime import datetime

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
from styles import get_styles, stat_card, vs_card, comp_strip, miss_card, get_sidebar_toggle_js
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             extract_brand, extract_size, extract_type, is_sample)
from engines.automation import automation_manager
from utils.db_manager import save_processed, get_processed, undo_processed, get_processed_keys
from utils.helpers import safe_float, format_price, format_diff

# ── إعداد الصفحة ──────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)

# إنشاء المجلدات المطلوبة
os.makedirs("data", exist_ok=True)

# ── Session State ─────────────────────────
if "results" not in st.session_state:
    st.session_state.results = {"price_lower": [], "missing": [], "approved": [], "review": [], "processed": [], "all": pd.DataFrame()}
if "our_df" not in st.session_state:
    OUR_PRODUCTS_PATH = "data/our_products.csv"
    if os.path.exists(OUR_PRODUCTS_PATH):
        try:
            df, err = read_file(OUR_PRODUCTS_PATH)
            if not err: st.session_state.our_df = df
        except: pass

# ── التحديث التلقائي ───────────────────────
if automation_manager.is_running:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=5000, key="auto_refresh")
    
    # تحديث النتائج من المحرك
    if not automation_manager.current_analysis_df.empty:
            df = automation_manager.current_analysis_df
            st.session_state.results["all"] = df
            st.session_state.results["price_lower"] = df[df["الحالة"] == "🟢 سعر أقل"].to_dict('records')
            st.session_state.results["approved"] = df[df["الحالة"] == "✅ موافق عليها"].to_dict('records')
            st.session_state.results["review"] = df[df["الحالة"] == "⚠️ تحت المراجعة"].to_dict('records')
        
        if not automation_manager.current_missing_df.empty:
            st.session_state.results["missing"] = automation_manager.current_missing_df.to_dict('records')
        
        # جلب الأرشيف (تمت المعالجة)
        st.session_state.results["processed"] = get_processed(limit=200)

# ── التنقل ───────────────────────────────
PAGES = ["📊 لوحة التحكم", "📂 رفع الملفات", "🟢 سعر أقل", "🔍 منتجات مفقودة", "✅ موافق عليها", "⚠️ تحت المراجعة", "✔️ تمت المعالجة", "⚙️ الإعدادات"]
with st.sidebar:
    st.image("https://mahwous.com/cdn/shop/files/logo_mahwous.png", width=180)
    st.markdown("---")
    page = st.radio("القائمة الرئيسية", PAGES)
    st.markdown("---")
    st.info(f"🤖 الحالة: {automation_manager.current_status}")
    if automation_manager.is_running:
        st.progress(automation_manager.progress)
        if st.button("🛑 إيقاف الأتمتة"):
            automation_manager.stop_automation()
            st.rerun()
    else:
        if st.button("🚀 بدء الأتمتة"):
            automation_manager.start_automation(["https://saeedsalah.com/sitemap.xml"])
            st.rerun()

# ── لوحة التحكم ───────────────────────────
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    
    # الإحصائيات
    res = st.session_state.results
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(stat_card("🟢 سعر أقل", len(res["price_lower"]), "#28a745"), unsafe_allow_html=True)
    c2.markdown(stat_card("🔍 مفقودة", len(res["missing"]), "#17a2b8"), unsafe_allow_html=True)
    c3.markdown(stat_card("✅ موافق", len(res["approved"]), "#007bff"), unsafe_allow_html=True)
    c4.markdown(stat_card("⚠️ مراجعة", len(res["review"]), "#ffc107"), unsafe_allow_html=True)
    c5.markdown(stat_card("✔️ معالجة", len(res["processed"]), "#6c757d"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # عرض البطاقات الذكية لأحدث النتائج
    st.subheader("✨ أحدث المقارنات الذكية")
    if not res["all"].empty:
        latest = res["all"].head(6)
        cols = st.columns(3)
        for i, (_, row) in enumerate(latest.iterrows()):
            with cols[i % 3]:
                # استخدام vs_card من styles.py لعرض المقارنة بجمالية
                st.markdown(vs_card(
                    row["اسم المنتج"], 
                    row["سعرنا"], 
                    row["سعر المنافس"], 
                    row["المنافس"],
                    row["رابط الصورة"],
                    row["الحالة"]
                ), unsafe_allow_html=True)
    else:
        st.info("جاري سحب البيانات والمقارنة... ستظهر البطاقات هنا فوراً.")

# ── قسم سعر أقل ───────────────────────────
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات بأسعار أقل عند المنافسين")
    items = st.session_state.results["price_lower"]
    if items:
        for item in items:
            st.markdown(vs_card(
                item["اسم المنتج"], item["سعرنا"], item["سعر المنافس"], 
                item["المنافس"], item["رابط الصورة"], item["الحالة"]
            ), unsafe_allow_html=True)
            if st.button(f"✔️ تمت المعالجة (أرشفة) - {item['sku']}", key=f"proc_{item['sku']}"):
                save_processed(str(item['sku']), item['اسم المنتج'], item['المنافس'], "أرشفة يدوية", item['سعرنا'], item['سعر المنافس'], str(item['sku']))
                st.rerun()
    else:
        st.success("لا توجد منتجات بأسعار أقل حالياً.")

# ── قسم تمت المعالجة ───────────────────────────
elif page == "✔️ تمت المعالجة":
    st.header("✔️ أرشيف المنتجات المعالجة")
    items = st.session_state.results["processed"]
    if items:
        for item in items:
            with st.container():
                c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
                c1.write(f"**{item['product_name']}**")
                c2.write(f"المنافس: {item['competitor']}")
                c3.write(f"الإجراء: {item['action']}")
                if c4.button("🔄 تراجع", key=f"undo_{item['product_key']}"):
                    undo_processed(item['product_key'])
                    st.rerun()
                st.markdown("---")
    else:
        st.info("الأرشيف فارغ حالياً.")

# ── قسم المنتجات المفقودة ──────────────────
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات مفقودة في متجرنا")
    items = st.session_state.results["missing"]
    if items:
        cols = st.columns(3)
        for i, item in enumerate(items):
            with cols[i % 3]:
                st.markdown(miss_card(
                    item["اسم المنتج"], item["سعر المنافس"], 
                    item["المنافس"], item["رابط الصورة"]
                ), unsafe_allow_html=True)
    else:
        st.info("لا توجد منتجات مفقودة مكتشفة بعد.")

# ── رفع الملفات ───────────────────────────
elif page == "📂 رفع الملفات":
    st.header("📂 إدارة ملفات المتجر")
    our_file = st.file_uploader("📦 ارفع ملف متجر مهووس (CSV/Excel)", type=["csv","xlsx","xls"])
    
    if "our_df" in st.session_state and st.session_state.our_df is not None:
        st.success(f"✅ ملف المتجر محمل: {len(st.session_state.our_df)} منتج")
        if st.button("🗑️ حذف الملف"):
            if os.path.exists("data/our_products.csv"): os.remove("data/our_products.csv")
            del st.session_state.our_df
            st.rerun()
            
    if our_file:
        df, err = read_file(our_file)
        if not err:
            st.session_state.our_df = df
            df.to_csv("data/our_products.csv", index=False)
            st.success("تم حفظ الملف! تبدأ المقارنة الآن...")
            automation_manager.start_automation(["https://saeedsalah.com/sitemap.xml"])
            st.rerun()

else:
    st.info("هذا القسم قيد التحديث ليتناسب مع المعالج الشامل.")
