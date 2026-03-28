"""
app.py v45.1 — تطبيق مهووس (Mahwoos App)
══════════════════════════════════════════════
✅ إدارة الأتمتة والبحث المستمر 24 ساعة
✅ عرض ذكي للمنتجات والمقارنات
✅ دعم 6 أقسام رئيسية للأسعار والمفقودات
"""
import streamlit as st
import pandas as pd
import os, time
from engines.automation import automation_manager
from utils.db_manager import get_processed, save_processed
from utils.ui_components import stat_card, vs_card, product_card

st.set_page_config(page_title="لوحة تحكم مهووس", layout="wide", initial_sidebar_state="expanded")

# CSS مخصص لتحسين المظهر
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Cairo', sans-serif; text-align: right; direction: rtl; }
    .stMetric { background: #f8f9fa; padding: 15px; border-radius: 10px; border-right: 5px solid #007bff; }
    div[data-testid="stSidebarNav"] { display: none; }
    .main { background-color: #f5f7f9; }
    .stButton>button { width: 100%; border-radius: 5px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# إنشاء المجلدات المطلوبة
os.makedirs("data", exist_ok=True)

# ── Session State ─────────────────────────
if "results" not in st.session_state:
    st.session_state.results = {"price_lower": [], "price_higher": [], "missing": [], "approved": [], "review": [], "processed": [], "all": pd.DataFrame()}
if "our_df" not in st.session_state:
    OUR_PRODUCTS_PATH = "data/our_products.csv"
    if os.path.exists(OUR_PRODUCTS_PATH):
        try:
            st.session_state.our_df = pd.read_csv(OUR_PRODUCTS_PATH)
            automation_manager.set_our_products(st.session_state.our_df)
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
        st.session_state.results["price_higher"] = df[df["الحالة"] == "🔴 سعرنا أعلى"].to_dict('records')
        st.session_state.results["approved"] = df[df["الحالة"] == "✅ موافق عليها"].to_dict('records')
        st.session_state.results["review"] = df[df["الحالة"] == "⚠️ تحت المراجعة"].to_dict('records')
    
    if not automation_manager.current_missing_df.empty:
        st.session_state.results["missing"] = automation_manager.current_missing_df.to_dict('records')
    
    # جلب الأرشيف (تمت المعالجة)
    st.session_state.results["processed"] = get_processed(limit=200)

# ── التنقل ───────────────────────────────
PAGES = ["📊 لوحة التحكم", "📂 رفع الملفات", "🔴 سعرنا أعلى", "🟢 سعر أقل", "🔍 منتجات مفقودة", "✅ موافق عليها", "⚠️ تحت المراجعة", "✔️ تمت المعالجة", "⚙️ الإعدادات"]
with st.sidebar:
    st.image("https://mahwous.com/cdn/shop/files/logo_mahwous.png", width=180)
    st.markdown("---")
    page = st.radio("القائمة الرئيسية", PAGES)
    st.markdown("---")
    st.info(f"🤖 الحالة: {automation_manager.current_status}")
    if st.button("🔄 تحديث يدوي للبيانات"):
        st.rerun()

# ── لوحة التحكم ───────────────────────────
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    
    # الإحصائيات
    res = st.session_state.results
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.markdown(stat_card("🔴 سعر أعلى", len(res["price_higher"]), "#dc3545"), unsafe_allow_html=True)
    c2.markdown(stat_card("🟢 سعر أقل", len(res["price_lower"]), "#28a745"), unsafe_allow_html=True)
    c3.markdown(stat_card("🔍 مفقودة", len(res["missing"]), "#17a2b8"), unsafe_allow_html=True)
    c4.markdown(stat_card("✅ موافق", len(res["approved"]), "#007bff"), unsafe_allow_html=True)
    c5.markdown(stat_card("⚠️ مراجعة", len(res["review"]), "#ffc107"), unsafe_allow_html=True)
    c6.markdown(stat_card("✔️ معالجة", len(res["processed"]), "#6c757d"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # عرض البطاقات الذكية لأحدث النتائج
    st.subheader("🆕 أحدث المنتجات المكتشفة")
    if not st.session_state.results["all"].empty:
        recent = st.session_state.results["all"].head(12)
        cols = st.columns(4)
        for i, (_, item) in enumerate(recent.iterrows()):
            with cols[i % 4]:
                st.markdown(product_card(
                    item["اسم المنتج"], item["سعرنا"], item["سعر المنافس"], 
                    item["المنافس"], item["رابط الصورة"], item["الحالة"], item["نسبة التشابه"]
                ), unsafe_allow_html=True)
    else:
        st.info("جاري سحب البيانات والمقارنة... ستظهر البطاقات هنا فوراً.")

# ── قسم سعرنا أعلى ───────────────────────────
elif page == "🔴 سعرنا أعلى":
    st.header("🔴 منتجات سعرنا فيها أعلى من المنافسين")
    items = st.session_state.results["price_higher"]
    if items:
        for item in items:
            st.markdown(vs_card(
                item["اسم المنتج"], item["سعرنا"], item["سعر المنافس"], 
                item["المنافس"], item["رابط الصورة"], item["الحالة"]
            ), unsafe_allow_html=True)
            if st.button(f"✔️ تمت المعالجة (أرشفة) - {item['sku']}", key=f"proc_h_{item['sku']}"):
                save_processed(str(item['sku']), item['اسم المنتج'], item['المنافس'], "أرشفة يدوية", item['سعرنا'], item['سعر المنافس'], str(item['sku']))
                st.rerun()
    else:
        st.success("ممتاز! لا توجد منتجات بسعر أعلى من المنافسين.")

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
            if st.button(f"✔️ تمت المعالجة (أرشفة) - {item['sku']}", key=f"proc_l_{item['sku']}"):
                save_processed(str(item['sku']), item['اسم المنتج'], item['المنافس'], "أرشفة يدوية", item['سعرنا'], item['سعر المنافس'], str(item['sku']))
                st.rerun()
    else:
        st.success("لا توجد منتجات بأسعار أقل حالياً.")

# ── قسم تمت المعالجة ───────────────────────────
elif page == "✔️ تمت المعالجة":
    st.header("✔️ أرشيف المنتجات المعالجة")
    items = st.session_state.results["processed"]
    if items:
        df_p = pd.DataFrame(items)
        st.dataframe(df_p, use_container_width=True)
    else:
        st.info("لا توجد منتجات مؤرشفة بعد.")

# ── قسم منتجات مفقودة ────────────────────────
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات متوفرة عند المنافسين وغير متوفرة عندنا")
    items = st.session_state.results["missing"]
    if items:
        for item in items:
            st.markdown(vs_card(
                item["اسم المنتج"], 0, item["سعر المنافس"], 
                item["المنافس"], item["رابط الصورة"], item["الحالة"]
            ), unsafe_allow_html=True)
    else:
        st.info("جاري البحث عن فرص مفقودة...")

# ── رفع الملفات ─────────────────────────────
elif page == "📂 رفع الملفات":
    st.header("📂 إدارة ملفات المنتجات")
    uploaded_file = st.file_uploader("ارفع ملف متجر مهووس (CSV/Excel)", type=["csv", "xlsx"])
    if uploaded_file:
        from engines.engine import read_file
        df, err = read_file(uploaded_file)
        if err: st.error(err)
        else:
            df.to_csv("data/our_products.csv", index=False)
            st.session_state.our_df = df
            automation_manager.set_our_products(df)
            st.success("تم تحديث قاعدة بيانات متجرنا بنجاح!")
            st.rerun()

# ── الإعدادات ───────────────────────────────
elif page == "⚙️ الإعدادات":
    st.header("⚙️ إعدادات النظام")
    st.write(f"إصدار التطبيق: v45.1")
    if st.button("🚀 إعادة تشغيل محرك الأتمتة"):
        automation_manager.stop()
        automation_manager.start()
        st.success("تمت إعادة التشغيل.")
