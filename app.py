"""
app.py v46.0 — تطبيق مهووس (Mahwoos)
"""
import streamlit as st
import pandas as pd
import os, json

st.set_page_config(page_title="مهووس - مقارنة الأسعار", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;700&display=swap');
html, body, [class*="css"] { font-family: 'Cairo', sans-serif; direction: rtl; }
</style>
""", unsafe_allow_html=True)

os.makedirs("data", exist_ok=True)

# ── استيراد آمن ──────────────────────────────
try:
    from utils.db_manager import init_db, get_processed, save_processed, get_processed_keys
    init_db()
except Exception as e:
    st.error(f"خطأ في قاعدة البيانات: {e}")
    st.stop()

try:
    from engines.automation import automation_manager
    _automation_ok = True
except Exception as e:
    _automation_ok = False
    st.warning(f"محرك الأتمتة غير متاح: {e}")

try:
    from engines.engine import run_full_analysis, find_missing_products, read_file
    _engine_ok = True
except Exception as e:
    _engine_ok = False

# ── Session State ─────────────────────────────
if "results" not in st.session_state:
    st.session_state.results = {
        "price_higher": [],
        "price_lower": [],
        "missing": [],
        "approved": [],
        "review": [],
        "all": pd.DataFrame()
    }
if "our_df" not in st.session_state:
    path = "data/our_products.csv"
    if os.path.exists(path):
        try:
            st.session_state.our_df = pd.read_csv(path)
        except:
            st.session_state.our_df = pd.DataFrame()
    else:
        st.session_state.our_df = pd.DataFrame()

# ── تحديث تلقائي من محرك الأتمتة ─────────────
if _automation_ok and automation_manager.is_running:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=5000, key="auto_refresh")
    except:
        pass

    if not automation_manager.current_analysis_df.empty:
        df = automation_manager.current_analysis_df
        st.session_state.results["all"] = df
        st.session_state.results["price_higher"] = df[df["الحالة"] == "🔴 سعرنا أعلى"].to_dict("records")
        st.session_state.results["price_lower"]  = df[df["الحالة"] == "🟢 سعر أقل"].to_dict("records")
        st.session_state.results["approved"]     = df[df["الحالة"] == "✅ موافق عليها"].to_dict("records")
        st.session_state.results["review"]       = df[df["الحالة"] == "⚠️ تحت المراجعة"].to_dict("records")

    if not automation_manager.current_missing_df.empty:
        st.session_state.results["missing"] = automation_manager.current_missing_df.to_dict("records")

# ── دالة مساعدة لحفظ JSON ─────────────────────
def _safe_results_for_json(data):
    if isinstance(data, list):
        return [{k: (v if not hasattr(v, 'item') else v.item()) for k, v in row.items()} for row in data]
    return []

# ── بطاقة إحصائية ────────────────────────────
def stat_card(title, value, color):
    return f"""
    <div style="background:white;padding:15px;border-radius:10px;
                box-shadow:0 2px 6px rgba(0,0,0,0.08);
                border-right:5px solid {color};text-align:center;margin:5px 0;">
        <div style="font-size:.85rem;color:#666;font-weight:bold;">{title}</div>
        <div style="font-size:1.6rem;color:{color};font-weight:900;">{value}</div>
    </div>"""

# ── بطاقة مقارنة ──────────────────────────────
def vs_card(name, our_price, comp_price, comp_name, img_url, status):
    color = "#dc3545" if "أعلى" in str(status) else "#28a745" if "أقل" in str(status) else "#007bff"
    img = f'<img src="{img_url}" style="width:70px;height:70px;object-fit:contain;border-radius:6px;">' \
          if img_url and str(img_url) not in ["nan",""] else \
          '<div style="width:70px;height:70px;background:#f0f0f0;border-radius:6px;"></div>'
    return f"""
    <div style="background:white;padding:14px;border-radius:10px;margin-bottom:12px;
                box-shadow:0 2px 8px rgba(0,0,0,0.08);display:flex;align-items:center;
                gap:14px;direction:rtl;">
        {img}
        <div style="flex:1;">
            <div style="font-weight:bold;font-size:1rem;color:#222;margin-bottom:4px;">{name}</div>
            <div style="font-size:.85rem;color:#555;">المنافس: <b style="color:#007bff;">{comp_name}</b></div>
            <div style="display:flex;gap:20px;margin-top:6px;font-size:.9rem;">
                <span>سعرنا: <b>{our_price} ر.س</b></span>
                <span>سعر المنافس: <b style="color:{color};">{comp_price} ر.س</b></span>
            </div>
        </div>
        <div style="padding:5px 12px;border-radius:20px;background:{color}18;
                    color:{color};font-weight:bold;font-size:.8rem;border:1px solid {color};">
            {status}
        </div>
    </div>"""

# ── التنقل ────────────────────────────────────
PAGES = [
    "📊 لوحة التحكم",
    "📂 رفع الملفات",
    "🔴 سعرنا أعلى",
    "🟢 سعر أقل",
    "🔍 منتجات مفقودة",
    "✅ موافق عليها",
    "⚠️ تحت المراجعة",
    "✔️ تمت المعالجة",
    "⚙️ الإعدادات"
]

with st.sidebar:
    try:
        st.image("https://mahwous.com/cdn/shop/files/logo_mahwous.png", width=160)
    except:
        st.title("مهووس")
    st.markdown("---")
    page = st.radio("القائمة", PAGES, label_visibility="collapsed")
    st.markdown("---")
    if _automation_ok:
        st.caption(f"🤖 {automation_manager.current_status}")
    if st.button("🔄 تحديث"):
        st.rerun()

res = st.session_state.results

# ══════════════════════════════════════════════
# لوحة التحكم
# ══════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.title("📊 لوحة تحكم مهووس")
    processed = get_processed(limit=1000)

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.markdown(stat_card("🔴 سعرنا أعلى", len(res["price_higher"]), "#dc3545"), unsafe_allow_html=True)
    c2.markdown(stat_card("🟢 سعر أقل",    len(res["price_lower"]),  "#28a745"), unsafe_allow_html=True)
    c3.markdown(stat_card("🔍 مفقودة",     len(res["missing"]),      "#17a2b8"), unsafe_allow_html=True)
    c4.markdown(stat_card("✅ موافق",       len(res["approved"]),     "#007bff"), unsafe_allow_html=True)
    c5.markdown(stat_card("⚠️ مراجعة",     len(res["review"]),       "#ffc107"), unsafe_allow_html=True)
    c6.markdown(stat_card("✔️ معالجة",     len(processed),           "#6c757d"), unsafe_allow_html=True)

    st.markdown("---")
    if not res["all"].empty:
        st.subheader("🆕 أحدث النتائج")
        st.dataframe(res["all"].head(20), use_container_width=True)
    else:
        st.info("📡 في انتظار بيانات المقارنة... ارفع ملف متجرنا أولاً من قسم 'رفع الملفات'.")

# ══════════════════════════════════════════════
# رفع الملفات
# ══════════════════════════════════════════════
elif page == "📂 رفع الملفات":
    st.title("📂 رفع ملف متجر مهووس")

    uploaded = st.file_uploader("اختر ملف CSV أو Excel", type=["csv","xlsx"])
    if uploaded:
        if not _engine_ok:
            st.error("محرك المقارنة غير متاح.")
        else:
            df, err = read_file(uploaded)
            if err:
                st.error(f"خطأ في قراءة الملف: {err}")
            elif df.empty:
                st.warning("الملف فارغ.")
            else:
                df.to_csv("data/our_products.csv", index=False, encoding="utf-8-sig")
                st.session_state.our_df = df
                st.success(f"✅ تم رفع {len(df):,} منتج بنجاح!")
                st.dataframe(df.head(5), use_container_width=True)

    if not st.session_state.our_df.empty:
        st.info(f"📦 الملف الحالي: {len(st.session_state.our_df):,} منتج محمّل.")

# ══════════════════════════════════════════════
# 🔴 سعرنا أعلى
# ══════════════════════════════════════════════
elif page == "🔴 سعرنا أعلى":
    st.title("🔴 منتجات سعرنا فيها أعلى من المنافسين")
    items = res["price_higher"]
    if items:
        st.caption(f"إجمالي: {len(items)} منتج")
        for item in items:
            st.markdown(vs_card(
                item.get("اسم المنتج",""),
                item.get("سعرنا", 0),
                item.get("سعر المنافس", 0),
                item.get("المنافس",""),
                item.get("رابط الصورة",""),
                item.get("الحالة","🔴")
            ), unsafe_allow_html=True)
            key = str(item.get("sku", item.get("اسم المنتج","")))
            if st.button(f"✔️ أرشفة", key=f"h_{key}"):
                save_processed(key, item.get("اسم المنتج",""), item.get("المنافس",""),
                               "أرشفة", item.get("سعرنا",0), item.get("سعر المنافس",0), key)
                st.rerun()
    else:
        st.success("ممتاز! لا توجد منتجات بسعر أعلى من المنافسين حالياً.")

# ══════════════════════════════════════════════
# 🟢 سعر أقل
# ══════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.title("🟢 منتجات المنافسين بأسعار أقل منا")
    items = res["price_lower"]
    if items:
        st.caption(f"إجمالي: {len(items)} منتج")
        for item in items:
            st.markdown(vs_card(
                item.get("اسم المنتج",""),
                item.get("سعرنا", 0),
                item.get("سعر المنافس", 0),
                item.get("المنافس",""),
                item.get("رابط الصورة",""),
                item.get("الحالة","🟢")
            ), unsafe_allow_html=True)
            key = str(item.get("sku", item.get("اسم المنتج","")))
            if st.button(f"✔️ أرشفة", key=f"l_{key}"):
                save_processed(key, item.get("اسم المنتج",""), item.get("المنافس",""),
                               "أرشفة", item.get("سعرنا",0), item.get("سعر المنافس",0), key)
                st.rerun()
    else:
        st.info("لا توجد منتجات بأسعار أقل حالياً.")

# ══════════════════════════════════════════════
# 🔍 منتجات مفقودة
# ══════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.title("🔍 منتجات عند المنافسين وغير موجودة عندنا")
    items = res["missing"]
    if items:
        st.caption(f"إجمالي: {len(items)} منتج")
        for item in items:
            st.markdown(vs_card(
                item.get("اسم المنتج",""),
                0,
                item.get("سعر المنافس", 0),
                item.get("المنافس",""),
                item.get("رابط الصورة",""),
                "🔍 مفقود"
            ), unsafe_allow_html=True)
    else:
        st.info("لا توجد منتجات مفقودة حالياً.")

# ══════════════════════════════════════════════
# ✅ موافق عليها
# ══════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.title("✅ منتجات بأسعار منافسة")
    items = res["approved"]
    if items:
        st.caption(f"إجمالي: {len(items)} منتج")
        for item in items:
            st.markdown(vs_card(
                item.get("اسم المنتج",""),
                item.get("سعرنا", 0),
                item.get("سعر المنافس", 0),
                item.get("المنافس",""),
                item.get("رابط الصورة",""),
                item.get("الحالة","✅")
            ), unsafe_allow_html=True)
    else:
        st.info("لا توجد منتجات موافق عليها بعد.")

# ══════════════════════════════════════════════
# ⚠️ تحت المراجعة
# ══════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.title("⚠️ منتجات تحت المراجعة")
    items = res["review"]
    if items:
        st.caption(f"إجمالي: {len(items)} منتج")
        for item in items:
            st.markdown(vs_card(
                item.get("اسم المنتج",""),
                item.get("سعرنا", 0),
                item.get("سعر المنافس", 0),
                item.get("المنافس",""),
                item.get("رابط الصورة",""),
                item.get("الحالة","⚠️")
            ), unsafe_allow_html=True)
    else:
        st.info("لا توجد منتجات تحت المراجعة.")

# ══════════════════════════════════════════════
# ✔️ تمت المعالجة
# ══════════════════════════════════════════════
elif page == "✔️ تمت المعالجة":
    st.title("✔️ أرشيف المنتجات المعالجة")
    processed = get_processed(limit=500)
    if processed:
        st.caption(f"إجمالي: {len(processed)} منتج")
        df_p = pd.DataFrame(processed)
        st.dataframe(df_p, use_container_width=True)
    else:
        st.info("لا توجد منتجات مؤرشفة بعد.")

# ══════════════════════════════════════════════
# ⚙️ الإعدادات
# ══════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    st.title("⚙️ إعدادات النظام")
    st.write("**الإصدار:** v46.0")
    st.write(f"**ملف متجرنا:** {'محمّل ✅' if not st.session_state.our_df.empty else 'غير محمّل ❌'}")
    if _automation_ok:
        st.write(f"**حالة الأتمتة:** {automation_manager.current_status}")
        if st.button("🔄 إعادة تشغيل الأتمتة"):
            automation_manager.stop_event.set()
            st.success("تم إيقاف الأتمتة. أعد تشغيل التطبيق.")
