"""
app_patch_v27.py — سكريبت ترقيع app.py v27.0
═══════════════════════════════════════════════
يطبّق 3 تعديلات جراحية على app.py:
  1. _split_results: توثيق واضح لـ 6 أقسام
  2. upsert_our_catalog: كشف مرن لأعمدة ملفنا
  3. قراءة CSV المكشوط: توافق أسماء الأعمدة

الاستخدام:
  python app_patch_v27.py
  
أو ضعه بجانب app.py وشغّله — يعدّل app.py مباشرة مع حفظ نسخة احتياطية.
"""
import os
import shutil
import sys

APP_FILE = "app.py"
BACKUP_FILE = "app.py.backup_v26"


def apply_patches():
    if not os.path.exists(APP_FILE):
        print(f"❌ الملف {APP_FILE} غير موجود!")
        print("   ضع هذا السكريبت في نفس مجلد app.py وأعد التشغيل")
        sys.exit(1)

    # نسخة احتياطية
    if not os.path.exists(BACKUP_FILE):
        shutil.copy2(APP_FILE, BACKUP_FILE)
        print(f"💾 نسخة احتياطية: {BACKUP_FILE}")

    with open(APP_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    original_len = len(content)
    patches_applied = 0

    # ════════════════════════════════════════════════
    # PATCH 1: _split_results — توثيق 6 أقسام
    # ════════════════════════════════════════════════
    old_1 = '''def _split_results(df):
    """تقسيم نتائج التحليل على الأقسام بأمان تام"""
    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))
    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved":    df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review":      df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "all":         df,
    }'''

    new_1 = '''def _split_results(df):
    """
    تقسيم نتائج التحليل على 6 أقسام:
    ─────────────────────────────────────
    🔴 سعر أعلى:      سعرنا أعلى من المنافس (فرق > عتبة السعر)
    🟢 سعر أقل:       سعرنا أقل من المنافس (فرصة رفع سعر)
    ✅ موافق عليها:    السعر متطابق أو الفرق مقبول جداً
    🔍 منتجات مفقودة: تُضاف منفصلة من find_missing_products()
    ⚠️ تحت المراجعة:  نسبة المطابقة بين 60% و 84% — تحتاج مراجعة بشرية أو ذكاء اصطناعي
    ✔️ تمت المعالجة:  تُقرأ من قاعدة البيانات (processed_products)
    """
    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))
    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved":    df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review":      df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "all":         df,
    }'''

    if old_1 in content:
        content = content.replace(old_1, new_1)
        patches_applied += 1
        print("✅ PATCH 1: _split_results — توثيق 6 أقسام")
    else:
        print("⚠️  PATCH 1: _split_results — لم يُعثر على النص الأصلي (قد يكون مُعدّلاً مسبقاً)")

    # ════════════════════════════════════════════════
    # PATCH 2: upsert_our_catalog — كشف مرن للأعمدة
    # ════════════════════════════════════════════════
    old_2 = '''                    with st.spinner("📦 تحديث الكتالوج اليومي..."):
                        r_our  = upsert_our_catalog(our_df,
                            name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر")'''

    new_2 = '''                    # ── v27: كشف مرن لأعمدة ملفنا (يدعم أسم/اسم، سعر المنتج/السعر) ──
                    from engines.engine import _fcol as _find_col
                    _our_name_c = _find_col(our_df, ["أسم المنتج","اسم المنتج","المنتج","Product","Name"])
                    _our_id_c   = _find_col(our_df, ["رمز المنتج sku","رقم المنتج","SKU","sku","معرف المنتج","product_id"])
                    _our_price_c= _find_col(our_df, ["سعر المنتج","السعر","Price","price"])
                    with st.spinner("📦 تحديث الكتالوج اليومي..."):
                        r_our  = upsert_our_catalog(our_df,
                            name_col=_our_name_c, id_col=_our_id_c, price_col=_our_price_c)'''

    if old_2 in content:
        content = content.replace(old_2, new_2)
        patches_applied += 1
        print("✅ PATCH 2: upsert_our_catalog — كشف مرن للأعمدة")
    else:
        print("⚠️  PATCH 2: upsert_our_catalog — لم يُعثر على النص الأصلي")

    # ════════════════════════════════════════════════
    # PATCH 3: توافق أعمدة CSV المكشوط
    # ════════════════════════════════════════════════
    old_3 = '''                    comp_df = pd.read_csv(comp_latest_path)
                    comp_dfs = {"المنافسين_المكشوطين": comp_df}'''

    new_3 = '''                    comp_df = pd.read_csv(comp_latest_path)
                    # ── v27: توافق أعمدة الكشط مع المحرك ──
                    # السكريبر يخرج: اسم المنتج, السعر, الماركة, رابط_الصورة, رابط_المنتج, sku
                    if "الاسم" in comp_df.columns and "اسم المنتج" not in comp_df.columns:
                        comp_df = comp_df.rename(columns={"الاسم": "اسم المنتج"})
                    comp_dfs = {"المنافسين_المكشوطين": comp_df}'''

    if old_3 in content:
        content = content.replace(old_3, new_3)
        patches_applied += 1
        print("✅ PATCH 3: توافق أعمدة CSV المكشوط")
    else:
        print("⚠️  PATCH 3: لم يُعثر على النص الأصلي")

    # ════════════════════════════════════════════════
    # حفظ الملف المعدّل
    # ════════════════════════════════════════════════
    if patches_applied > 0:
        # تحديث رقم الإصدار في التعليق العلوي
        content = content.replace("v26.0", "v27.0", 1)

        with open(APP_FILE, "w", encoding="utf-8") as f:
            f.write(content)

        new_len = len(content)
        print(f"\n{'='*50}")
        print(f"✅ تم تطبيق {patches_applied}/3 ترقيع على {APP_FILE}")
        print(f"   الحجم: {original_len:,} → {new_len:,} حرف")
        print(f"   النسخة الاحتياطية: {BACKUP_FILE}")
    else:
        print(f"\n⚠️ لم يتم تطبيق أي ترقيع — تحقق من محتوى {APP_FILE}")


if __name__ == "__main__":
    apply_patches()
