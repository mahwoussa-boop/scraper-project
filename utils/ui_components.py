import streamlit as st

def stat_card(title, value, color="#007bff"):
    """بطاقة إحصائية بسيطة وجذابة"""
    return f"""
    <div style="
        background-color: white; 
        padding: 15px; 
        border-radius: 10px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border-right: 5px solid {color};
        text-align: center;
        margin-bottom: 10px;
    ">
        <div style="font-size: 0.9rem; color: #6c757d; font-weight: bold;">{title}</div>
        <div style="font-size: 1.5rem; color: {color}; font-weight: 800;">{value}</div>
    </div>
    """

def vs_card(name, our_price, comp_price, comp_name, img_url, status):
    """بطاقة مقارنة للمنتج الواحد مع المنافس"""
    color = "#dc3545" if "أعلى" in status else "#28a745" if "أقل" in status else "#007bff"
    img_html = f'<img src="{img_url}" style="width:80px; height:80px; object-fit:contain; border-radius:5px;">' if img_url and str(img_url) != "nan" else '<div style="width:80px; height:80px; background:#eee; border-radius:5px; display:flex; align-items:center; justify-content:center; color:#999;">صورة</div>'
    
    return f"""
    <div style="
        background: white; 
        padding: 15px; 
        border-radius: 10px; 
        margin-bottom: 15px; 
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        display: flex;
        align-items: center;
        gap: 15px;
        direction: rtl;
    ">
        {img_html}
        <div style="flex-grow: 1;">
            <div style="font-weight: bold; font-size: 1.1rem; color: #333;">{name}</div>
            <div style="font-size: 0.9rem; color: #666;">المنافس: <span style="color:#007bff; font-weight:bold;">{comp_name}</span></div>
            <div style="display: flex; gap: 20px; margin-top: 5px;">
                <div>سعرنا: <span style="font-weight:bold; color:#333;">{our_price} ريال</span></div>
                <div>سعر المنافس: <span style="font-weight:bold; color:{color};">{comp_price} ريال</span></div>
            </div>
        </div>
        <div style="
            padding: 5px 15px; 
            border-radius: 20px; 
            background: {color}15; 
            color: {color}; 
            font-weight: bold; 
            font-size: 0.85rem;
            border: 1px solid {color};
        ">
            {status}
        </div>
    </div>
    """

def product_card(name, our_price, comp_price, comp_name, img_url, status, score):
    """بطاقة عرض المنتج في لوحة التحكم"""
    color = "#dc3545" if "أعلى" in status else "#28a745" if "أقل" in status else "#007bff"
    img_html = f'<img src="{img_url}" style="width:100%; height:150px; object-fit:contain; margin-bottom:10px;">' if img_url and str(img_url) != "nan" else '<div style="width:100%; height:150px; background:#f8f9fa; display:flex; align-items:center; justify-content:center; color:#ccc; margin-bottom:10px;">لا توجد صورة</div>'
    
    return f"""
    <div style="
        background: white; 
        padding: 15px; 
        border-radius: 12px; 
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        text-align: center;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    ">
        <div>
            {img_html}
            <div style="font-size: 0.85rem; font-weight: bold; color: #333; margin-bottom: 10px; height: 40px; overflow: hidden;">{name}</div>
        </div>
        <div>
            <div style="font-size: 0.8rem; color: #777;">{comp_name} | {score} تطابق</div>
            <div style="display: flex; justify-content: space-around; margin: 10px 0;">
                <div style="font-size: 0.9rem;">مهووس: <br><b>{our_price}</b></div>
                <div style="font-size: 0.9rem;">المنافس: <br><b style="color:{color};">{comp_price}</b></div>
            </div>
            <div style="
                font-size: 0.75rem; 
                padding: 3px; 
                border-radius: 5px; 
                background: {color}; 
                color: white; 
                font-weight: bold;
            ">
                {status}
            </div>
        </div>
    </div>
    """
