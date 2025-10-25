
# app.py
# Streamlit MVP cho "Track hàng dự án" — theo spec đã lock với bạn
# Tabs:
#   1) Projects (Viewer & Editor) — bảng + filter + header "Latest update S4"
#   2) Add Project (Editor) — Form/Paste/Import với bộ bắt buộc: DGW PIC, ASUS PIC, Mã hàng, Số lượng, Đơn giá FV, SI, EU
#   3) Editor Tools (Editor) — Quick PI, Import Logistics (map chuẩn), Status Update (By Project & Bulk)
#
# Lưu ý:
# - Không có panel chi tiết
# - Import Logistics: match theo PI (map từ "Hợp đồng/ Số PO NK"), ô trống KHÔNG ghi đè, duplicate PI trong cùng file => last-write-wins
# - Sau import, KHÔNG hiện summary; chỉ thông báo “Import thành công”
# - Chỉ cập nhật "Latest update S4" (timestamp import gần nhất) ở Tab 1; không đổi "last_updated" từng dòng dự án
# - Bulk Status: filter chỉ Bill / Số lô / Số tờ khai

import streamlit as st
import pandas as pd
import sqlite3
from sqlite3 import Connection
from datetime import datetime
import io
import os

st.set_page_config(page_title="Track hàng dự án", layout="wide")

DB_PATH = "data.db"

# ========= DB =========
def get_conn() -> Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS projects(
        project_id     TEXT PRIMARY KEY,
        dgw_pic        TEXT NOT NULL,
        asus_pic       TEXT NOT NULL,
        partnumber     TEXT,               -- optional
        sku_code       TEXT NOT NULL,      -- Mã hàng
        qty            INTEGER NOT NULL,
        price_vnd      REAL NOT NULL,
        asus_order_email TEXT,             -- optional
        si             TEXT NOT NULL,
        eu             TEXT NOT NULL,
        pi_no          TEXT,               -- có thể bổ sung/đổi qua Quick PI
        bill_no        TEXT,
        lot_no         TEXT,
        declaration_no TEXT,
        s4_in_warehouse_date TEXT,         -- dd/mm/yyyy
        s4_arrival_port_date TEXT,         -- dd/mm/yyyy
        s4_departure_date TEXT,            -- dd/mm/yyyy
        row_created_at TEXT NOT NULL
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS status_logs(
        log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id  TEXT NOT NULL,
        status_text TEXT NOT NULL,
        note        TEXT,
        updated_by  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    conn.commit()

def get_latest_update_s4(conn: Connection) -> str | None:
    cur = conn.execute("SELECT value FROM settings WHERE key='latest_update_s4'")
    row = cur.fetchone()
    return row[0] if row else None

def set_latest_update_s4(conn: Connection, ts_str: str):
    conn.execute("INSERT INTO settings(key,value) VALUES('latest_update_s4', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (ts_str,))
    conn.commit()

def gen_project_id(conn: Connection) -> str:
    cur = conn.execute("SELECT COUNT(*) FROM projects")
    n = cur.fetchone()[0] + 1
    return f"PJT-{n:05d}"

# ========= Helpers =========
REQUIRED_ADD_FIELDS = ["dgw_pic","asus_pic","sku_code","qty","price_vnd","si","eu"]  # bắt buộc
OPTIONAL_ADD_FIELDS = ["partnumber","asus_order_email"]

# chuẩn hóa ngày: input có thể dd/mm/yyyy hoặc yyyy-mm-dd -> output dd/mm/yyyy
def normalize_date_cell(val) -> str | None:
    if val is None: return None
    if isinstance(val, float) and pd.isna(val): return None
    s = str(val).strip()
    if s == "" or s.lower() in {"nan","nat","none"}:
        return None
    # cố parse
    try:
        if "-" in s:
            # yyyy-mm-dd (hoặc ISO)
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt): return None
            return dt.strftime("%d/%m/%Y")
        elif "/" in s:
            # dd/mm/yyyy có thể
            # pandas thông minh đủ, chúng ta ép dayfirst
            dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.isna(dt): return None
            return dt.strftime("%d/%m/%Y")
        else:
            # Excel serial?
            dt = pd.to_datetime(s, errors="coerce", unit="D", origin="1899-12-30")
            if pd.isna(dt):
                return None
            return dt.strftime("%d/%m/%Y")
    except Exception:
        return None

def contains_like(series: pd.Series, keyword: str) -> pd.Series:
    if keyword is None or str(keyword).strip() == "":
        return pd.Series([True]*len(series), index=series.index)
    return series.fillna("").str.contains(str(keyword).strip(), case=False, na=False)

# đọc file CSV/XLSX an toàn
def read_any_table(uploaded) -> pd.DataFrame:
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    return pd.read_excel(uploaded)

# === Mapping Import Logistics (LOCKED) ===
# PI: Hợp đồng/ Số PO NK
# Bill: Bill of lading
# S4_Arrival_Port_Date: Ngày đến cảng
# S4_In_Warehouse_Date: Ngày đến kho
# S4_Departure_Date: Ngày khởi hành
# Declaration_No: Số tờ khai
# Lot_No: Số lô

def map_import_logistics_columns(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {}
    # chuẩn hoá tiêu đề: strip + lower để map dễ
    lower_cols = {c: str(c).strip().lower() for c in df.columns}
    def find_col(*cands):
        # trả về tên cột gốc nếu trùng một trong các ứng viên (lower)
        cands_l = [c.lower() for c in cands]
        for orig, low in lower_cols.items():
            if low in cands_l:
                return orig
        return None

    col_pi = find_col("hợp đồng/ số po nk","hop dong/ so po nk","hop dong / so po nk","so po nk","pi","số pi","pi no")
    col_bill = find_col("bill of lading","bill","vận đơn")
    col_arrival = find_col("ngày đến cảng","ngay den cang","s4_arrival_port_date")
    col_inwh = find_col("ngày đến kho","ngay den kho","s4_in_warehouse_date")
    col_depart = find_col("ngày khởi hành","ngay khoi hanh","s4_departure_date")
    col_decl = find_col("số tờ khai","so to khai","declaration_no")
    col_lot = find_col("số lô","so lo","lot_no")

    # tạo df chuẩn với các cột chính (có thể None)
    out = pd.DataFrame()
    out["PI"] = df[col_pi] if col_pi else None
    out["Bill"] = df[col_bill] if col_bill else None
    out["S4_Arrival_Port_Date"] = df[col_arrival] if col_arrival else None
    out["S4_In_Warehouse_Date"] = df[col_inwh] if col_inwh else None
    out["S4_Departure_Date"] = df[col_depart] if col_depart else None
    out["Declaration_No"] = df[col_decl] if col_decl else None
    out["Lot_No"] = df[col_lot] if col_lot else None
    return out

def normalize_string(x):
    if x is None: return None
    s = str(x).strip()
    return s if s != "" else None

# ========= UI =========
st.title("Track hàng dự án — MVP")

# Role (demo)
role = st.sidebar.selectbox("Role", ["Viewer","Editor"], index=0)
st.sidebar.caption("Tab 2 & Tab 3 chỉ hiện khi Role = Editor")

# ---------- Tab 1: Projects ----------
conn = get_conn()
init_db(conn)

tab1, tab2, tab3 = st.tabs(["Projects","Add Project (Editor)","Editor Tools (Editor)"])

with tab1:
    # Header: Latest update S4
    latest_s4 = get_latest_update_s4(conn)
    if latest_s4:
        st.markdown(f"**Latest update S4:** {latest_s4}")
    else:
        st.markdown("**Latest update S4:** (chưa có)")

    # Filters (tối giản, theo spec không yêu cầu cụ thể; vẫn hữu ích)
    st.subheader("Projects")
    c1, c2, c3, c4, c5 = st.columns(5)
    f_si = c1.text_input("Lọc: SI")
    f_eu = c2.text_input("EU")
    f_dgw = c3.text_input("DGW PIC")
    f_asus = c4.text_input("Asus PIC")
    f_sku = c5.text_input("Mã hàng")
    c6, c7, c8, c9, c10, c11 = st.columns(6)
    f_pn = c6.text_input("Partnumber")
    f_pi = c7.text_input("PI")
    f_lot = c8.text_input("Số lô")
    f_bill = c9.text_input("Bill")
    f_decl = c10.text_input("Số tờ khai")
    # (Theo spec: date range theo S4 đến kho không bắt buộc; có thể bổ sung sau nếu cần)

    df = pd.read_sql_query("SELECT * FROM projects", conn)

    if len(df)==0:
        st.info("Chưa có dữ liệu.")
    else:
        # Column order locked
        # 1) SI | 2) EU | 3) DGW PIC | 4) Asus PIC | 5) Last updated (-> row_created_at tạm xem như last created/updated field ở MVP, vì spec yêu cầu không cập nhật per-row)
        # 6) Mã hàng | 7) Partnumber | 8) Qty | 9) Giá | 10) PI | 11) Số lô | 12) Bill | 13) Số tờ khai | 14) S4 đến kho | 15) S4 cập cảng | 16) S4 đi
        # Ta sẽ hiển thị "Last updated" = row_created_at (vì spec không cập nhật per-project khi import).
        df_display = pd.DataFrame({
            "SI": df["si"],
            "EU": df["eu"],
            "DGW PIC": df["dgw_pic"],
            "Asus PIC": df["asus_pic"],
            "Last updated": df["row_created_at"],
            "Mã hàng": df["sku_code"],
            "Partnumber": df["partnumber"],
            "Qty": df["qty"],
            "Giá": df["price_vnd"],
            "PI": df["pi_no"],
            "Số lô": df["lot_no"],
            "Bill": df["bill_no"],
            "Số tờ khai": df["declaration_no"],
            "S4 đến kho": df["s4_in_warehouse_date"],
            "S4 cập cảng": df["s4_arrival_port_date"],
            "S4 đi": df["s4_departure_date"],
        })

        # Apply filters
        ok = (
            contains_like(df_display["SI"], f_si) &
            contains_like(df_display["EU"], f_eu) &
            contains_like(df_display["DGW PIC"], f_dgw) &
            contains_like(df_display["Asus PIC"], f_asus) &
            contains_like(df_display["Mã hàng"], f_sku) &
            contains_like(df_display["Partnumber"], f_pn) &
            contains_like(df_display["PI"], f_pi) &
            contains_like(df_display["Số lô"], f_lot) &
            contains_like(df_display["Bill"], f_bill) &
            contains_like(df_display["Số tờ khai"], f_decl)
        )
        out = df_display[ok].copy()

        # Sort mặc định: Last updated (mới -> cũ) — với dữ liệu MVP, row_created_at làm proxy
        try:
            out["_sort"] = pd.to_datetime(out["Last updated"], errors="coerce")
        except Exception:
            out["_sort"] = pd.NaT
        out = out.sort_values("_sort", ascending=False).drop(columns=["_sort"])

        st.dataframe(out, use_container_width=True, height=540)

# ---------- Tab 2: Add Project ----------
with tab2:
    if role != "Editor":
        st.info("Chỉ Editor mới truy cập.")
    else:
        st.subheader("Add Project — Form")
        with st.form("add_one_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            dgw_pic = c1.text_input("DGW PIC *")
            asus_pic = c2.text_input("ASUS PIC *")
            partnumber = c3.text_input("Part number (optional)")

            c4, c5, c6 = st.columns(3)
            sku_code = c4.text_input("Mã hàng *").upper().strip() if c4.text_input else ""
            qty = c5.number_input("Số lượng *", min_value=1, step=1)
            price_vnd = c6.number_input("Đơn giá FV (VND) *", min_value=0.0, step=1000.0)

            c7, c8, c9 = st.columns(3)
            asus_order_email = c7.text_input("Mail nhận đơn hàng từ Asus (optional)")
            si = c8.text_input("SI *")
            eu = c9.text_input("EU *")

            submitted = st.form_submit_button("Tạo dự án")
            if submitted:
                # validate bắt buộc
                req_vals = {
                    "dgw_pic": dgw_pic.strip(),
                    "asus_pic": asus_pic.strip(),
                    "sku_code": sku_code.strip().upper(),
                    "qty": int(qty),
                    "price_vnd": float(price_vnd),
                    "si": si.strip(),
                    "eu": eu.strip(),
                }
                if any(v is None or (isinstance(v, str) and v == "") for v in req_vals.values()):
                    st.error("Thiếu 1 trong các trường bắt buộc.")
                else:
                    pid = gen_project_id(conn)
                    now = datetime.now().strftime("%Y-%m-%d %H:%M")
                    conn.execute("""
                        INSERT INTO projects(project_id,dgw_pic,asus_pic,partnumber,sku_code,qty,price_vnd,asus_order_email,si,eu,pi_no,bill_no,lot_no,declaration_no,
                                             s4_in_warehouse_date,s4_arrival_port_date,s4_departure_date,row_created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        pid, dgw_pic.strip(), asus_pic.strip(), partnumber.strip() or None,
                        req_vals["sku_code"], req_vals["qty"], req_vals["price_vnd"],
                        asus_order_email.strip() or None, req_vals["si"], req_vals["eu"],
                        None, None, None, None, None, None, None, now
                    ))
                    conn.commit()
                    st.success(f"Đã tạo {pid}")

        st.divider()
        st.subheader("Add Project — Paste nhiều dòng (TSV/CSV, theo THỨ TỰ đã lock)")
        st.caption("Thứ tự: DGW PIC / ASUS PIC / Part number / Mã hàng / Số lượng / Đơn giá FV / Mail nhận đơn hàng từ Asus / SI / EU")
        txt = st.text_area("Dán dữ liệu (mỗi dòng 1 dự án; header có/không đều được)", height=180,
                           placeholder="Nguyen Van A,Alice,,NB-B1403CVA,50,15500000,orders.asus@asus.com,SI001,EU-HN")
        if st.button("Append từ Paste"):
            if not txt.strip():
                st.warning("Không có dữ liệu.")
            else:
                # tách dòng
                lines = [ln for ln in txt.strip().splitlines() if ln.strip()!=""]
                # nếu có header: phát hiện bằng cách thử parse số lượng (cột 5)
                created = 0
                skipped = 0
                for idx, line in enumerate(lines):
                    parts = [p.strip() for p in (line.replace("\t",",")).split(",")]
                    if len(parts) < 9:
                        skipped += 1
                        continue
                    dgw_pic, asus_pic, partnumber, sku_code, qty, price_vnd, asus_ord_mail, si, eu = parts[:9]
                    # bỏ qua dòng header nếu cột qty không phải số
                    if idx == 0:
                        try:
                            _ = float(qty)
                        except:
                            # có header -> skip dòng này
                            continue
                    # validate bắt buộc
                    if any([not dgw_pic, not asus_pic, not sku_code, not qty, not price_vnd, not si, not eu]):
                        skipped += 1
                        continue
                    try:
                        qty_i = int(float(qty))
                        price_f = float(price_vnd)
                    except:
                        skipped += 1
                        continue
                    pid = gen_project_id(conn)
                    now = datetime.now().strftime("%Y-%m-%d %H:%M")
                    conn.execute("""
                        INSERT INTO projects(project_id,dgw_pic,asus_pic,partnumber,sku_code,qty,price_vnd,asus_order_email,si,eu,pi_no,bill_no,lot_no,declaration_no,
                                             s4_in_warehouse_date,s4_arrival_port_date,s4_departure_date,row_created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        pid, dgw_pic, asus_pic, (partnumber or None),
                        sku_code.upper(), qty_i, price_f, (asus_ord_mail or None),
                        si, eu, None, None, None, None, None, None, None, now
                    ))
                    created += 1
                conn.commit()
                st.success(f"Đã thêm {created} dòng. Bỏ qua {skipped} dòng không hợp lệ.")

        st.divider()
        st.subheader("Add Project — Import (CSV/XLSX)")
        st.caption("Header VN hoặc EN, thứ tự/hoa thường không bắt buộc. Bắt buộc có: DGW PIC, ASUS PIC, Mã hàng, Số lượng, Đơn giá FV, SI, EU")
        up = st.file_uploader("Chọn file", type=["csv","xlsx"], key="addproj")
        if up:
            try:
                df_imp = read_any_table(up)
                # chuẩn hóa header: strip lower để map
                cols_lower = {c: str(c).strip().lower() for c in df_imp.columns}
                def pick(*cand):
                    cl = [c.lower() for c in cand]
                    for orig, low in cols_lower.items():
                        if low in cl:
                            return orig
                    return None
                c_dgw = pick("dgw pic","dgw_pic")
                c_asus = pick("asus pic","asus_pic")
                c_pn = pick("part number","partnumber")
                c_sku = pick("mã hàng","ma hang","sku_code","sku")
                c_qty = pick("số lượng","so luong","qty","quantity")
                c_price = pick("đơn giá fv","don gia fv","price_vnd","price","unit price","gia")
                c_mail = pick("mail nhận đơn hàng từ asus","mail nhan don hang tu asus","asus_order_email","order email")
                c_si = pick("si")
                c_eu = pick("eu")

                need_cols = [c_dgw, c_asus, c_sku, c_qty, c_price, c_si, c_eu]
                if any(x is None for x in need_cols):
                    st.error("Thiếu cột bắt buộc trong file import.")
                else:
                    df_use = pd.DataFrame({
                        "dgw_pic": df_imp[c_dgw],
                        "asus_pic": df_imp[c_asus],
                        "partnumber": df_imp[c_pn] if c_pn else None,
                        "sku_code": df_imp[c_sku],
                        "qty": df_imp[c_qty],
                        "price_vnd": df_imp[c_price],
                        "asus_order_email": df_imp[c_mail] if c_mail else None,
                        "si": df_imp[c_si],
                        "eu": df_imp[c_eu],
                    })
                    # preview
                    st.dataframe(df_use.head(20), use_container_width=True, height=280)
                    if st.button("Run Import Projects"):
                        created = 0
                        skipped = 0
                        for _, r in df_use.iterrows():
                            try:
                                dgw_pic = str(r["dgw_pic"]).strip()
                                asus_pic = str(r["asus_pic"]).strip()
                                sku_code = str(r["sku_code"]).strip().upper()
                                qty_i = int(float(r["qty"]))
                                price_f = float(r["price_vnd"])
                                si = str(r["si"]).strip()
                                eu = str(r["eu"]).strip()
                                # validate
                                if any([not dgw_pic, not asus_pic, not sku_code, qty_i<=0, price_f<0, not si, not eu]):
                                    skipped += 1
                                    continue
                                partnumber = (None if pd.isna(r["partnumber"]) else str(r["partnumber"]).strip()) if "partnumber" in r else None
                                mail = (None if pd.isna(r["asus_order_email"]) else str(r["asus_order_email"]).strip()) if "asus_order_email" in r else None
                                pid = gen_project_id(conn)
                                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                                conn.execute("""
                                    INSERT INTO projects(project_id,dgw_pic,asus_pic,partnumber,sku_code,qty,price_vnd,asus_order_email,si,eu,pi_no,bill_no,lot_no,declaration_no,
                                                         s4_in_warehouse_date,s4_arrival_port_date,s4_departure_date,row_created_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                """, (
                                    pid, dgw_pic, asus_pic, partnumber, sku_code, qty_i, price_f,
                                    mail, si, eu, None, None, None, None, None, None, None, now
                                ))
                                created += 1
                            except Exception:
                                skipped += 1
                        conn.commit()
                        st.success(f"Đã tạo {created} dự án. Bỏ qua {skipped} dòng không hợp lệ.")

# ---------- Tab 3: Editor Tools ----------
with tab3:
    if role != "Editor":
        st.info("Chỉ Editor mới truy cập.")
    else:
        st.subheader("Editor Tools")

        # A) Quick PI
        with st.expander("A) Quick PI — cập nhật PI tức thời cho 1 dự án", expanded=False):
            # chọn dự án
            dfp = pd.read_sql_query("SELECT project_id, sku_code, partnumber, pi_no FROM projects", conn)
            if len(dfp)==0:
                st.info("Chưa có dự án.")
            else:
                dfp["label"] = dfp.apply(lambda r: f"{r['project_id']} | {r['sku_code']} | {r['partnumber'] or ''} | PI={r['pi_no'] or '-'}", axis=1)
                sel = st.selectbox("Chọn dự án", options=dfp["label"].tolist())
                new_pi = st.text_input("Nhập PI *").strip().upper()
                if st.button("Cập nhật PI"):
                    pid = sel.split("|")[0].strip()
                    if new_pi == "":
                        st.error("PI không được rỗng.")
                    else:
                        conn.execute("UPDATE projects SET pi_no=? WHERE project_id=?", (new_pi, pid))
                        # ghi log trạng thái (append-only)
                        now = datetime.now().strftime("%Y-%m-%d %H:%M")
                        conn.execute("""
                            INSERT INTO status_logs(project_id,status_text,note,updated_by,updated_at)
                            VALUES (?,?,?,?,?)
                        """, (pid, f"Confirmed PI ({new_pi})", "", "PM", now))
                        conn.commit()
                        st.success(f"Đã cập nhật PI cho {pid}")

        # B) Import Logistics
        with st.expander("B) Import Logistics — cập nhật S4/Bill/Tờ khai/Lô (match theo PI)", expanded=False):
            up2 = st.file_uploader("Chọn file (CSV/XLSX) — cột sẽ auto-map đúng chuẩn đã lock", type=["csv","xlsx"], key="logimp")
            if up2:
                try:
                    df_raw = read_any_table(up2)
                    df_map = map_import_logistics_columns(df_raw)
                    # preview gọn
                    st.dataframe(df_map.head(20), use_container_width=True, height=300)
                    if st.button("Run Import Logistics"):
                        # duyệt tuần tự — duplicate PI: last-write-wins do ta ghi đè theo thứ tự dòng
                        # ô trống không ghi đè
                        # lấy danh sách project có PI
                        dfp = pd.read_sql_query("SELECT project_id, pi_no FROM projects WHERE pi_no IS NOT NULL", conn)
                        # index theo PI -> list project_id
                        pi_groups = {}
                        for _, r in dfp.iterrows():
                            pi = str(r["pi_no"]).strip().upper()
                            if not pi: continue
                            pi_groups.setdefault(pi, []).append(r["project_id"])

                        for _, r in df_map.iterrows():
                            pi = normalize_string(r.get("PI"))
                            if not pi:    # nếu không có PI -> bỏ qua (theo spec match theo PI)
                                continue
                            pi_u = pi.upper()
                            target_ids = pi_groups.get(pi_u, [])
                            if not target_ids:
                                continue
                            # chuẩn hoá giá trị update
                            bill = normalize_string(r.get("Bill"))
                            lot = normalize_string(r.get("Lot_No"))
                            decl = normalize_string(r.get("Declaration_No"))
                            d_in = normalize_date_cell(r.get("S4_In_Warehouse_Date"))
                            d_arr = normalize_date_cell(r.get("S4_Arrival_Port_Date"))
                            d_dep = normalize_date_cell(r.get("S4_Departure_Date"))
                            for pid in target_ids:
                                # build SET động, chỉ set khi có giá trị (ô trống không ghi đè)
                                sets = []
                                vals = []
                                if bill is not None:
                                    sets.append("bill_no=?"); vals.append(bill)
                                if lot is not None:
                                    sets.append("lot_no=?"); vals.append(lot)
                                if decl is not None:
                                    sets.append("declaration_no=?"); vals.append(decl)
                                if d_in is not None:
                                    sets.append("s4_in_warehouse_date=?"); vals.append(d_in)
                                if d_arr is not None:
                                    sets.append("s4_arrival_port_date=?"); vals.append(d_arr)
                                if d_dep is not None:
                                    sets.append("s4_departure_date=?"); vals.append(d_dep)
                                if sets:
                                    sql = f"UPDATE projects SET {', '.join(sets)} WHERE project_id=?"
                                    vals.append(pid)
                                    conn.execute(sql, tuple(vals))
                        conn.commit()
                        # chỉ cập nhật Latest update S4 (Tab 1 header)
                        ts = datetime.now().strftime("%d-%m-%Y %H:%M")
                        set_latest_update_s4(conn, ts)
                        st.success("Import thành công. (Đã cập nhật 'Latest update S4')")

                except Exception as e:
                    st.error(f"Lỗi file/import: {e}")

        # C) Status Update
        with st.expander("C) Status Update — By Project & Bulk", expanded=False):
            st.markdown("**C1) By Project**")
            dfp = pd.read_sql_query("SELECT project_id, sku_code, partnumber FROM projects", conn)
            if len(dfp)==0:
                st.info("Chưa có dự án.")
            else:
                dfp["label"] = dfp.apply(lambda r: f"{r['project_id']} | {r['sku_code']} | {r['partnumber'] or ''}", axis=1)
                sel1 = st.selectbox("Chọn dự án", options=dfp["label"].tolist(), key="st_one_sel")
                stt = st.text_input("Status text *", key="st_one_text")
                note = st.text_area("Note (optional)", height=80, key="st_one_note")
                if st.button("Ghi trạng thái (1 dự án)"):
                    if not stt.strip():
                        st.error("Thiếu status text.")
                    else:
                        pid = sel1.split("|")[0].strip()
                        now = datetime.now().strftime("%Y-%m-%d %H:%M")
                        conn.execute("""
                            INSERT INTO status_logs(project_id,status_text,note,updated_by,updated_at)
                            VALUES (?,?,?,?,?)
                        """, (pid, stt.strip(), note.strip() or None, "PM", now))
                        conn.commit()
                        st.success("Đã ghi log.")

            st.divider()
            st.markdown("**C2) Bulk** — lọc theo **Bill / Số lô / Số tờ khai**")
            c1, c2, c3 = st.columns(3)
            f_bill = c1.text_input("Bill (contains)", key="bulk_bill")
            f_lot = c2.text_input("Số lô (contains)", key="bulk_lot")
            f_decl = c3.text_input("Số tờ khai (contains)", key="bulk_decl")

            d = pd.read_sql_query("SELECT project_id, sku_code, partnumber, bill_no, lot_no, declaration_no FROM projects", conn)
            ok_mask = (
                contains_like(d["bill_no"], f_bill) &
                contains_like(d["lot_no"], f_lot) &
                contains_like(d["declaration_no"], f_decl)
            )
            dsel = d[ok_mask].copy()
            st.dataframe(dsel, use_container_width=True, height=300)
            stt_all = st.text_input("Status text * (áp cho tất cả match)", key="bulk_text")
            note_all = st.text_area("Note (optional)", height=60, key="bulk_note")
            if st.button("Ghi trạng thái hàng loạt"):
                if not stt_all.strip():
                    st.error("Thiếu status text.")
                elif len(dsel)==0:
                    st.warning("Không có dự án nào match.")
                else:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M")
                    for _, r in dsel.iterrows():
                        conn.execute("""
                            INSERT INTO status_logs(project_id,status_text,note,updated_by,updated_at)
                            VALUES (?,?,?,?,?)
                        """, (r["project_id"], stt_all.strip(), note_all.strip() or None, "PM", now))
                    conn.commit()
                    st.success(f"Đã ghi trạng thái cho {len(dsel)} dự án.")
