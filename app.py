import streamlit as st
import db
import pandas as pd
import os
from datetime import date, timedelta
from io import BytesIO

APP_VERSION = 'v1.0.0'

st.set_page_config(
    page_title=f'Fabrication Tracker {APP_VERSION}',
    page_icon='🏗️',
    layout='wide',
    initial_sidebar_state='expanded',
)

STAGE_COLOR = {
    'FIT UP':              '#2563eb',
    'WELDING':             '#dc2626',
    'BLASTING & PAINTING': '#7c3aed',
    'SEND TO SITE':        '#ea7c00',
}
STAGE_BADGE = {
    'FIT UP':              '🔵',
    'WELDING':             '🔴',
    'BLASTING & PAINTING': '🟣',
    'SEND TO SITE':        '🟠',
}

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stSidebar"] {
    background: #1e293b;
    min-width: 280px !important;
    max-width: 280px !important;
}
[data-testid="stSidebar"] * { color: #f1f5f9 !important; }
[data-testid="stSidebar"] .stRadio label {
    font-size: 16px;
    padding: 6px 4px;
}
[data-testid="stSidebar"] h3 { font-size: 18px !important; }
div[data-testid="metric-container"] {
    background: white; border-radius: 8px;
    padding: 12px 16px; box-shadow: 0 1px 3px rgba(0,0,0,.1);
}
.block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# Login
# ══════════════════════════════════════════════════════════════════════════════
def show_login():
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(f'## 🏗️ Fabrication Tracker')
        st.caption(APP_VERSION)
        st.divider()
        with st.form('login_form'):
            username = st.text_input('Username', placeholder='Enter username')
            password = st.text_input('Password', type='password', placeholder='Enter password')
            submitted = st.form_submit_button('Login', use_container_width=True, type='primary')
            if submitted:
                if not username or not password:
                    st.error('Enter username and password.')
                else:
                    user = db.authenticate(username, password)
                    if user:
                        st.session_state.user  = user
                        default = '📅 Report' if user['role'] == 'viewer' else '✏️ Daily Entry'
                        st.session_state.page  = default
                        st.session_state.queue = []
                        st.rerun()
                    else:
                        st.error('Invalid username or password.')
        st.caption('Default login: admin / admin123')


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════
def show_sidebar():
    user = st.session_state.user
    with st.sidebar:
        st.markdown(f'### 🏗️ Fabrication Tracker')
        st.caption(APP_VERSION)
        st.divider()
        role_labels = {'admin': 'Administrator', 'user': 'User', 'viewer': 'Viewer (QC)'}
        role_label  = role_labels.get(user['role'], user['role'].capitalize())
        st.markdown(f"👤 **{user['username']}**  ({role_label})")
        st.divider()

        if user['role'] == 'viewer':
            pages = ['📅 Report', '📊 Progress', '🚚 Delivery', '📦 Raw Material']
        elif user['role'] == 'admin':
            pages = ['✏️ Daily Entry', '📅 Report', '📊 Progress', '🚚 Delivery',
                     '📦 Raw Material', '⚙️ Manage']
        else:
            pages = ['✏️ Daily Entry', '📅 Report', '📊 Progress', '🚚 Delivery',
                     '📦 Raw Material']

        default_page = pages[0]
        current = st.session_state.get('page', default_page)
        page = st.radio('Navigation', pages, label_visibility='collapsed',
                        index=pages.index(current) if current in pages else 0)
        st.session_state.page = page

        st.divider()
        st.caption(f'📅 {date.today().strftime("%d %B %Y")}')
        if st.button('🔓 Logout', use_container_width=True):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Page: Daily Entry
# ══════════════════════════════════════════════════════════════════════════════
def page_daily_entry():
    st.header('✏️ Daily Entry')

    if 'queue' not in st.session_state:
        st.session_state.queue = []

    marks = db.get_marks()

    col_form, col_right = st.columns([1, 1.6])

    # ── Left: Entry Form ──────────────────────────────────────────────────────
    with col_form:
        with st.container(border=True):
            st.subheader('Add Entry')
            entry_date = st.date_input('Date', value=date.today(), key='entry_date')
            mark = st.selectbox('Assembly Mark', [''] + marks, key='entry_mark')

            subs = db.get_sub_assemblies(mark) if mark else []
            sub  = st.selectbox('Sub-Assembly Mark', [''] + subs, key='entry_sub')

            st.write('**Stage**')
            stage_cols = st.columns(2)
            stages_r1  = db.STAGES[:2]
            stages_r2  = db.STAGES[2:]
            if 'sel_stage' not in st.session_state:
                st.session_state.sel_stage = db.STAGES[0]

            for i, s in enumerate(stages_r1):
                if stage_cols[i].button(
                    f'{STAGE_BADGE[s]} {s}',
                    use_container_width=True,
                    type='primary' if st.session_state.sel_stage == s else 'secondary'
                ):
                    st.session_state.sel_stage = s
                    st.rerun()
            for i, s in enumerate(stages_r2):
                if stage_cols[i].button(
                    f'{STAGE_BADGE[s]} {s}',
                    use_container_width=True,
                    type='primary' if st.session_state.sel_stage == s else 'secondary'
                ):
                    st.session_state.sel_stage = s
                    st.rerun()
            stage = st.session_state.sel_stage

            # Auto weight
            weight_val = 0.0
            if mark:
                all_parts = db.get_parts(mark)
                parts = [p for p in all_parts if p['sub_assembly_mark'] == sub] if sub else all_parts
                weight_val = sum(p['total_weight_kg'] for p in parts)

            weight  = st.number_input('Weight (kg)', value=weight_val, min_value=0.0, format='%.2f')
            qty     = st.number_input('Qty', value=1, min_value=0, step=1)
            do_no   = ''
            if stage in ('BLASTING & PAINTING', 'SEND TO SITE'):
                do_no = st.text_input('D.O. Number *', placeholder='Required for this stage')
            remarks = st.text_area('Remarks', height=70)

            if st.button('➕ Add to Queue', type='primary', use_container_width=True):
                errors = []
                if not mark:
                    errors.append('Assembly Mark is required.')
                if stage in ('BLASTING & PAINTING', 'SEND TO SITE') and not do_no.strip():
                    errors.append(f'D.O. Number is required for [{stage}].')
                if mark and not errors:
                    stage_idx = db.STAGES.index(stage)
                    if stage_idx > 0:
                        prev_stage    = db.STAGES[stage_idx - 1]
                        completed     = db.get_completed_stages(mark, sub or '')
                        queued_stages = {e['stage'] for e in st.session_state.queue
                                         if e['mark'] == mark.upper() and e['sub'] == (sub or '').upper()}
                        if prev_stage not in completed and prev_stage not in queued_stages:
                            errors.append(f'"{prev_stage}" must be completed before [{stage}].')
                if errors:
                    for e in errors:
                        st.error(e)
                else:
                    st.session_state.queue.append({
                        'date':    str(entry_date),
                        'mark':    mark.upper(),
                        'sub':     (sub or '').upper(),
                        'stage':   stage,
                        'weight':  round(weight, 2),
                        'qty':     int(qty),
                        'do_no':   do_no.strip(),
                        'remarks': remarks.strip(),
                    })
                    st.success(f'Added: {mark} — {stage}')
                    st.rerun()

    # ── Right: Queue + Saved ──────────────────────────────────────────────────
    with col_right:
        with st.container(border=True):
            st.subheader(f'Queue  ({len(st.session_state.queue)} items)')
            if st.session_state.queue:
                df_q = pd.DataFrame(st.session_state.queue)
                df_q.columns = ['Date', 'Assembly', 'Sub-Assembly', 'Stage',
                                 'Weight (kg)', 'Qty', 'D.O. No.', 'Remarks']
                st.dataframe(df_q, use_container_width=True, hide_index=True)

                c1, c2 = st.columns(2)
                with c1:
                    if st.button('💾 Save All', type='primary', use_container_width=True):
                        for e in st.session_state.queue:
                            db.add_progress(e['date'], e['mark'], e['sub'], e['stage'],
                                            e['weight'], e['qty'], e['remarks'], e['do_no'])
                        count = len(st.session_state.queue)
                        st.session_state.queue = []
                        st.success(f'Saved {count} entries.')
                        st.rerun()
                with c2:
                    if st.button('🗑 Clear Queue', use_container_width=True):
                        st.session_state.queue = []
                        st.rerun()
            else:
                st.info('Queue is empty. Add entries from the form.')

        st.markdown('---')

        with st.container(border=True):
            st.subheader("Today's Saved Entries")
            today_rows = db.search_progress(start=str(date.today()), end=str(date.today()))
            if today_rows:
                df_t = pd.DataFrame(today_rows)[
                    ['id', 'entry_date', 'assembly_mark', 'sub_assembly_mark',
                     'stage', 'delivery_order_no', 'weight_kg', 'qty', 'remarks']]
                df_t.columns = ['ID', 'Date', 'Assembly', 'Sub-Assembly',
                                 'Stage', 'D.O. No.', 'Weight (kg)', 'Qty', 'Remarks']
                st.dataframe(df_t, use_container_width=True, hide_index=True)

                with st.form('del_today'):
                    del_id = st.number_input('Delete entry by ID', min_value=0, step=1, value=0)
                    if st.form_submit_button('🗑 Delete', type='secondary'):
                        if del_id > 0:
                            db.delete_progress(int(del_id))
                            st.success(f'Deleted entry #{int(del_id)}')
                            st.rerun()
            else:
                st.info('No entries saved today.')


# ══════════════════════════════════════════════════════════════════════════════
# Page: Report
# ══════════════════════════════════════════════════════════════════════════════
def page_report():
    st.header('📅 Report')

    c1, c2, c3, c4 = st.columns([1, 1, 1.2, 1.2])
    with c1:
        start = st.date_input('From', value=date.today() - timedelta(days=7), key='rpt_start')
    with c2:
        end = st.date_input('To', value=date.today(), key='rpt_end')
    with c3:
        asm_filter = st.selectbox('Assembly', ['All'] + db.get_marks(), key='rpt_asm')
    with c4:
        stage_filter = st.selectbox('Stage', ['All'] + db.STAGES, key='rpt_stage')

    bc1, bc2 = st.columns([1, 1])
    with bc1:
        load = st.button('🔍 Load by Date', type='primary', use_container_width=True)
    with bc2:
        load_all = st.button('📋 Show All', use_container_width=True)

    if 'report_rows' not in st.session_state:
        st.session_state.report_rows = []

    asm = None if asm_filter == 'All' else asm_filter
    stg = None if stage_filter == 'All' else stage_filter

    if load:
        st.session_state.report_rows = db.search_progress(
            stage=stg, assembly_mark=asm, start=str(start), end=str(end))
    if load_all:
        st.session_state.report_rows = db.search_progress(stage=stg, assembly_mark=asm)

    rows = st.session_state.report_rows
    if rows:
        summary       = db.get_summary()
        project_total = summary.get('total', 0) or 0
        stage_totals  = {s: sum(r['weight_kg'] for r in rows if r['stage'] == s)
                         for s in db.STAGES}

        st.divider()
        metric_cols = st.columns(len(db.STAGES) + 1)
        with metric_cols[0]:
            st.metric('Total Weight (Project)', f'{project_total:,.1f} kg')
        for i, s in enumerate(db.STAGES):
            pct = min(stage_totals[s] / project_total * 100, 100) if project_total else 0
            with metric_cols[i + 1]:
                st.metric(f'{STAGE_BADGE[s]} {s}',
                          f'{stage_totals[s]:,.1f} kg', f'{pct:.1f}%')

        st.divider()
        df = pd.DataFrame(rows)[
            ['entry_date', 'assembly_mark', 'sub_assembly_mark', 'stage',
             'delivery_order_no', 'weight_kg', 'qty', 'remarks']]
        df.columns = ['Date', 'Assembly', 'Sub-Assembly', 'Stage',
                      'D.O. No.', 'Weight (kg)', 'Qty', 'Remarks']
        st.dataframe(df, use_container_width=True, hide_index=True)

        ec1, ec2 = st.columns(2)
        with ec1:
            st.download_button('📥 Export CSV',
                               df.to_csv(index=False).encode('utf-8'),
                               f'report_{start}_{end}.csv', 'text/csv',
                               use_container_width=True)
        with ec2:
            buf = BytesIO()
            df.to_excel(buf, index=False, engine='openpyxl')
            st.download_button('📥 Export Excel', buf.getvalue(),
                               f'report_{start}_{end}.xlsx',
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                               use_container_width=True)
    else:
        st.info('Click **Load by Date** or **Show All** to display data.')


# ══════════════════════════════════════════════════════════════════════════════
# Page: Progress
# ══════════════════════════════════════════════════════════════════════════════
def page_progress():
    st.header('📊 Progress Overview')

    summary       = db.get_summary()
    project_total = summary.get('total', 0) or 0

    met_cols = st.columns(len(db.STAGES))
    for i, s in enumerate(db.STAGES):
        done = summary.get(s, 0) or 0
        pct  = min(done / project_total * 100, 100) if project_total else 0
        with met_cols[i]:
            st.metric(f'{STAGE_BADGE[s]} {s}', f'{done:,.1f} kg', f'{pct:.1f}%')

    st.divider()

    rows = db.get_cumulative_by_sub()
    if not rows:
        st.info('No progress data yet.')
        return

    df = pd.DataFrame(rows).rename(columns={
        'assembly_mark':    'Assembly',
        'sub_assembly_mark':'Sub-Assembly',
        'total_weight_kg':  'Total (kg)',
        'fitup':            'Fit Up (kg)',
        'welding':          'Welding (kg)',
        'blasting':         'Blast/Paint (kg)',
        'sendsite':         'Send to Site (kg)',
    })

    def pct_str(row, col):
        t = row['Total (kg)']
        return f"{min(row[col]/t*100, 100):.1f}%" if t else '—'

    df['Fit Up %']       = df.apply(lambda r: pct_str(r, 'Fit Up (kg)'),       axis=1)
    df['Welding %']      = df.apply(lambda r: pct_str(r, 'Welding (kg)'),      axis=1)
    df['Blast/Paint %']  = df.apply(lambda r: pct_str(r, 'Blast/Paint (kg)'),  axis=1)
    df['Send to Site %'] = df.apply(lambda r: pct_str(r, 'Send to Site (kg)'), axis=1)

    display_cols = [
        'Assembly', 'Sub-Assembly', 'Total (kg)',
        'Fit Up (kg)', 'Fit Up %',
        'Welding (kg)', 'Welding %',
        'Blast/Paint (kg)', 'Blast/Paint %',
        'Send to Site (kg)', 'Send to Site %',
    ]
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# Page: Delivery
# ══════════════════════════════════════════════════════════════════════════════
def page_delivery():
    st.header('🚚 Delivery Log')

    c1, c2 = st.columns([1, 1])
    with c1:
        start = st.date_input('From', value=date.today() - timedelta(days=30), key='del_start')
    with c2:
        end = st.date_input('To', value=date.today(), key='del_end')

    all_rows = db.get_deliveries()
    rows = [r for r in all_rows
            if str(start) <= r['entry_date'] <= str(end)] if all_rows else []

    if rows:
        df = pd.DataFrame(rows)[
            ['entry_date', 'assembly_mark', 'sub_assembly_mark', 'stage',
             'delivery_order_no', 'weight_kg', 'qty', 'remarks']]
        df.columns = ['Date', 'Assembly', 'Sub-Assembly', 'Type',
                      'D.O. No.', 'Weight (kg)', 'Qty', 'Remarks']
        st.dataframe(df, use_container_width=True, hide_index=True)

        ec1, ec2 = st.columns(2)
        with ec1:
            st.download_button('📥 Export CSV',
                               df.to_csv(index=False).encode('utf-8'),
                               'delivery_log.csv', 'text/csv',
                               use_container_width=True)
        with ec2:
            buf = BytesIO()
            df.to_excel(buf, index=False, engine='openpyxl')
            st.download_button('📥 Export Excel', buf.getvalue(),
                               'delivery_log.xlsx',
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                               use_container_width=True)
    else:
        st.info('No delivery records in this date range.')


# ══════════════════════════════════════════════════════════════════════════════
# Page: Manage (admin only)
# ══════════════════════════════════════════════════════════════════════════════
def page_manage():
    st.header('⚙️ Manage Data')

    tab_import, tab_export, tab_users, tab_danger = st.tabs(
        ['📥 Import Excel', '📤 Export Master', '👥 Users', '⚠️ Danger Zone'])

    # ── Import ────────────────────────────────────────────────────────────────
    with tab_import:
        st.subheader('Import from Excel')
        st.caption('Columns: Assembly Mark, Sub-Assembly, Part Mark, No., Name, '
                   'Profile, kg/m, Length, Weight, Profile2, Grade')
        uploaded = st.file_uploader('Choose Excel file (.xlsx)', type=['xlsx', 'xls'])
        if uploaded and st.button('Import', type='primary'):
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                tmp.write(uploaded.read())
                tmp_path = tmp.name
            try:
                count = db.import_excel(tmp_path)
                st.success(f'Imported {count} parts successfully.')
            except Exception as e:
                st.error(f'Import failed: {e}')
            finally:
                os.unlink(tmp_path)

        st.divider()
        st.subheader('Add Assembly Manually')
        with st.form('add_asm'):
            mark = st.text_input('Assembly Mark')
            desc = st.text_input('Description (optional)')
            if st.form_submit_button('Add Assembly', type='primary'):
                if mark:
                    db.add_assembly(mark.upper(), desc)
                    st.success(f'Assembly {mark.upper()} added.')
                    st.rerun()
                else:
                    st.error('Assembly Mark is required.')

    # ── Export ────────────────────────────────────────────────────────────────
    with tab_export:
        st.subheader('Export Master Database')
        st.caption('Parts list with cumulative progress per stage.')
        rows = db.get_master_export()
        if rows:
            df = pd.DataFrame(rows)
            ec1, ec2 = st.columns(2)
            with ec1:
                st.download_button('📥 Download CSV',
                                   df.to_csv(index=False).encode('utf-8'),
                                   f'master_database_{date.today()}.csv', 'text/csv',
                                   use_container_width=True, type='primary')
            with ec2:
                buf = BytesIO()
                df.to_excel(buf, index=False, engine='openpyxl')
                st.download_button('📥 Download Excel', buf.getvalue(),
                                   f'master_database_{date.today()}.xlsx',
                                   'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                   use_container_width=True, type='primary')
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info('No parts data to export.')

    # ── Users ─────────────────────────────────────────────────────────────────
    with tab_users:
        st.subheader('User List')
        users = db.get_users()
        if users:
            df_u = pd.DataFrame(users)
            df_u['active'] = df_u['active'].map({1: '✅ Active', 0: '❌ Inactive'})
            df_u.columns   = ['ID', 'Username', 'Role', 'Status']
            st.dataframe(df_u, use_container_width=True, hide_index=True)
        else:
            st.info('No users found.')

        st.divider()
        uc1, uc2 = st.columns(2)

        with uc1:
            with st.form('add_user'):
                st.subheader('Add User')
                uname = st.text_input('Username')
                pwd   = st.text_input('Password', type='password')
                role  = st.selectbox('Role', ['user', 'admin', 'viewer'])
                if st.form_submit_button('Add User', type='primary'):
                    if not uname or not pwd:
                        st.error('Username and password required.')
                    elif db.add_user(uname, pwd, role):
                        st.success(f'User "{uname}" added.')
                        st.rerun()
                    else:
                        st.error('Username already exists.')

        with uc2:
            with st.form('reset_pwd'):
                st.subheader('Reset Password')
                uid     = st.number_input('User ID', min_value=1, step=1, value=1)
                new_pwd = st.text_input('New Password', type='password')
                if st.form_submit_button('Reset Password'):
                    if new_pwd:
                        db.update_user_password(int(uid), new_pwd)
                        st.success('Password updated.')
                    else:
                        st.error('Password cannot be empty.')

        st.divider()
        with st.form('toggle_user'):
            st.subheader('Toggle Active / Delete')
            act_uid = st.number_input('User ID', min_value=1, step=1, value=1, key='act_uid')
            tc1, tc2 = st.columns(2)
            with tc1:
                toggle = st.form_submit_button('🔄 Toggle Active')
            with tc2:
                delete = st.form_submit_button('🗑 Delete User')
            if toggle:
                db.toggle_user_active(int(act_uid))
                st.success(f'Toggled user {int(act_uid)}.')
                st.rerun()
            if delete:
                db.delete_user_entry(int(act_uid))
                st.success(f'Deleted user {int(act_uid)}.')
                st.rerun()

    # ── Danger Zone ───────────────────────────────────────────────────────────
    with tab_danger:
        st.subheader('⚠️ Danger Zone')
        st.error('**Clear All Database** will permanently delete ALL assemblies, '
                 'parts, and progress records. This cannot be undone.')
        confirm = st.checkbox('I understand this will delete everything permanently.')
        if confirm:
            if st.button('🗑 Clear All Database', type='primary'):
                db.clear_all_data()
                st.session_state.report_rows = []
                st.success('All database records have been cleared.')
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════
# Page: Raw Material Delivery
# ══════════════════════════════════════════════════════════════════════════════
def page_raw_material():
    st.header('📦 Raw Material Delivery')
    role = st.session_state.user['role']

    if role != 'viewer':
        col_add, col_imp = st.columns([1.4, 1])

        with col_add:
            with st.container(border=True):
                st.subheader('Add Received Material')
                with st.form('raw_form', clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    with c1:
                        recv_date   = st.date_input('Received Date', value=date.today())
                        do_no       = st.text_input('D.O. Number', placeholder='Delivery Order No.')
                        description = st.text_input('Description', placeholder='e.g. UB 356x171x45')
                    with c2:
                        grade    = st.text_input('Material Grade', placeholder='e.g. S275, S355')
                        qty      = st.number_input('Qty', min_value=0.0, format='%.2f')
                        total_kg = st.number_input('Total kg', min_value=0.0, format='%.2f')
                        remark   = st.text_area('Remark', height=70)
                    if st.form_submit_button('➕ Add', type='primary', use_container_width=True):
                        if not description:
                            st.error('Description is required.')
                        else:
                            db.add_raw_material(recv_date, do_no, description, grade, qty, total_kg, remark)
                            st.success('Raw material entry added.')
                            st.rerun()

        with col_imp:
            with st.container(border=True):
                st.subheader('Import from Excel')
                st.caption(
                    'Excel columns (row 1 header):\n'
                    '**Received Date · D.O. Number · Description · Grade · Qty · Remark**'
                )
                uploaded = st.file_uploader('Choose Excel file (.xlsx)', type=['xlsx', 'xls'],
                                            key='rm_upload')
                if uploaded and st.button('📥 Import', type='primary', use_container_width=True):
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                        tmp.write(uploaded.read())
                        tmp_path = tmp.name
                    try:
                        count, err = db.import_raw_materials_excel(tmp_path)
                        if err:
                            st.error(f'Import failed: {err}')
                        else:
                            st.success(f'Imported {count} records.')
                            st.session_state.rm_rows = []
                            st.rerun()
                    finally:
                        os.unlink(tmp_path)

    st.markdown('---')

    # ── Overall summary (always visible) ──────────────────────────────────────
    summ = db.get_raw_material_summary()
    sm1, sm2, sm3 = st.columns(3)
    sm1.metric('Total Entries', f"{summ['entries']:,}")
    sm2.metric('Total Qty Received', f"{summ['total_qty']:,.2f}")
    sm3.metric('Total kg Received', f"{summ['total_kg']:,.2f} kg")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        start = st.date_input('From', value=date.today() - timedelta(days=30), key='rm_start')
    with c2:
        end = st.date_input('To', value=date.today(), key='rm_end')

    bc1, bc2 = st.columns(2)
    with bc1:
        load     = st.button('🔍 Load by Date', type='primary', use_container_width=True)
    with bc2:
        load_all = st.button('📋 Show All', use_container_width=True)

    if 'rm_rows' not in st.session_state:
        st.session_state.rm_rows = []

    if load:
        st.session_state.rm_rows = db.get_raw_materials(str(start), str(end))
    if load_all:
        st.session_state.rm_rows = db.get_raw_materials()

    rows = st.session_state.rm_rows
    if rows:
        # ── Filtered summary ──────────────────────────────────────────────────
        filt_qty = sum(r.get('qty', 0) for r in rows)
        filt_kg  = sum(r.get('total_kg', 0) for r in rows)
        fc1, fc2, fc3 = st.columns(3)
        fc1.metric('Entries (filtered)', str(len(rows)))
        fc2.metric('Qty (filtered)', f'{filt_qty:,.2f}')
        fc3.metric('Total kg (filtered)', f'{filt_kg:,.2f} kg')

        st.markdown('')
        df = pd.DataFrame(rows)[['id', 'received_date', 'do_no', 'description', 'grade', 'qty', 'total_kg', 'remark']]
        df.columns = ['ID', 'Received Date', 'D.O. No.', 'Description', 'Grade', 'Qty', 'Total kg', 'Remark']
        st.dataframe(df, use_container_width=True, hide_index=True)

        ec1, ec2 = st.columns(2)
        with ec1:
            st.download_button('📥 Export CSV',
                               df.to_csv(index=False).encode('utf-8'),
                               f'raw_material_{date.today()}.csv', 'text/csv',
                               use_container_width=True)
        with ec2:
            buf = BytesIO()
            df.to_excel(buf, index=False, engine='openpyxl')
            st.download_button('📥 Export Excel', buf.getvalue(),
                               f'raw_material_{date.today()}.xlsx',
                               'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                               use_container_width=True)

        if role != 'viewer':
            with st.form('del_rm'):
                del_id = st.number_input('Delete entry by ID', min_value=0, step=1, value=0)
                if st.form_submit_button('🗑 Delete', type='secondary'):
                    if del_id > 0:
                        db.delete_raw_material(int(del_id))
                        st.success(f'Deleted entry #{int(del_id)}')
                        st.rerun()
    else:
        st.info('Click **Load by Date** or **Show All** to display records.')


# ══════════════════════════════════════════════════════════════════════════════
def main():
    db.init()

    if 'user' not in st.session_state:
        show_login()
        return

    show_sidebar()

    page = st.session_state.get('page', '✏️ Daily Entry')

    if '✏️' in page:
        page_daily_entry()
    elif '📅' in page:
        page_report()
    elif '📊' in page:
        page_progress()
    elif '🚚' in page:
        page_delivery()
    elif '📦' in page:
        page_raw_material()
    elif '⚙️' in page:
        page_manage()


if __name__ == '__main__':
    main()
