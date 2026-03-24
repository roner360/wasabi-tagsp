import streamlit as st
import boto3
import os
import pandas as pd
import math
import io
import zipfile
import cv2
import json
from thefuzz import fuzz
from botocore.exceptions import ClientError

st.set_page_config(page_title="Wasabi Cloud Explorer", layout="wide")
st.title("🗂️ Wasabi Cloud Explorer")

# --- GESTIONE IMPOSTAZIONI ---
SETTINGS_FILE = "settings.json"
VALID_SMODES = [
    "Nome (A-Z)", "Nome (Z-A)", 
    "Data (Più recenti prima)", "Data (Più vecchi prima)", 
    "Dimensione (Maggiore prima)", "Dimensione (Minore prima)"
]

def load_settings():
    settings = {
        "folder_view": "📁 Griglia (Affiancate)",
        "hide_dot": True,
        "tsize": "Media (Default)",
        "vmode": "🖼️ Griglia (Anteprime)",
        "smode": "Data (Più recenti prima)",
        "ipp": 25,
        "max_results": 500
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                loaded = json.load(f)
                if loaded.get("smode") not in VALID_SMODES:
                    loaded["smode"] = "Data (Più recenti prima)"
                settings.update(loaded)
        except: pass
    return settings

def save_settings():
    settings = {
        "folder_view": st.session_state.folder_view,
        "hide_dot": st.session_state.hide_dot,
        "tsize": st.session_state.tsize,
        "vmode": st.session_state.vmode,
        "smode": st.session_state.smode,
        "ipp": st.session_state.ipp,
        "max_results": st.session_state.max_results
    }
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f)

if "settings_loaded" not in st.session_state:
    st.session_state.update(load_settings())
    st.session_state.settings_loaded = True

@st.cache_resource
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=st.secrets["wasabi"]["ENDPOINT_URL"],
        aws_access_key_id=st.secrets["wasabi"]["ACCESS_KEY"],
        aws_secret_access_key=st.secrets["wasabi"]["SECRET_KEY"],
        region_name=st.secrets["wasabi"]["REGION"]
    )

s3 = get_s3_client()
BUCKET_NAME = st.secrets["wasabi"]["BUCKET_NAME"]

if "current_path" not in st.session_state: st.session_state.current_path = ""
if "page" not in st.session_state: st.session_state.page = 0
if "selected_files" not in st.session_state: st.session_state.selected_files = set()

def change_dir(new_path):
    st.session_state.current_path = new_path
    st.session_state.page = 0

def is_valid_s3_item(key, prefix):
    return key != prefix and '/.ts/' not in key and not key.startswith('.ts/')

def is_match(filename, query, mode):
    if not query: return True
    q, f = query.lower(), filename.lower()
    if mode == "🧠 Smart (Parole libere)": return all(word in f for word in q.split())
    elif mode == "✨ Fuzzy (Tollera errori)": return fuzz.token_set_ratio(q, f) >= 70
    else: return q in f

@st.cache_data(ttl=300, show_spinner=False)
def fetch_s3_data_cached(prefix, query, scope, max_res, search_mode):
    folders = []
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    if query and scope == "Globale (Cerca in tutto il bucket)":
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        for page in pages:
            for c in page.get('Contents', []):
                if not c['Key'].endswith('/') and is_valid_s3_item(c['Key'], prefix):
                    if is_match(os.path.basename(c['Key']), query, search_mode):
                        files.append(c)
                        if max_res != "Nessun limite" and len(files) >= int(max_res):
                            return folders, files
    else:
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        for page in pages:
            for p in page.get('CommonPrefixes', []):
                if not p['Prefix'].endswith('.ts/'):
                    folders.append(p['Prefix'])
            for c in page.get('Contents', []):
                if is_valid_s3_item(c['Key'], prefix):
                    files.append(c)
    return folders, files

def get_presigned_url(file_key, expires_in=3600):
    try: return s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': file_key}, ExpiresIn=expires_in)
    except ClientError: return None

def create_uncompressed_zip(file_keys):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zip_file:
        for key in file_keys:
            file_data = s3.get_object(Bucket=BUCKET_NAME, Key=key)['Body'].read()
            zip_file.writestr(os.path.basename(key), file_data)
    return zip_buffer.getvalue()

def get_ts_thumbnail_key(file_key):
    dirname = os.path.dirname(file_key)
    basename = os.path.basename(file_key)
    return f"{dirname}/.ts/{basename}.jpg" if dirname else f".ts/{basename}.jpg"

def get_thumbnail_url(file_key):
    thumb_key = get_ts_thumbnail_key(file_key)
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=thumb_key)
        return get_presigned_url(thumb_key)
    except ClientError: return None

def generate_and_upload_thumbnail(file_key):
    thumb_key = get_ts_thumbnail_key(file_key)
    video_url = get_presigned_url(file_key)
    try:
        cap = cv2.VideoCapture(video_url)
        cap.set(cv2.CAP_PROP_POS_FRAMES, 30)
        ret, frame = cap.read()
        cap.release()
        if ret:
            _, buffer = cv2.imencode('.jpg', frame)
            s3.put_object(Bucket=BUCKET_NAME, Key=thumb_key, Body=io.BytesIO(buffer).getvalue(), ContentType='image/jpeg')
            return True
    except: pass
    return False

def render_pagination_buttons(position_key, total_pages):
    if total_pages > 1:
        pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])
        with pag_col1:
            if st.button("⬅️ Precedente", disabled=(st.session_state.page == 0), key=f"prev_{position_key}"): 
                st.session_state.page -= 1
                st.rerun()
        with pag_col2: 
            st.markdown(f"<div style='text-align: center'>Pagina <b>{st.session_state.page + 1}</b> di {total_pages}</div>", unsafe_allow_html=True)
        with pag_col3:
            if st.button("Avanti ➡️", disabled=(st.session_state.page >= total_pages - 1), key=f"next_{position_key}"): 
                st.session_state.page += 1
                st.rerun()

# --- FUNZIONE DI AGGIORNAMENTO SELEZIONE (LA NUOVA LOGICA VELOCE) ---
def process_form_selection(current_page_files):
    """Scorre tutti i file della pagina e legge la loro spunta dal form"""
    for f in current_page_files:
        chk_key = f"form_chk_{f['Key']}"
        if chk_key in st.session_state:
            if st.session_state[chk_key]:
                st.session_state.selected_files.add(f['Key'])
            else:
                st.session_state.selected_files.discard(f['Key'])

# --- UI BARRA SUPERIORE ---
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button("🏠 Home", use_container_width=True): change_dir("")
    with btn_col2:
        if st.button("🔄 Aggiorna", use_container_width=True): 
            fetch_s3_data_cached.clear()
            st.rerun()
with col2:
    search_query = st.text_input("🔍 Cerca file e cartelle...", "")
    with st.expander("⚙️ Opzioni di Ricerca"):
        search_scope = st.radio("Raggio d'azione:", ["Locale (Solo questa cartella)", "Globale (Cerca in tutto il bucket)"], horizontal=True)
        search_mode = st.radio("Metodo:", ["🧠 Smart (Parole libere)", "✨ Fuzzy (Tollera errori)", "📏 Esatta"], horizontal=True)
        st.selectbox("Ferma ricerca dopo aver trovato X file:", [100, 500, 1000, "Nessun limite"], key="max_results", on_change=save_settings)
with col3:
    st.write(f"**Percorso:** `/{st.session_state.current_path}`")

# --- UI OPZIONI DI VISUALIZZAZIONE ---
with st.expander("👁️ Impostazioni Visualizzazione", expanded=False):
    c_v1, c_v2, c_v3 = st.columns(3)
    with c_v1: st.radio("Vista Cartelle:", ["📁 Griglia (Affiancate)", "📝 Lista Compatta (Verticale)"], horizontal=True, key="folder_view", on_change=save_settings)
    with c_v2: st.checkbox("🚫 Nascondi file/cartelle nascoste (es. '.pending')", key="hide_dot", on_change=save_settings)
    with c_v3: st.selectbox("Dimensione Miniature:", ["Molto Grande", "Grande", "Media (Default)", "Piccola", "Piccolissima"], key="tsize", on_change=save_settings)

st.divider()

col_v, col_s, col_pag = st.columns([2, 1, 1])
with col_v: st.radio("Modalità File:", ["🖼️ Griglia (Anteprime)", "📝 Lista (Veloce)"], horizontal=True, key="vmode", on_change=save_settings)
with col_s: st.selectbox("Ordina File per:", VALID_SMODES, key="smode", on_change=save_settings)
with col_pag: st.selectbox("File per pagina:", [10, 25, 50, 100], key="ipp", on_change=save_settings)

st.divider()

with st.spinner("Connessione a Wasabi in corso..."):
    folders, files = fetch_s3_data_cached(st.session_state.current_path, search_query, search_scope, st.session_state.max_results, search_mode)

# --- FILTRO E RENDERING CARTELLE ---
if folders and search_scope == "Locale (Solo questa cartella)":
    st.subheader("📁 Cartelle")
    filtered_folders = [f for f in folders if not (st.session_state.hide_dot and os.path.basename(f.strip('/')).startswith('.')) and is_match(os.path.basename(f.strip('/')), search_query, search_mode)]

    if st.session_state.folder_view == "📁 Griglia (Affiancate)":
        cols = st.columns(4)
        for i, folder in enumerate(filtered_folders):
            folder_name = folder.replace(st.session_state.current_path, "").strip("/")
            with cols[i % 4]:
                if st.button(f"📂 {folder_name}", key=f"dir_{folder}", use_container_width=True):
                    change_dir(folder)
                    st.rerun()
    else:
        for folder in filtered_folders:
            folder_name = folder.replace(st.session_state.current_path, "").strip("/")
            if st.button(f"📂 {folder_name}", key=f"dir_{folder}"):
                change_dir(folder)
                st.rerun()

# --- FILTRO FILE E CARRELLO DOWNLOAD ---
if files:
    st.subheader("📄 File")
    filtered_files = [f for f in files if not (st.session_state.hide_dot and os.path.basename(f['Key']).startswith('.')) and is_match(os.path.basename(f['Key']), search_query, search_mode)]
    
    if st.session_state.smode == "Nome (A-Z)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower())
    elif st.session_state.smode == "Nome (Z-A)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower(), reverse=True)
    elif st.session_state.smode == "Data (Più recenti prima)": filtered_files.sort(key=lambda x: x['LastModified'], reverse=True)
    elif st.session_state.smode == "Data (Più vecchi prima)": filtered_files.sort(key=lambda x: x['LastModified'])
    elif st.session_state.smode == "Dimensione (Maggiore prima)": filtered_files.sort(key=lambda x: x['Size'], reverse=True)
    elif st.session_state.smode == "Dimensione (Minore prima)": filtered_files.sort(key=lambda x: x['Size'])

    # PANNELLO CARRELLO DOWNLOAD
    if st.session_state.selected_files:
        st.success(f"🛒 Hai {len(st.session_state.selected_files)} file nel carrello pronti per il download.")
        c_down1, c_down2 = st.columns([3, 1])
        with c_down1:
            if st.button("📦 Prepara ZIP per il Download", use_container_width=True):
                with st.spinner("Creazione ZIP in corso..."):
                    st.session_state.zip_data = create_uncompressed_zip(st.session_state.selected_files)
                    st.session_state.show_download = True
            
            if st.session_state.get("show_download", False):
                st.download_button("⬇️ Clicca qui per Salvare il file ZIP nel PC", data=st.session_state.zip_data, file_name="wasabi_download.zip", mime="application/zip", use_container_width=True)
        with c_down2:
            if st.button("🗑️ Svuota Carrello", use_container_width=True):
                st.session_state.selected_files.clear()
                st.session_state.show_download = False
                st.rerun()

    total_files = len(filtered_files)
    if total_files == 0: 
        if search_query: st.warning("Nessun file corrisponde alla ricerca corrente.")
    else:
        total_pages = math.ceil(total_files / st.session_state.ipp)
        if st.session_state.page >= total_pages: st.session_state.page = max(0, total_pages - 1)
        
        render_pagination_buttons("top", total_pages)
        start_idx = st.session_state.page * st.session_state.ipp
        paginated_files = filtered_files[start_idx : start_idx + st.session_state.ipp]

        st.info("💡 **Modalità Selezione Veloce:** Spunta i file qui sotto (nessun caricamento!) e premi su **'✅ Conferma Selezione'** in fondo per aggiungerli al carrello.")

        # IL NUOVO RECINTO: Tutto dentro un "Form" che non fa laggare la pagina
        with st.form("selezione_file_form"):
            
            # --- VISTA GRIGLIA FILE ---
            if st.session_state.vmode == "🖼️ Griglia (Anteprime)":
                col_count_map = {"Molto Grande": 2, "Grande": 3, "Media (Default)": 4, "Piccola": 6, "Piccolissima": 8}
                num_cols = col_count_map[st.session_state.tsize]
                
                for row_idx in range(0, len(paginated_files), num_cols):
                    cols = st.columns(num_cols)
                    row_files = paginated_files[row_idx : row_idx + num_cols]
                    
                    for i, file_obj in enumerate(row_files):
                        file_key = file_obj['Key']
                        file_name = os.path.basename(file_key)
                        
                        with cols[i]:
                            with st.container(border=True):
                                display_name = f"📂 {os.path.dirname(file_key)}/\n{file_name}" if search_scope == "Globale (Cerca in tutto il bucket)" else file_name
                                if st.session_state.tsize in ["Piccola", "Piccolissima"] and len(display_name) > 25: 
                                    display_name = display_name[:22] + "..."
                                    
                                # Checkbox ultraveloce (aggiorna i dati solo al click del bottone finale)
                                st.checkbox(f"{display_name}", value=(file_key in st.session_state.selected_files), key=f"form_chk_{file_key}")
                                
                                st.caption(f"{(file_obj['Size'] / 1048576):.2f} MB")
                                url = get_presigned_url(file_key)
                                ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
                                
                                if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']: 
                                    st.image(url, use_container_width=True)
                                elif ext in ['mp4', 'mov', 'webm', 'avi', 'mkv'] or ext == '': 
                                    thumb_url = get_thumbnail_url(file_key)
                                    if not thumb_url:
                                        with st.spinner("📸 Creazione..."):
                                            if generate_and_upload_thumbnail(file_key):
                                                thumb_url = get_presigned_url(get_ts_thumbnail_key(file_key))
                                    
                                    if thumb_url:
                                        st.image(thumb_url, use_container_width=True)
                                        with st.expander("▶️ Play"): st.video(url, format="video/mp4")
                                    else:
                                        st.warning("Anteprima Fallita.")
                                        with st.expander("▶️ Play"): st.video(url, format="video/mp4")
                                else: st.write("*(Nessuna anteprima)*")
                                    
                                st.markdown(f"[⬇️ Scarica Singolo]({url})")

            # --- VISTA LISTA FILE ---
            elif st.session_state.vmode == "📝 Lista (Veloce)":
                st.markdown("---")
                col_h1, col_h2, col_h3, col_h4 = st.columns([1, 5, 2, 2])
                col_h1.markdown("**Seleziona**")
                col_h2.markdown("**Nome File**")
                col_h3.markdown("**Dimensione**")
                col_h4.markdown("**Download Diretto**")
                st.markdown("---")
                
                for f in paginated_files:
                    file_key = f['Key']
                    col1, col2, col3, col4 = st.columns([1, 5, 2, 2])
                    with col1:
                        st.checkbox("Seleziona", value=(file_key in st.session_state.selected_files), key=f"form_chk_{file_key}", label_visibility="collapsed")
                    with col2:
                        st.write(file_key if search_scope == "Globale (Cerca in tutto il bucket)" else os.path.basename(file_key))
                    with col3:
                        st.write(f"{round(f['Size']/1048576, 2)} MB")
                    with col4:
                        st.markdown(f"[⬇️ Scarica]({get_presigned_url(file_key)})")

            # IL BOTTONE CHE SALVA TUTTO IN UN COLPO SOLO
            st.write("")
            st.form_submit_button("✅ Conferma Selezione", on_click=process_form_selection, args=(paginated_files,), use_container_width=True)

        st.write("") 
        render_pagination_buttons("bottom", total_pages)

if not folders and not files: 
    if not search_query: st.info("Nessun contenuto in questa cartella.")
