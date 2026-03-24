import streamlit as st
import boto3
import os
import pandas as pd
import math
import io
import zipfile
import cv2
from thefuzz import fuzz
from botocore.exceptions import ClientError

st.set_page_config(page_title="Wasabi Cloud Explorer", layout="wide")
st.title("🗂️ Wasabi Cloud Explorer")

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
if "batch_gen" not in st.session_state: st.session_state.batch_gen = None

def change_dir(new_path):
    st.session_state.current_path = new_path
    st.session_state.page = 0
    st.session_state.selected_files = set()

# --- MOTORE S3 (NASCONDE LE CARTELLE TAGSPACES) ---
def is_valid_s3_item(key, prefix):
    # Ignora il file stesso e NASCONDE tutte le directory nascoste di TagSpaces (.ts)
    return key != prefix and '/.ts/' not in key and not key.startswith('.ts/')

def get_s3_items(prefix, query, scope):
    folders = []
    files = []
    if query and scope == "Globale (Cerca in tutto il bucket)":
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        for page in pages:
            for c in page.get('Contents', []):
                if not c['Key'].endswith('/') and is_valid_s3_item(c['Key'], prefix):
                    files.append(c)
    else:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        # Nascondiamo le cartelle .ts visivamente
        folders = [p['Prefix'] for p in response.get('CommonPrefixes', []) if not p['Prefix'].endswith('.ts/')]
        files = [c for c in response.get('Contents', []) if is_valid_s3_item(c['Key'], prefix)]
        
    return folders, files

def is_match(filename, query, mode):
    if not query: return True
    q, f = query.lower(), filename.lower()
    if mode == "🧠 Smart (Parole libere)": return all(word in f for word in q.split())
    elif mode == "✨ Fuzzy (Tollera errori)": return fuzz.token_set_ratio(q, f) >= 70
    else: return q in f

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

# --- COMPATIBILITÀ TAGSPACES (.ts) ---
def get_ts_thumbnail_key(file_key):
    """Calcola il percorso esatto della miniatura richiesto da TagSpaces (es. cartella/.ts/video.mp4.jpg)"""
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
        cap.set(cv2.CAP_PROP_POS_FRAMES, 30) # Estrae il fotogramma al 30° istante
        ret, frame = cap.read()
        cap.release()
        if ret:
            _, buffer = cv2.imencode('.jpg', frame)
            # Salva la miniatura nella cartella nascosta .ts
            s3.put_object(Bucket=BUCKET_NAME, Key=thumb_key, Body=io.BytesIO(buffer).getvalue(), ContentType='image/jpeg')
            return True
    except: pass
    return False

# --- INTERFACCIA UTENTE ---
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    if st.button("🏠 Home"): change_dir("")
with col2:
    search_query = st.text_input("🔍 Cerca file e cartelle...", "")
    with st.expander("⚙️ Opzioni di Ricerca"):
        search_scope = st.radio("Raggio d'azione:", ["Locale (Solo questa cartella)", "Globale (Cerca in tutto il bucket)"], horizontal=True)
        search_mode = st.radio("Metodo:", ["🧠 Smart (Parole libere)", "✨ Fuzzy (Tollera errori)", "📏 Esatta"], horizontal=True)
with col3:
    st.write(f"**Percorso:** `/{st.session_state.current_path}`")

# --- GENERATORE MASSIVO TAGSPACES ---
with st.expander("🛠️ Generazione Massiva Anteprime (Compatibile TagSpaces)"):
    st.write("Cerca video privi di anteprima e le genera salvandole nelle cartelle nascoste `.ts`.")
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        if st.button("Genera per QUESTA cartella"): st.session_state.batch_gen = "local"
    with col_g2:
        if st.button("Genera GLOBALE (Intero Database)"): st.session_state.batch_gen = "global"

if st.session_state.batch_gen:
    mode = st.session_state.batch_gen
    st.session_state.batch_gen = None
    with st.spinner("Scansione database in corso..."):
        _, files_to_check = get_s3_items("" if mode == "global" else st.session_state.current_path, "", "Globale (Cerca in tutto il bucket)" if mode == "global" else "Locale")
        videos = [f['Key'] for f in files_to_check if f['Key'].lower().endswith(('.mp4', '.mov', '.webm', '.avi', '.mkv'))]
    
    if not videos: st.info("Nessun video trovato.")
    else:
        st.write("Generazione in corso. Non chiudere la pagina...")
        progress_bar = st.progress(0)
        status_text = st.empty()
        count = 0
        for i, v_key in enumerate(videos):
            if not get_thumbnail_url(v_key):
                status_text.text(f"Elaborazione: {os.path.basename(v_key)} ({i+1}/{len(videos)})")
                if generate_and_upload_thumbnail(v_key): count += 1
            progress_bar.progress((i + 1) / len(videos))
        status_text.text(f"Completato! Generate {count} nuove anteprime su {len(videos)} video controllati.")
        st.success("Operazione conclusa con successo!")

st.divider()

col_v, col_s, col_opt, col_pag = st.columns([2, 1, 1, 1])
with col_v: view_mode = st.radio("Modalità:", ["🖼️ Griglia (Anteprime)", "📝 Lista (Veloce)"], horizontal=True)
with col_s: sort_mode = st.selectbox("Ordina per:", ["Nome (A-Z)", "Nome (Z-A)", "Più recenti", "Dimensione"])
with col_opt: skip_pending = st.checkbox("🚫 Ignora '.pending'", value=True)
with col_pag: items_per_page = st.selectbox("File per pagina:", [10, 25, 50, 100], index=0)

st.divider()

with st.spinner("Caricamento..."):
    folders, files = get_s3_items(st.session_state.current_path, search_query, search_scope)

if folders and search_scope == "Locale (Solo questa cartella)":
    st.subheader("📁 Cartelle")
    cols = st.columns(4)
    filtered_folders = [f for f in folders if is_match(os.path.basename(f.strip('/')), search_query, search_mode)]
    for i, folder in enumerate(filtered_folders):
        folder_name = folder.replace(st.session_state.current_path, "").strip("/")
        with cols[i % 4]:
            if st.button(f"📂 {folder_name}", key=folder):
                change_dir(folder)
                st.rerun()

if files:
    st.subheader("📄 File")
    filtered_files = [f for f in files if is_match(os.path.basename(f['Key']), search_query, search_mode)]
    
    if sort_mode == "Nome (A-Z)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower())
    elif sort_mode == "Nome (Z-A)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower(), reverse=True)
    elif sort_mode == "Più recenti": filtered_files.sort(key=lambda x: x['LastModified'], reverse=True)
    elif sort_mode == "Dimensione": filtered_files.sort(key=lambda x: x['Size'], reverse=True)

    if st.session_state.selected_files:
        st.success(f"Hai selezionato {len(st.session_state.selected_files)} file.")
        if st.button("📦 Scarica Selezionati (ZIP Veloce)"):
            with st.spinner("Creazione ZIP in corso..."):
                zip_data = create_uncompressed_zip(st.session_state.selected_files)
                st.download_button("⬇️ Salva ZIP", data=zip_data, file_name="wasabi_download.zip", mime="application/zip")

    total_files = len(filtered_files)
    if total_files == 0: st.warning("Nessun file trovato.")
    else:
        total_pages = math.ceil(total_files / items_per_page)
        if st.session_state.page >= total_pages: st.session_state.page = max(0, total_pages - 1)
        
        if total_files > items_per_page:
            pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])
            with pag_col1:
                if st.button("⬅️ Precedente", disabled=(st.session_state.page == 0)): st.session_state.page -= 1; st.rerun()
            with pag_col2: st.markdown(f"<div style='text-align: center'>Pagina <b>{st.session_state.page + 1}</b> di {total_pages}</div>", unsafe_allow_html=True)
            with pag_col3:
                if st.button("Avanti ➡️", disabled=(st.session_state.page >= total_pages - 1)): st.session_state.page += 1; st.rerun()

        start_idx = st.session_state.page * items_per_page
        paginated_files = filtered_files[start_idx : start_idx + items_per_page]

        if view_mode == "🖼️ Griglia (Anteprime)":
            cols = st.columns(4)
            for i, file_obj in enumerate(paginated_files):
                file_key = file_obj['Key']
                file_name = os.path.basename(file_key)
                
                with cols[i % 4]:
                    with st.container(border=True):
                        display_name = f"📂 {os.path.dirname(file_key)}/\n{file_name}" if search_scope == "Globale (Cerca in tutto il bucket)" else file_name
                        is_selected = file_key in st.session_state.selected_files
                        if st.checkbox(f"{display_name}", value=is_selected, key=f"chk_{file_key}"): st.session_state.selected_files.add(file_key)
                        else: st.session_state.selected_files.discard(file_key)
                        
                        st.caption(f"{(file_obj['Size'] / 1048576):.2f} MB")
                        
                        if file_name.startswith(".pending") and skip_pending: st.info("🚫 File in lavorazione")
                        else:
                            url = get_presigned_url(file_key)
                            ext = file_name.split('.')[-1].lower()
                            
                            if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']: 
                                st.image(url, use_container_width=True)
                            elif ext in ['mp4', 'mov', 'webm', 'avi', 'mkv']: 
                                thumb_url = get_thumbnail_url(file_key)
                                # GENERAZIONE AUTOMATICA ISTANTANEA:
                                if not thumb_url:
                                    with st.spinner("📸 Creazione miniatura..."):
                                        if generate_and_upload_thumbnail(file_key):
                                            thumb_url = get_presigned_url(get_ts_thumbnail_key(file_key))
                                
                                if thumb_url:
                                    st.image(thumb_url, use_container_width=True)
                                    with st.expander("▶️ Riproduci"): st.video(url)
                                else:
                                    st.warning("Impossibile generare anteprima.")
                                    with st.expander("▶️ Riproduci"): st.video(url)
                            else: st.write("*(Nessuna anteprima)*")
                            
                        st.markdown(f"[⬇️ Scarica Singolo]({url})")

        elif view_mode == "📝 Lista (Veloce)":
            all_keys_on_page = [f['Key'] for f in paginated_files]
            selected_in_list = st.multiselect("Aggiungi file allo ZIP:", options=all_keys_on_page, default=[k for k in all_keys_on_page if k in st.session_state.selected_files], format_func=lambda x: x if search_scope == "Globale (Cerca in tutto il bucket)" else os.path.basename(x))
            for key in all_keys_on_page:
                if key in selected_in_list: st.session_state.selected_files.add(key)
                else: st.session_state.selected_files.discard(key)

            file_data = [{"Percorso/Nome": f['Key'] if search_scope == "Globale (Cerca in tutto il bucket)" else os.path.basename(f['Key']), "MB": round(f['Size'] / 1048576, 2), "Data": f['LastModified'].strftime("%Y-%m-%d"), "Download": get_presigned_url(f['Key'])} for f in paginated_files]
            if file_data: st.dataframe(pd.DataFrame(file_data), column_config={"Download": st.column_config.LinkColumn("Link", display_text="⬇️ Scarica")}, hide_index=True, use_container_width=True)

if not folders and not files: st.info("Nessun contenuto in questa cartella.")
