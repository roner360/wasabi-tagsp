import streamlit as st
import boto3
import os
import pandas as pd
import math
import io
import zipfile
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

# --- GESTIONE STATO ---
if "current_path" not in st.session_state: st.session_state.current_path = ""
if "page" not in st.session_state: st.session_state.page = 0
if "selected_files" not in st.session_state: st.session_state.selected_files = set()

def change_dir(new_path):
    st.session_state.current_path = new_path
    st.session_state.page = 0 # Resetta la pagina
    st.session_state.selected_files = set() # Resetta la selezione

def list_s3_objects(prefix):
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    folders = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
    files = [c for c in response.get('Contents', []) if c['Key'] != prefix]
    return folders, files

def get_presigned_url(file_key, expires_in=3600):
    try:
        return s3.generate_presigned_url('get_object',
                                        Params={'Bucket': BUCKET_NAME, 'Key': file_key},
                                        ExpiresIn=expires_in)
    except ClientError:
        return None

def create_uncompressed_zip(file_keys):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zip_file:
        for key in file_keys:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            file_data = response['Body'].read()
            filename = os.path.basename(key)
            zip_file.writestr(filename, file_data)
    return zip_buffer.getvalue()

# --- BARRA DI NAVIGAZIONE ---
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    if st.button("🏠 Home"): change_dir("")
with col2:
    search_query = st.text_input("🔍 Cerca...", "")
with col3:
    st.write(f"**Percorso:** `/{st.session_state.current_path}`")

st.divider()

# --- OPZIONI DI VISUALIZZAZIONE E PAGINAZIONE ---
col_v, col_s, col_opt, col_pag = st.columns([2, 1, 1, 1])
with col_v:
    view_mode = st.radio("Modalità:", ["🖼️ Griglia (Anteprime)", "📝 Lista (Veloce)"], horizontal=True)
with col_s:
    sort_mode = st.selectbox("Ordina per:", ["Nome (A-Z)", "Nome (Z-A)", "Più recenti", "Dimensione"])
with col_opt:
    st.write("") # Spazio per allineare verticalmente il checkbox
    skip_pending = st.checkbox("🚫 Ignora '.pending'", value=True)
with col_pag:
    # --- NUOVO: SELETTORE FILE PER PAGINA (Default: 10) ---
    items_per_page = st.selectbox("File per pagina:", [10, 25, 50, 100], index=0)

st.divider()

# --- RECUPERO E FILTRAGGIO ---
with st.spinner("Caricamento..."):
    folders, files = list_s3_objects(st.session_state.current_path)

# MOSTRA CARTELLE
if folders:
    st.subheader("📁 Cartelle")
    cols = st.columns(4)
    filtered_folders = [f for f in folders if search_query.lower() in f.lower()]
    for i, folder in enumerate(filtered_folders):
        folder_name = folder.replace(st.session_state.current_path, "").strip("/")
        with cols[i % 4]:
            if st.button(f"📂 {folder_name}", key=folder):
                change_dir(folder)
                st.rerun()

# MOSTRA FILE
if files:
    st.subheader("📄 File")
    
    # Filtraggio e Ordinamento
    filtered_files = [f for f in files if search_query.lower() in os.path.basename(f['Key']).lower()]
    if sort_mode == "Nome (A-Z)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower())
    elif sort_mode == "Nome (Z-A)": filtered_files.sort(key=lambda x: os.path.basename(x['Key']).lower(), reverse=True)
    elif sort_mode == "Più recenti": filtered_files.sort(key=lambda x: x['LastModified'], reverse=True)
    elif sort_mode == "Dimensione": filtered_files.sort(key=lambda x: x['Size'], reverse=True)

    # --- DOWNLOAD MULTIPLO (ZIP) ---
    if st.session_state.selected_files:
        st.success(f"Hai selezionato {len(st.session_state.selected_files)} file.")
        if st.button("📦 Scarica Selezionati (ZIP a Compressione Zero)"):
            with st.spinner("Creazione ZIP in corso..."):
                zip_data = create_uncompressed_zip(st.session_state.selected_files)
                st.download_button("⬇️ Clicca qui per salvare lo ZIP", data=zip_data, file_name="wasabi_download.zip", mime="application/zip")

    # --- LOGICA PAGINAZIONE DINAMICA ---
    total_files = len(filtered_files)
    total_pages = math.ceil(total_files / items_per_page) if total_files > 0 else 1
    
    # Previene errori se cambi da 10 a 50 e ti trovi in una pagina che non esiste più
    if st.session_state.page >= total_pages:
        st.session_state.page = max(0, total_pages - 1)
    
    # Mostra i bottoni di paginazione solo se i file superano il limite per pagina
    if total_files > items_per_page:
        pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])
        with pag_col1:
            if st.button("⬅️ Precedente", disabled=(st.session_state.page == 0)):
                st.session_state.page -= 1
                st.rerun()
        with pag_col2:
            st.markdown(f"<div style='text-align: center'>Pagina <b>{st.session_state.page + 1}</b> di {total_pages} ({total_files} file totali)</div>", unsafe_allow_html=True)
        with pag_col3:
            if st.button("Avanti ➡️", disabled=(st.session_state.page >= total_pages - 1)):
                st.session_state.page += 1
                st.rerun()

    # Estrai solo i file della pagina corrente
    start_idx = st.session_state.page * items_per_page
    end_idx = start_idx + items_per_page
    paginated_files = filtered_files[start_idx:end_idx]

    # --- VISTA GRIGLIA ---
    if view_mode == "🖼️ Griglia (Anteprime)":
        cols = st.columns(4)
        for i, file_obj in enumerate(paginated_files):
            file_key = file_obj['Key']
            file_name = os.path.basename(file_key)
            is_pending = file_name.startswith(".pending")
            
            with cols[i % 4]:
                with st.container(border=True):
                    is_selected = file_key in st.session_state.selected_files
                    if st.checkbox(f"{file_name}", value=is_selected, key=f"chk_{file_key}"):
                        st.session_state.selected_files.add(file_key)
                    else:
                        st.session_state.selected_files.discard(file_key)
                    
                    st.caption(f"{(file_obj['Size'] / 1048576):.2f} MB")
                    
                    if is_pending and skip_pending:
                        st.info("🚫 File in lavorazione")
                    else:
                        url = get_presigned_url(file_key)
                        ext = file_name.split('.')[-1].lower()
                        if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']: st.image(url, use_container_width=True)
                        elif ext in ['mp4', 'mov', 'webm']: st.video(url)
                        else: st.write("*(Nessuna anteprima)*")
                        
                    st.markdown(f"[⬇️ Scarica Singolo]({get_presigned_url(file_key)})")

    # --- VISTA LISTA ---
    elif view_mode == "📝 Lista (Veloce)":
        all_keys_on_page = [f['Key'] for f in paginated_files]
        selected_in_list = st.multiselect(
            "Seleziona i file da aggiungere allo ZIP (nella pagina corrente):",
            options=all_keys_on_page,
            default=[k for k in all_keys_on_page if k in st.session_state.selected_files],
            format_func=lambda x: os.path.basename(x)
        )
        
        for key in all_keys_on_page:
            if key in selected_in_list: st.session_state.selected_files.add(key)
            else: st.session_state.selected_files.discard(key)

        file_data = []
        for file_obj in paginated_files:
            file_key = file_obj['Key']
            file_name = os.path.basename(file_key)
            file_data.append({
                "Nome File": file_name,
                "Dimensione (MB)": round(file_obj['Size'] / 1048576, 2),
                "Ultima Modifica": file_obj['LastModified'].strftime("%Y-%m-%d %H:%M"),
                "Download": get_presigned_url(file_key)
            })
        
        if file_data:
            st.dataframe(
                pd.DataFrame(file_data),
                column_config={"Download": st.column_config.LinkColumn("Link", display_text="⬇️ Scarica")},
                hide_index=True, use_container_width=True
            )

if not folders and not files:
    st.info("La cartella è vuota.")
