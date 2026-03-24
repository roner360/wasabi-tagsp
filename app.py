import streamlit as st
import boto3
import os
import pandas as pd
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

if "current_path" not in st.session_state:
    st.session_state.current_path = ""

def change_dir(new_path):
    st.session_state.current_path = new_path

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
    except ClientError as e:
        return None

# --- BARRA DEGLI STRUMENTI (NAVIGAZIONE, RICERCA, VISTA, ORDINAMENTO) ---
col1, col2, col3 = st.columns([1, 2, 2])
with col1:
    if st.button("🏠 Home (Radice)"):
        change_dir("")
with col2:
    search_query = st.text_input("🔍 Cerca...", "")
with col3:
    st.write(f"**Percorso:** `/{st.session_state.current_path}`")

st.divider()

col_view, col_sort = st.columns(2)
with col_view:
    view_mode = st.radio("Modalità di visualizzazione:", ["📝 Lista (Veloce)", "🖼️ Griglia (Anteprime)"], horizontal=True)
with col_sort:
    sort_mode = st.selectbox("Ordina file per:", ["Nome (A-Z)", "Nome (Z-A)", "Più recenti", "Dimensione (Maggiore-Minore)"])

st.divider()

# --- RECUPERO CONTENUTI ---
with st.spinner("Caricamento in corso..."):
    folders, files = list_s3_objects(st.session_state.current_path)

# --- MOSTRA CARTELLE ---
if folders:
    st.subheader("📁 Cartelle")
    cols = st.columns(4)
    # Filtro ricerca per cartelle
    filtered_folders = [f for f in folders if search_query.lower() in f.lower() or search_query == ""]
    
    for i, folder in enumerate(filtered_folders):
        folder_name = folder.replace(st.session_state.current_path, "").strip("/")
        with cols[i % 4]:
            if st.button(f"📂 {folder_name}", key=folder):
                change_dir(folder)
                st.rerun()

# --- ORDINAMENTO E FILTRAGGIO FILE ---
if files:
    st.subheader("📄 File")
    
    # 1. Filtro Ricerca
    filtered_files = [f for f in files if search_query.lower() in os.path.basename(f['Key']).lower() or search_query == ""]
    
    # 2. Ordinamento
    if sort_mode == "Nome (A-Z)":
        filtered_files = sorted(filtered_files, key=lambda x: os.path.basename(x['Key']).lower())
    elif sort_mode == "Nome (Z-A)":
        filtered_files = sorted(filtered_files, key=lambda x: os.path.basename(x['Key']).lower(), reverse=True)
    elif sort_mode == "Più recenti":
        filtered_files = sorted(filtered_files, key=lambda x: x['LastModified'], reverse=True)
    elif sort_mode == "Dimensione (Maggiore-Minore)":
        filtered_files = sorted(filtered_files, key=lambda x: x['Size'], reverse=True)

    if not filtered_files:
        st.warning("Nessun file corrisponde alla tua ricerca.")
    else:
        # --- VISTA LISTA (TABELLA PANDAS) ---
        if view_mode == "📝 Lista (Veloce)":
            file_data = []
            for file_obj in filtered_files:
                file_key = file_obj['Key']
                file_name = os.path.basename(file_key)
                file_data.append({
                    "Nome File": file_name,
                    "Dimensione (MB)": round(file_obj['Size'] / (1024 * 1024), 2),
                    "Ultima Modifica": file_obj['LastModified'].strftime("%Y-%m-%d %H:%M"),
                    "Download": get_presigned_url(file_key)
                })
            
            df = pd.DataFrame(file_data)
            st.dataframe(
                df,
                column_config={
                    "Nome File": st.column_config.TextColumn("Nome File"),
                    "Dimensione (MB)": st.column_config.NumberColumn("MB", format="%.2f"),
                    "Ultima Modifica": st.column_config.DatetimeColumn("Modificato il"),
                    "Download": st.column_config.LinkColumn("Link", display_text="⬇️ Scarica / Apri")
                },
                hide_index=True,
                use_container_width=True
            )

        # --- VISTA GRIGLIA (ANTEPRIME) ---
        elif view_mode == "🖼️ Griglia (Anteprime)":
            cols = st.columns(3) # Cambia a 4 o 5 se vuoi miniature più piccole
            for i, file_obj in enumerate(filtered_files):
                file_key = file_obj['Key']
                file_name = os.path.basename(file_key)
                file_size_mb = file_obj['Size'] / (1024 * 1024)
                
                with cols[i % 3]:
                    with st.container(border=True):
                        st.write(f"**{file_name}**")
                        st.caption(f"{file_size_mb:.2f} MB")
                        
                        url = get_presigned_url(file_key)
                        ext = file_name.split('.')[-1].lower()
                        
                        if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                            st.image(url, use_container_width=True)
                        elif ext in ['mp4', 'mov', 'webm']:
                            st.video(url)
                        else:
                            st.info("Anteprima non disponibile")
                            
                        st.markdown(f"[⬇️ Scarica File]({url})")

if not folders and not files:
    st.info("La cartella è vuota.")
