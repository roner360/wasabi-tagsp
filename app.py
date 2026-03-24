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
    # Nota: list_objects_v2 restituisce max 1000 elementi alla volta.
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

# --- UI DI NAVIGAZIONE E RICERCA ---
col1, col2, col3 = st.columns([1, 3, 2])
with col1:
    if st.button("🏠 Home"):
        change_dir("")
with col2:
    st.write(f"**Percorso:** `/{st.session_state.current_path}`")
with col3:
    search_query = st.text_input("🔍 Cerca file qui...", "")

st.divider()

# --- RECUPERO CONTENUTI ---
with st.spinner("Caricamento in corso..."):
    folders, files = list_s3_objects(st.session_state.current_path)

# --- MOSTRA CARTELLE ---
if folders:
    st.subheader("📁 Cartelle")
    cols = st.columns(4)
    for i, folder in enumerate(folders):
        folder_name = folder.replace(st.session_state.current_path, "").strip("/")
        # Filtra anche le cartelle se c'è una ricerca
        if search_query.lower() in folder_name.lower() or search_query == "":
            with cols[i % 4]:
                if st.button(f"📂 {folder_name}", key=folder):
                    change_dir(folder)
                    st.rerun()

# --- MOSTRA FILE (TRAMITE TABELLA PANDAS) ---
if files:
    st.subheader("📄 File")
    
    # Costruiamo i dati per la tabella
    file_data = []
    for file_obj in files:
        file_key = file_obj['Key']
        file_name = os.path.basename(file_key)
        
        # Applica il filtro di ricerca
        if search_query.lower() in file_name.lower() or search_query == "":
            file_size_mb = file_obj['Size'] / (1024 * 1024)
            last_modified = file_obj['LastModified'].strftime("%Y-%m-%d %H:%M")
            download_url = get_presigned_url(file_key)
            
            file_data.append({
                "Nome File": file_name,
                "Dimensione (MB)": round(file_size_mb, 2),
                "Ultima Modifica": last_modified,
                "Download": download_url,
                "Key": file_key
            })

    if file_data:
        df = pd.DataFrame(file_data)
        
        # Configuriamo la tabella interattiva di Streamlit
        st.dataframe(
            df,
            column_config={
                "Nome File": st.column_config.TextColumn("Nome File"),
                "Dimensione (MB)": st.column_config.NumberColumn("MB", format="%.2f"),
                "Ultima Modifica": st.column_config.DatetimeColumn("Modificato il"),
                "Download": st.column_config.LinkColumn("Link", display_text="⬇️ Scarica"),
                "Key": None, # Nascondiamo la colonna Key interna
            },
            hide_index=True,
            use_container_width=True
        )
    elif search_query:
        st.warning("Nessun file corrisponde alla tua ricerca.")

if not folders and not files:
    st.info("La cartella è vuota.")
