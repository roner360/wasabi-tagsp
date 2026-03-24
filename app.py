import streamlit as st
import boto3
import os
from botocore.exceptions import ClientError

# --- CONFIGURAZIONE PAGINA ---
st.set_page_config(page_title="Wasabi Cloud Explorer", layout="wide")
st.title("🗂️ Wasabi Cloud Explorer")

# --- INIZIALIZZAZIONE CLIENT WASABI (S3 compatibile) ---
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

# --- GESTIONE STATO DI NAVIGAZIONE ---
if "current_path" not in st.session_state:
    st.session_state.current_path = ""

def change_dir(new_path):
    st.session_state.current_path = new_path

# --- FUNZIONI DI SUPPORTO ---
def list_s3_objects(prefix):
    """Recupera cartelle (CommonPrefixes) e file (Contents) dal bucket."""
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    
    folders = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
    # Filtra i file escludendo la 'cartella' stessa se appare come oggetto
    files = [c for c in response.get('Contents', []) if c['Key'] != prefix]
    
    return folders, files

def get_presigned_url(file_key):
    """Genera un URL temporaneo per visualizzare immagini/video in modo sicuro."""
    try:
        url = s3.generate_presigned_url('get_object',
                                        Params={'Bucket': BUCKET_NAME, 'Key': file_key},
                                        ExpiresIn=3600) # Scade in 1 ora
        return url
    except ClientError as e:
        st.error(f"Errore nella generazione dell'URL: {e}")
        return None

# --- UI DI NAVIGAZIONE ---
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("🏠 Home (Radice)"):
        change_dir("")

with col2:
    st.write(f"**Percorso attuale:** `/{st.session_state.current_path}`")

st.divider()

# --- RECUPERO E VISUALIZZAZIONE CONTENUTI ---
folders, files = list_s3_objects(st.session_state.current_path)

# Mostra le Cartelle
if folders:
    st.subheader("📁 Cartelle")
    cols = st.columns(4)
    for i, folder in enumerate(folders):
        folder_name = folder.replace(st.session_state.current_path, "").strip("/")
        with cols[i % 4]:
            if st.button(f"📂 {folder_name}", key=folder):
                change_dir(folder)
                st.rerun()

# Mostra i File (Immagini e Video)
if files:
    st.subheader("📄 File (Immagini e Video)")
    
    # Crea una griglia per i file
    cols = st.columns(3)
    for i, file_obj in enumerate(files):
        file_key = file_obj['Key']
        file_name = os.path.basename(file_key)
        file_size_mb = file_obj['Size'] / (1024 * 1024)
        
        with cols[i % 3]:
            with st.container(border=True):
                st.write(f"**{file_name}** ({file_size_mb:.2f} MB)")
                
                url = get_presigned_url(file_key)
                
                if url:
                    # Riconosci il tipo di file dall'estensione
                    ext = file_name.split('.')[-1].lower()
                    if ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                        st.image(url, use_container_width=True)
                    elif ext in ['mp4', 'mov', 'avi', 'mkv']:
                        st.video(url)
                    else:
                        st.write("*(Anteprima non disponibile per questo formato)*")
                
                # Bottone per il Download diretto scaricando l'oggetto in memoria
                # Nota: per file enormi (es. video giganti), scaricarli tutti in memoria potrebbe rallentare l'app.
                # Per file standard va benissimo.
                file_data = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)['Body'].read()
                st.download_button(
                    label="⬇️ Scarica",
                    data=file_data,
                    file_name=file_name,
                    mime="application/octet-stream",
                    key=f"dl_{file_key}"
                )

if not folders and not files:
    st.info("La cartella è vuota.")
