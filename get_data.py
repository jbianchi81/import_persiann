import requests
from datetime import datetime, timedelta
import os

# Carpeta de salida
output_dir = "descargas_persiann"
os.makedirs(output_dir, exist_ok=True)

# Fecha inicial y final
start_date = datetime(2013, 12, 6)
end_date = datetime.today()

# URL base
base_url = "http://persiann.eng.uci.edu/CHRSdata/PERSIANN/daily/ms6s4_d{code}.bin.gz"

# Iteración diaria
for delta in range((end_date - start_date).days + 1):
    date = start_date + timedelta(days=delta)
    year_code = date.year % 100  # últimos dos dígitos del año
    julian_day = date.timetuple().tm_yday
    code = f"{year_code:02d}{julian_day:03d}"
    url = base_url.format(code=code)
    filename = f"persiann_{date.strftime('%Y%m%d')}.bin.gz"
    filepath = os.path.join(output_dir, filename)

    # Evitar re-descarga
    if os.path.exists(filepath):
        continue

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f" Descargado: {filename}")
        else:
            print(f"⚠️ No disponible: {filename} (HTTP {response.status_code})")
    except Exception as e:
        print(f" Error en {filename}: {e}")
