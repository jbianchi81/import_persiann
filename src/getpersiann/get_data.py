import argparse
import requests
from datetime import datetime, timedelta
import os

def valid_datetime(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'. Expected format: YYYY-MM-DD"
        raise argparse.ArgumentTypeError(msg)

# Carpeta de salida
defaults = {
    "output_dir": "descargas_persiann"
}

def download(start_date : datetime, end_date : datetime, output_dir : str=defaults["output_dir"]):

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


def main():
    parser = argparse.ArgumentParser(
    prog='downloadpersiann',
    description='Downloads persiann data')
    
    parser.add_argument("timestart", type=valid_datetime, help="Start datetime (YYYY-MM-DD)")
    parser.add_argument("timeend", type=valid_datetime, help="End datetime (YYYY-MM-DD)")
    parser.add_argument("-o","--output-dir")
    pars = parser.parse_args()

    output_dir = pars.output_dir if pars.output_dir is not None else defaults["output_dir"]

    os.makedirs(output_dir, exist_ok=True)

    download(pars.timestart, pars.timeend, pars.output_dir if pars.output_dir is not None else defaults["output_dir"])

if __name__ == '__main__':
    main()