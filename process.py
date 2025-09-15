import os
import gzip
import shutil
import numpy as np
from struct import unpack
from osgeo import gdal, osr
import rasterio
from rasterio.mask import mask
import fiona
import gc

# === CONFIGURACIÓN ===
input_dir = "descargas_persiann"
output_dir = "persiann_cdp"
geojson_path = "cca_CDP.geojson"
pixelsize = 0.25
xs, ys = 1440, 400
originx, originy = -180, 50
nodata_value = -9999

os.makedirs(output_dir, exist_ok=True)

# === FUNCIONES ===

def decompress_gz(gz_path, bin_path):
    with gzip.open(gz_path, 'rb') as f_in, open(bin_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def bin_to_tif(bin_path, tif_path):
    NumbytesFile = xs * ys
    NumElementxRecord = -xs
    myarr = []

    with open(bin_path, "rb") as f:
        for PositionByte in range(NumbytesFile, 0, NumElementxRecord):
            Record = []
            for c in range(PositionByte - 720, PositionByte):
                f.seek(c * 4)
                DataElement = unpack('>f', f.read(4))
                Record.append(DataElement[0])
            for c in range(PositionByte - 1440, PositionByte - 720):
                f.seek(c * 4)
                DataElement = unpack('>f', f.read(4))
                Record.append(DataElement[0])
            myarr.append(Record)

    myarr = np.array(myarr, dtype='float32')
    myarr[myarr < 0] = nodata_value
    myarr = myarr[::-1]

    transform = (originx, pixelsize, 0.0, originy, 0.0, -pixelsize)
    driver = gdal.GetDriverByName('GTiff')
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)

    outputDataset = driver.Create(tif_path, xs, ys, 1, gdal.GDT_Float32)
    if outputDataset is None:
        raise RuntimeError(f"No se pudo crear el archivo TIFF: {tif_path}")

    outputDataset.SetGeoTransform(transform)
    outputDataset.SetProjection(target.ExportToWkt())
    outputDataset.GetRasterBand(1).WriteArray(myarr)
    outputDataset.GetRasterBand(1).SetNoDataValue(nodata_value)
    outputDataset = None  # Cierre explícito

def recortar_tif(tif_path, output_path, geojson_path):
    with fiona.open(geojson_path, "r") as shapefile:
        geoms = [feature["geometry"] for feature in shapefile]

    with rasterio.open(tif_path) as src:
        out_image, out_transform = mask(src, geoms, crop=True, nodata=nodata_value)
        out_meta = src.meta.copy()

    out_meta.update({
        "driver": "GTiff",
        "height": out_image.shape[1],
        "width": out_image.shape[2],
        "transform": out_transform,
        "nodata": nodata_value
    })

    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(out_image)

def procesar_archivo(filename):
    if not (filename.endswith(".gz") and filename.startswith("persiann_")):
        return

    datecode = filename[9:17]
    gz_path = os.path.join(input_dir, filename)
    bin_path = os.path.join(input_dir, f"{datecode}.bin")
    tif_path = os.path.join(input_dir, f"{datecode}.tif")
    recortado_path = os.path.join(output_dir, f"persiann_{datecode}_cdp.tif")

    print(f"\nProcesando {filename}...")

    try:
        decompress_gz(gz_path, bin_path)
        bin_to_tif(bin_path, tif_path)
        recortar_tif(tif_path, recortado_path, geojson_path)

        os.remove(bin_path)
        os.remove(tif_path)

        print(f"Guardado: {recortado_path}")

    except Exception as e:
        print(f"Error procesando {filename}: {e}")

    finally:
        gc.collect()

# === LOOP PRINCIPAL ===

for filename in os.listdir(input_dir):
    procesar_archivo(filename)

