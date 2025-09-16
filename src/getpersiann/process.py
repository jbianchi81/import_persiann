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
import argparse
import re

# === CONFIGURACIÓN ===
defaults = {
    "input_dir": "descargas_persiann",
    "output_dir": "persiann_cdp",
    "bbox_file": "data/bbox.geojson"
}
pixelsize = 0.25
xs = 1440
ys = 400
originx = -180
originy = 50
nodata_value = -9999

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

def procesar_archivo(gz_path, output_dir_= defaults["output_dir"], bbox_file = defaults["bbox_file"], output_full_path : str = None):

    filename = os.path.basename(gz_path)
    input_dir_ = os.path.dirname(gz_path)
    # gz_path = os.path.join(input_dir, filename)
    bin_path = os.path.join(input_dir_, re.sub(".bin.gz",".bin",filename))
    tif_path = os.path.join(input_dir_, re.sub(".bin.gz",".tif",filename))
    recortado_path = output_full_path if output_full_path is not None else os.path.join(output_dir_, re.sub(".bin.gz","_cdp.tif",filename))

    print(f"\nProcesando {filename}...")

    try:
        decompress_gz(gz_path, bin_path)
        bin_to_tif(bin_path, tif_path)
        recortar_tif(tif_path, recortado_path, bbox_file)

        os.remove(bin_path)
        os.remove(tif_path)

        print(f"Guardado: {recortado_path}")

    except Exception as e:
        print(f"Error procesando {filename}: {e}")
        raise e

    finally:
        gc.collect()

# === LOOP PRINCIPAL ===

def main():
    parser = argparse.ArgumentParser(
        prog='processpersiann',
        description='Processes persiann data')
    
    parser.add_argument("-f","--filename")
    parser.add_argument("-i","--input-dir")
    parser.add_argument("-o","--output-dir", help="Output directory, or output file path when used together with -f")
    parser.add_argument("-b","--bbox-file")
    pars = parser.parse_args()

    input_dir = pars.input_dir if pars.input_dir is not None else defaults["input_dir"]
    output_dir = pars.output_dir if pars.output_dir is not None else defaults["output_dir"]
    bbox_file = pars.bbox_file if pars.bbox_file is not None else defaults["bbox_file"]

    if pars.filename is not None:
        if pars.output_dir is not None and not os.path.isdir(pars.output_dir):
            procesar_archivo(pars.filename, bbox_file=bbox_file, output_full_path=output_dir)
        else:
            os.makedirs(output_dir, exist_ok=True)
            procesar_archivo(pars.filename, output_dir=output_dir, bbox_file=bbox_file)
    else:
        os.makedirs(output_dir, exist_ok=True)
        for filename in os.listdir(input_dir):
            if not (filename.endswith(".gz") and filename.startswith("persiann_")):
                continue
            gz_path = os.path.join(input_dir, filename)
            procesar_archivo(gz_path, output_dir, bbox_file)

if __name__ == '__main__':
    main()