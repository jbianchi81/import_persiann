# get persiann
## install (Ubuntu)
```bash
sudo apt install python3-gdal libgdal-dev
pip install .
pip install --no-cache-dir --force-reinstall 'GDAL[numpy]==3.8.4'
## download
```bash
$ persiann-download --help
usage: persiann-download [-h] [-o OUTPUT_DIR] timestart timeend

Downloads persiann data

positional arguments:
  timestart             Start datetime (YYYY-MM-DD)
  timeend               End datetime (YYYY-MM-DD)

options:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
```
## process
```bash
$ persiann-process --help
usage: processpersiann [-h] [-f FILENAME] [-i INPUT_DIR] [-o OUTPUT_DIR] [-b BBOX_FILE]

Processes persiann data

options:
  -h, --help            show this help message and exit
  -f FILENAME, --filename FILENAME
  -i INPUT_DIR, --input-dir INPUT_DIR
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory, or output file path when used together with -f
  -b BBOX_FILE, --bbox-file BBOX_FILE
```
