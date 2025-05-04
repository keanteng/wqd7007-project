# Running Pyspark

> Change the file path to your local file path
> Important !!!

```bash
# frist of all get the images (1.6GB)
# takes a few minutes to download
docker pull quay.io/jupyter/pyspark-notebook

# run it
docker run -p 8888:8888 quay.io/jupyter/pyspark-notebook

# if want to use local file
docker run -p 8888:8888 -v c:/Users/Khor\ Kean\ Teng/Downloads/test:/home/jovyan/work quay.io/jupyter/pyspark-notebook
```