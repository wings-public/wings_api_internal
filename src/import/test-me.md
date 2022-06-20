**Docker image contains the miniconda setup and the cadd installation. cadd installation script retrieves the cadd git repository and executes the install.auto.sh**

_Docker volume and also bind volume is used as illustrated in the docker-compose.yml below. /cadd/dependecies will be destination volume created on the container. In the below compose, the container volume is mounted on the source volume /variantdb_mongo/cadd . The source volume can be updated based on mount volume available on the host machine._
```
docker-compose.yml
version: '2'
services:
  cadd:
    container_name: cadd
    image: variantdb/caddsetup:n3
    volumes:
      - /variantdb_mongo/cadd:/cadd/dependencies
      - caddscripts:/setup
    environment:
      - CADD_DATA=/cadd/dependencies
volumes:
  caddscripts:

```