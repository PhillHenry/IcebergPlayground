rm -rf /tmp/polaris/ || echo "/tmp/polaris/ does not yet exist"
mkdir /tmp/polaris/
chmod -R a+rw /tmp/polaris/

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN:-principal:root;realm:default-realm}"

# create a catalog backed by the local filesystem
curl -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
     -H 'Accept: application/json' \
     -H 'Content-Type: application/json' \
     http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
     -d '{
           "catalog": {
             "name": "manual_spark",
             "type": "INTERNAL",
             "readOnly": false,
             "properties": {
               "default-base-location": "file:///tmp/polaris/"
             },
             "storageConfigInfo": {
               "storageType": "FILE",
               "allowedLocations": [
                 "file:///tmp/polaris/"
               ]
             }
           }
         }'

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}'

# Assign the catalog_admin to the service_admin.
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/manual_spark \
  -d '{"name": "catalog_admin"}'

curl -X GET -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark


