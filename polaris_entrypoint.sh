if [  $(grep $RUN_USER_ID /etc/passwd) ] ; then
  su -c "java -jar /app/quarkus-run.jar"  $(id -un $RUN_USER_ID)
elif [ "$RUN_USER_ID" != "" ] ; then
  echo "Creating user with ID $RUN_USER_ID and GID $RUN_GROUP_ID"
  groupadd -g $RUN_GROUP_ID tempgroup || true
  useradd -u $RUN_USER_ID -g $RUN_GROUP_ID polaris
  su -c "java -jar /app/quarkus-run.jar" polaris
fi