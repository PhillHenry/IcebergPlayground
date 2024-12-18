if [ "$RUN_USER_ID" != "" ] &>/dev/null; then
  echo "Creating user with ID $RUN_USER_ID"
  useradd -u $RUN_USER_ID -g 1000 polaris
fi
su -c "/app/bin/polaris-dropwizard-service server polaris-server.yml" polaris