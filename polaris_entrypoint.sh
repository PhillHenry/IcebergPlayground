#!/bin/bash

echo About to start bespoke Polaris container

if [[ "$RUN_USER_ID" != "" ]]  && [[ $(grep $RUN_USER_ID /etc/passwd) ]] ; then
  echo Running as $RUN_USER_ID
  su -c "/opt/jboss/container/java/run/run-java.sh"  $(id -un $RUN_USER_ID)
elif [ "$RUN_USER_ID" != "" ] ; then
  echo "Creating user with ID $RUN_USER_ID and GID $RUN_GROUP_ID"
  groupadd -g $RUN_GROUP_ID tempgroup || true
  useradd -u $RUN_USER_ID -g $RUN_GROUP_ID polaris
  su -c "/opt/jboss/container/java/run/run-java.sh" polaris
else
  echo Unknown user: $RUN_USER_ID
fi