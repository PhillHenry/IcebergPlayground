# run the shadowDistZip Gradle task then
# cp POLARIS_SRC_HOME/polaris-service/build/distributions/polaris-service-shadow-999-SNAPSHOT.zip .
# Build with something like:
# docker build -t my-polaris .
# docker tag $(docker images | grep my-polaris  | awk '{print $3}') ph1ll1phenry/polaris_for_bdd
# docker push ph1ll1phenry/polaris_for_bdd

FROM redhat/ubi9 AS donor
FROM apache/polaris AS final


# Copy su binary + required libraries
#COPY --from=donor /usr/bin/su /usr/bin/su
COPY --from=donor /lib64/libaudit.so* /lib64/
COPY --from=donor /lib64/libcap-ng.so* /lib64/
COPY --from=donor /lib64/libeconf.so* /lib64/
COPY --from=donor /lib64/libm.so* /lib64/
COPY --from=donor /lib64/libpam.so* /lib64/
COPY --from=donor /lib64/libpam_misc.so* /lib64/
COPY --from=donor /lib64/libselinux.so* /lib64/
COPY --from=donor /lib64/linux-vdso.so* /lib64/
COPY --from=donor /usr/bin/su /usr/bin/su
# PAM modules (all of them, to be safe)
COPY --from=donor /lib64/security/* /lib64/security/
#COPY --from=donor /lib64/security/pam_*.so /lib64/security/

#COPY --from=donor /etc/security/limits.conf /etc/security/limits.conf
#COPY --from=donor /etc/security/limits.d /etc/security/limits.d
#COPY --from=donor /etc/environment /etc/environment
#COPY --from=donor /etc/pam.d/su /etc/pam.d/su
#COPY --from=donor /etc/pam.d/common-* /etc/pam.d/
#COPY --from=donor /etc/pam.d/su /etc/pam.d/su
#COPY --from=donor /etc/pam.d/common* /etc/pam.d/

COPY --from=donor /etc /etc

#COPY --from=donor /lib/security/* /lib/security/
# (you may need a few more .so deps, check with `ldd /usr/bin/su`)

#FROM target
COPY --from=donor /usr/bin/su /usr/bin/su
COPY --from=donor /lib64/* /lib64/

ARG TARGET=/app
COPY polaris_entrypoint.sh /
COPY src/main/resources/application.properties /deployments/config/
#COPY su /etc/pam.d/su
#COPY shadow /etc/shadow

USER root

# Run entrypoint
ENTRYPOINT ["/polaris_entrypoint.sh"]

