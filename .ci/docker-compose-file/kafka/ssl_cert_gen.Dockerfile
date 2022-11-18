# jdk version must be <= jdk in kafka container for java keystore to work
FROM azul/zulu-openjdk-alpine:8-jre-headless

ARG TEST_DOWNLOAD_BUILD_ARGUMENT=https://nrk.no

RUN apk --update --no-cache add curl=~7 ca-certificates=20220614-r0 openssl=1.1.1s-r1
RUN find /usr/local/share/ca-certificates -not -name "*.crt" -type f -delete
RUN update-ca-certificates 2>/dev/null || true && echo "NOTE: CA warnings suppressed."
RUN curl -s -L -o /dev/null ${TEST_DOWNLOAD_BUILD_ARGUMENT} || { printf "\n###############\nERROR: You are probably behind a corporate proxy. Add your custom ca .crt in the conf/certificates docker build folder\n###############\n"; exit 1; }
