# Keep the Firebase CLI version explicit: Auth export/import files are part of
# the development service's durable state, so silent CLI upgrades are unsafe.
FROM node:20.19.5-bookworm-slim

RUN npm install --global firebase-tools@13.35.1 \
    && npm cache clean --force

WORKDIR /workspace
COPY firebase.json /workspace/firebase.json
COPY docker-entrypoint.sh /usr/local/bin/sumi-firebase-auth-emulator
RUN chmod 0555 /usr/local/bin/sumi-firebase-auth-emulator

ENTRYPOINT ["/usr/local/bin/sumi-firebase-auth-emulator"]
