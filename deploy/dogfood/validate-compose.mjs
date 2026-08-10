let input = "";
for await (const chunk of process.stdin) input += chunk;
const config = JSON.parse(input);
const services = config.services ?? {};
if (services.api?.deploy?.replicas !== 1) {
  throw new Error("api.deploy.replicas must be exactly 1");
}
if (services.api.deploy?.update_config?.order !== "stop-first") {
  throw new Error("api deployment order must be stop-first");
}
for (const [name, service] of Object.entries(services)) {
  if ((service.ports ?? []).length !== 0)
    throw new Error(`${name} publishes a host port`);
  if (
    typeof service.image === "string" &&
    !service.image.includes("@sha256:")
  ) {
    throw new Error(`${name} does not use an image digest`);
  }
}
if (!services.api.healthcheck?.test?.join(" ").includes("/ready")) {
  throw new Error("api deploy healthcheck does not use dependency readiness");
}
if (services.cloudflared?.depends_on?.api?.condition !== "service_healthy") {
  throw new Error("cloudflared can start before API readiness");
}
if (
  !(services.cloudflared?.command ?? [])
    .join(" ")
    .includes("--token-file /run/secrets/cloudflare_tunnel_token")
) {
  throw new Error("cloudflared must use the named Tunnel token file");
}
if (
  !services.postgres?.volumes?.some(
    (volume) =>
      volume.source === "postgres-data" &&
      volume.target === "/var/lib/postgresql/data",
  )
) {
  throw new Error("Postgres data is not on the declared persistent volume");
}
const databaseClient = services["database-client"];
if (
  (services.postgres?.ports ?? []).length !== 0 ||
  databaseClient?.image !== services.postgres?.image ||
  JSON.stringify(databaseClient?.profiles) !==
    JSON.stringify(["maintenance"]) ||
  databaseClient?.read_only !== true ||
  !(databaseClient?.cap_drop ?? []).includes("ALL") ||
  databaseClient?.networks?.data === undefined ||
  (databaseClient?.networks
    ? Object.keys(databaseClient.networks).some((network) => network !== "data")
    : true) ||
  (databaseClient?.volumes ?? []).length !== 0 ||
  config.networks?.data?.internal !== true
) {
  throw new Error(
    "database maintenance must use a volume-free client on the internal data network",
  );
}
if (
  !services.api?.volumes?.some((volume) => volume.target === "/var/lib/sumi")
) {
  throw new Error("API durable state is not mounted at /var/lib/sumi");
}
if (
  services["runtime-provisioner"]?.environment?.DOCKER_CONFIG !==
    "/run/sumi/docker-config" ||
  !services["runtime-provisioner"]?.volumes?.some(
    (volume) =>
      volume.target === "/run/sumi/docker-config/config.json" &&
      volume.read_only === true,
  )
) {
  throw new Error("runtime provisioner has no protected registry credential");
}
process.stdout.write("dogfood compose contract ok\n");
