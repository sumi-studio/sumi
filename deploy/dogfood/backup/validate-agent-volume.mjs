let input = "";
for await (const chunk of process.stdin) input += chunk;
const [project, logical, expectedName] = process.argv.slice(2);
const parsed = JSON.parse(input);
if (!Array.isArray(parsed) || parsed.length !== 1) {
  throw new Error("Docker volume inspect did not return exactly one object");
}
const volume = parsed[0];
if (
  volume.Name !== expectedName ||
  volume.Driver !== "local" ||
  volume.Scope !== "local" ||
  volume.Labels?.["com.docker.compose.project"] !== project ||
  volume.Labels?.["com.docker.compose.volume"] !== logical
) {
  throw new Error(
    `volume ${expectedName} is not the canonical local Compose volume`,
  );
}
