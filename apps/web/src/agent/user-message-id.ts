const UserMessageNamespace = uuidBytes("78f62d15-b945-4a4f-9d84-d73c7f932b51");
const CanonicalUUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * Mirrors Rust `Uuid::new_v5(USER_MESSAGE_ID_NAMESPACE,
 * command_id.as_uuid().as_bytes())`.
 */
export function userMessageIdFromCommandId(commandId: string): string {
  if (!CanonicalUUID.test(commandId)) {
    throw new Error("command_id must be a canonical lower-case UUID");
  }
  const input = new Uint8Array(32);
  input.set(UserMessageNamespace);
  input.set(uuidBytes(commandId), 16);
  const digest = sha1(input).slice(0, 16);
  digest[6] = (digest[6] & 0x0f) | 0x50;
  digest[8] = (digest[8] & 0x3f) | 0x80;
  return formatUUID(digest);
}

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function formatUUID(bytes: Uint8Array): string {
  const hex = [...bytes]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(
    12,
    16,
  )}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function rotateLeft(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

/** Small synchronous SHA-1 implementation for UUIDv5's fixed 32-byte input. */
function sha1(input: Uint8Array): Uint8Array {
  const paddedLength = Math.ceil((input.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(input);
  padded[input.length] = 0x80;
  const view = new DataView(padded.buffer);
  const bitLength = input.length * 8;
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x1_0000_0000));
  view.setUint32(paddedLength - 4, bitLength >>> 0);

  let h0 = 0x6745_2301;
  let h1 = 0xefcd_ab89;
  let h2 = 0x98ba_dcfe;
  let h3 = 0x1032_5476;
  let h4 = 0xc3d2_e1f0;
  const words = new Uint32Array(80);

  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) {
      words[index] = view.getUint32(offset + index * 4);
    }
    for (let index = 16; index < 80; index += 1) {
      words[index] = rotateLeft(
        words[index - 3] ^
          words[index - 8] ^
          words[index - 14] ^
          words[index - 16],
        1,
      );
    }

    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;
    for (let index = 0; index < 80; index += 1) {
      let f: number;
      let k: number;
      if (index < 20) {
        f = (b & c) | (~b & d);
        k = 0x5a82_7999;
      } else if (index < 40) {
        f = b ^ c ^ d;
        k = 0x6ed9_eba1;
      } else if (index < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8f1b_bcdc;
      } else {
        f = b ^ c ^ d;
        k = 0xca62_c1d6;
      }
      const next = (rotateLeft(a, 5) + (f >>> 0) + e + k + words[index]) >>> 0;
      e = d;
      d = c;
      c = rotateLeft(b, 30);
      b = a;
      a = next;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
  }

  const output = new Uint8Array(20);
  const outputView = new DataView(output.buffer);
  [h0, h1, h2, h3, h4].forEach((value, index) => {
    outputView.setUint32(index * 4, value);
  });
  return output;
}
