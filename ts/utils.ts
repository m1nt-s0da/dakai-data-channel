const CHUNK_ID_MASK = 0x00ff_ffff_ffff_ffffn;

const HEX: string[] = [];
for (let index = 0; index < 256; index += 1) {
  HEX.push(index.toString(16).padStart(2, "0"));
}

export async function sha256Hex(data: Uint8Array): Promise<string> {
  const normalized = new Uint8Array(data.byteLength);
  normalized.set(data);
  const digest = await crypto.subtle.digest("SHA-256", normalized);
  return bytesToHex(new Uint8Array(digest));
}

export function bytesToHex(data: Uint8Array): string {
  let output = "";
  for (const value of data) {
    output += HEX[value];
  }
  return output;
}

export function uuid7(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let timestamp = BigInt(Date.now());

  for (let index = 5; index >= 0; index -= 1) {
    bytes[index] = Number(timestamp & 0xffn);
    timestamp >>= 8n;
  }

  bytes[6] = (bytes[6] & 0x0f) | 0x70;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  return [
    bytesToHex(bytes.subarray(0, 4)),
    bytesToHex(bytes.subarray(4, 6)),
    bytesToHex(bytes.subarray(6, 8)),
    bytesToHex(bytes.subarray(8, 10)),
    bytesToHex(bytes.subarray(10, 16)),
  ].join("-");
}

export function createChunkId(): bigint {
  const bytes = crypto.getRandomValues(new Uint8Array(8));
  let chunkId = 0n;
  for (const value of bytes) {
    chunkId = (chunkId << 8n) | BigInt(value);
  }
  return chunkId & CHUNK_ID_MASK;
}

export function frameChunkId(chunkId: bigint): Uint8Array {
  if (chunkId < 0n || chunkId > CHUNK_ID_MASK) {
    throw new RangeError("chunkId must fit within 56 bits.");
  }

  const frameId = (chunkId << 8n) | 0x66n;
  const bytes = new Uint8Array(8);
  let value = frameId;
  for (let index = 0; index < 8; index += 1) {
    bytes[index] = Number(value & 0xffn);
    value >>= 8n;
  }
  return bytes;
}

export function parseChunkId(frame: Uint8Array): bigint {
  let value = 0n;
  for (let index = 7; index >= 0; index -= 1) {
    value = (value << 8n) | BigInt(frame[index]);
  }
  return value >> 8n;
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const totalLength = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const output = new Uint8Array(totalLength);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

export function normalizeBinaryData(data: ArrayBuffer | Uint8Array): Uint8Array {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data.slice(0));
  }

  const view = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  const normalized = new Uint8Array(view.byteLength);
  normalized.set(view);
  return normalized;
}

export async function normalizeMessageData(data: string | Blob | ArrayBuffer | ArrayBufferView): Promise<string | Uint8Array> {
  if (typeof data === "string") {
    return data;
  }
  if (data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }
  if (data instanceof ArrayBuffer) {
    return normalizeBinaryData(data);
  }
  return normalizeBinaryData(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
}

export function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}