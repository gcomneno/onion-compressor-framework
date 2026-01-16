from __future__ import annotations

from gcc_ocf.layers.vocab_blob import pack_vocab_list, unpack_vocab_list

# -------------------------------------------------------------------
# Legacy codec payloads (Huffman)
#
# These are the "legacy payload" encodings historically used by v1-v4
# and by the first iterations of the v5 container.
#
# NOTE: KIND_IDS and KIND_IDS_META_VOCAB intentionally share the same
# value (1). The distinction is made by how the vocabulary is provided:
#   - META_VOCAB: vocab_list is stored in container meta
#   - INLINE_VOCAB: vocab_list is stored inline in the payload
# -------------------------------------------------------------------

# Codec payload: Huffman (bytes / ids)
KIND_BYTES = 0
KIND_IDS = 1

KIND_IDS_META_VOCAB = 1
KIND_IDS_INLINE_VOCAB = 2


# -------------------
# bytes payload (KIND_BYTES)
# [KIND(1)|NUM_USED(u32)|repeat(sym u8, freq u32)|LASTBITS(u8)|BITSTREAM...]
# -------------------
def pack_huffman_payload_bytes(freq: list[int], lastbits: int, bitstream: bytes) -> bytes:
    used = [(sym, f) for sym, f in enumerate(freq) if f > 0]

    out = bytearray()
    out.append(KIND_BYTES)
    out += len(used).to_bytes(4, "big")
    for sym, f in used:
        out.append(sym)  # u8
        out += f.to_bytes(4, "big")  # u32
    out.append(lastbits & 0xFF)  # u8
    out += bitstream
    return bytes(out)


def unpack_huffman_payload_bytes(payload: bytes) -> tuple[list[int], int, bytes]:
    if len(payload) < 1 + 4 + 1:
        raise ValueError("payload Huffman(bytes) troppo corto")

    idx = 0
    kind = payload[idx]
    idx += 1
    if kind != KIND_BYTES:
        raise ValueError(f"payload kind inatteso: {kind} (atteso bytes=0)")

    num = int.from_bytes(payload[idx : idx + 4], "big")
    idx += 4

    freq = [0] * 256
    for _ in range(num):
        if idx + 1 + 4 > len(payload):
            raise ValueError("payload troncato (freq entries)")
        sym = payload[idx]
        idx += 1
        f = int.from_bytes(payload[idx : idx + 4], "big")
        idx += 4
        freq[sym] = f

    if idx >= len(payload):
        raise ValueError("payload troncato (lastbits)")
    lastbits = payload[idx]
    idx += 1

    bitstream = payload[idx:]
    return freq, lastbits, bitstream


# -------------------
# ids payload (KIND_IDS / KIND_IDS_META_VOCAB)
# [KIND(1)|VOCAB_SIZE(u32)|NUM_USED(u32)|repeat(sym u32, freq u32)|LASTBITS(u8)|BITSTREAM...]
# -------------------
def pack_huffman_payload_ids(
    vocab_size: int, freq: list[int], lastbits: int, bitstream: bytes
) -> bytes:
    if len(freq) != vocab_size:
        raise ValueError("freq len != vocab_size")

    used = [(sym, f) for sym, f in enumerate(freq) if f > 0]

    out = bytearray()
    out.append(KIND_IDS)
    out += vocab_size.to_bytes(4, "big")
    out += len(used).to_bytes(4, "big")
    for sym, f in used:
        out += sym.to_bytes(4, "big")  # u32
        out += f.to_bytes(4, "big")  # u32
    out.append(lastbits & 0xFF)
    out += bitstream
    return bytes(out)


def unpack_huffman_payload_ids(payload: bytes) -> tuple[int, list[int], int, bytes]:
    if len(payload) < 1 + 4 + 4 + 1:
        raise ValueError("payload Huffman(ids) troppo corto")

    idx = 0
    kind = payload[idx]
    idx += 1
    if kind != KIND_IDS:
        raise ValueError(f"payload kind inatteso: {kind} (atteso ids=1)")

    vocab_size = int.from_bytes(payload[idx : idx + 4], "big")
    idx += 4
    num = int.from_bytes(payload[idx : idx + 4], "big")
    idx += 4

    freq = [0] * vocab_size
    for _ in range(num):
        if idx + 4 + 4 > len(payload):
            raise ValueError("payload troncato (freq entries ids)")
        sym = int.from_bytes(payload[idx : idx + 4], "big")
        idx += 4
        f = int.from_bytes(payload[idx : idx + 4], "big")
        idx += 4
        if sym >= vocab_size:
            raise ValueError("payload corrotto: sym >= vocab_size")
        freq[sym] = f

    if idx >= len(payload):
        raise ValueError("payload troncato (lastbits ids)")
    lastbits = payload[idx]
    idx += 1

    bitstream = payload[idx:]
    return vocab_size, freq, lastbits, bitstream


# -------------------
# ids payload with inline vocabulary (KIND_IDS_INLINE_VOCAB)
# [KIND(1)|VOCAB_BLOB_LEN(u32)|VOCAB_BLOB|NUM_USED(u32)|repeat(sym u32, freq u32)|LASTBITS(u8)|BITSTREAM...]
# -------------------
def pack_huffman_payload_ids_inline_vocab(
    vocab_list: list[bytes],
    freq: list[int],
    lastbits: int,
    bitstream: bytes,
) -> bytes:
    vocab_blob = pack_vocab_list(vocab_list)
    vocab_size = len(vocab_list)
    if len(freq) != vocab_size:
        raise ValueError("freq len != vocab_size")

    used = [(sym, f) for sym, f in enumerate(freq) if f > 0]

    out = bytearray()
    out.append(KIND_IDS_INLINE_VOCAB)
    out += len(vocab_blob).to_bytes(4, "big")
    out += vocab_blob
    out += len(used).to_bytes(4, "big")
    for sym, f in used:
        out += sym.to_bytes(4, "big")  # u32
        out += f.to_bytes(4, "big")  # u32
    out.append(lastbits & 0xFF)
    out += bitstream
    return bytes(out)


def unpack_huffman_payload_ids_inline_vocab(
    payload: bytes,
) -> tuple[list[bytes], list[int], int, bytes]:
    if len(payload) < 1 + 4 + 4 + 1:
        raise ValueError("payload Huffman(ids+vocab) troppo corto")

    idx = 0
    kind = payload[idx]
    idx += 1
    if kind != KIND_IDS_INLINE_VOCAB:
        raise ValueError(f"payload kind inatteso: {kind} (atteso ids+vocab=2)")

    vocab_len = int.from_bytes(payload[idx : idx + 4], "big")
    idx += 4
    if idx + vocab_len > len(payload):
        raise ValueError("payload troncato (vocab)")
    vocab_blob = payload[idx : idx + vocab_len]
    idx += vocab_len
    vocab_list = unpack_vocab_list(vocab_blob)
    vocab_size = len(vocab_list)

    if idx + 4 > len(payload):
        raise ValueError("payload troncato (num_used)")
    num = int.from_bytes(payload[idx : idx + 4], "big")
    idx += 4

    freq = [0] * vocab_size
    for _ in range(num):
        if idx + 8 > len(payload):
            raise ValueError("payload troncato (freq entries)")
        sym = int.from_bytes(payload[idx : idx + 4], "big")
        idx += 4
        f = int.from_bytes(payload[idx : idx + 4], "big")
        idx += 4
        if sym >= vocab_size:
            raise ValueError("payload corrotto: sym >= vocab_size")
        freq[sym] = f

    if idx >= len(payload):
        raise ValueError("payload troncato (lastbits)")
    lastbits = payload[idx]
    idx += 1

    bitstream = payload[idx:]
    return vocab_list, freq, lastbits, bitstream
