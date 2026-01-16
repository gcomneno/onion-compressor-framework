
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from .codec_base import Codec

import heapq
import itertools

# -------------------
# Strutture di base Huffman
# -------------------
@dataclass
class HuffmanNode:
    freq: int
    symbol: Optional[int] = None  # 0-255 per foglie, None per interni
    left: Optional["HuffmanNode"] = None
    right: Optional["HuffmanNode"] = None

def build_freq_table(data: bytes) -> List[int]:
    freq = [0] * 256
    for b in data:
        freq[b] += 1
    return freq

def build_huffman_tree(freq: List[int]) -> Optional[HuffmanNode]:
    heap: List[tuple[int, int, HuffmanNode]] = []
    counter = itertools.count()

    for sym, f in enumerate(freq):
        if f > 0:
            node = HuffmanNode(freq=f, symbol=sym)
            heapq.heappush(heap, (f, next(counter), node))

    if not heap:
        return None

    # Caso speciale: un solo simbolo => aggiungo dummy
    if len(heap) == 1:
        f, _, only = heap[0]
        dummy_symbol = (only.symbol + 1) % len(freq)
        dummy = HuffmanNode(freq=0, symbol=dummy_symbol)
        heapq.heappush(heap, (0, next(counter), dummy))

    while len(heap) > 1:
        f1, _, n1 = heapq.heappop(heap)
        f2, _, n2 = heapq.heappop(heap)
        parent = HuffmanNode(freq=f1 + f2, symbol=None, left=n1, right=n2)
        heapq.heappush(heap, (parent.freq, next(counter), parent))

    return heap[0][2]

def build_code_table(root: HuffmanNode) -> Dict[int, List[int]]:
    codes: Dict[int, List[int]] = {}

    def dfs(node: HuffmanNode, path: List[int]):
        # Foglia
        if node.symbol is not None and node.left is None and node.right is None:
            codes[node.symbol] = path.copy() if path else [0]
            return
        if node.left is not None:
            dfs(node.left, path + [0])
        if node.right is not None:
            dfs(node.right, path + [1])

    dfs(root, [])
    return codes

def encode_data(data: bytes, codes: Dict[int, List[int]]) -> Tuple[bytes, int]:
    """
    data -> (bitstream, lastbits)
    lastbits = numero di bit validi nell'ultimo byte (1..8) oppure 0 se data vuoto.
    """
    if not data:
        return b"", 0

    out_bytes = bytearray()
    current_byte = 0
    bit_count = 0

    for b in data:
        for bit in codes[b]:
            current_byte = (current_byte << 1) | bit
            bit_count += 1
            if bit_count == 8:
                out_bytes.append(current_byte)
                current_byte = 0
                bit_count = 0

    if bit_count > 0:
        current_byte = current_byte << (8 - bit_count)
        out_bytes.append(current_byte)
        lastbits = bit_count
    else:
        lastbits = 8  # tutti i byte pieni

    return bytes(out_bytes), lastbits

def decode_bitstream(root: HuffmanNode, bitstream: bytes, N: int, lastbits: int) -> bytes:
    """
    Decodifica N simboli a partire dall'albero, dal bitstream e da lastbits.
    """
    if N == 0:
        return b""
    if root is None:
        return b""

    out = bytearray()
    node = root
    total_symbols = 0
    total_bytes = len(bitstream)

    for i, byte in enumerate(bitstream):
        bits_in_this_byte = 8
        if i == total_bytes - 1 and lastbits != 0:
            bits_in_this_byte = lastbits

        for bit_index in range(bits_in_this_byte):
            bit = (byte >> (7 - bit_index)) & 1
            node = node.left if bit == 0 else node.right
            if node.symbol is not None and node.left is None and node.right is None:
                out.append(node.symbol)
                total_symbols += 1
                node = root
                if total_symbols == N:
                    return bytes(out)

    return bytes(out)

def huffman_compress_core(data: bytes) -> Tuple[List[int], int, bytes]:
    """
    Core riusabile (Step1/Step2/Step3/Step4): data -> (freq, lastbits, bitstream)
    """
    freq = build_freq_table(data)
    root = build_huffman_tree(freq)
    if root is None:
        return freq, 0, b""
    codes = build_code_table(root)
    bitstream, lastbits = encode_data(data, codes)
    return freq, lastbits, bitstream

def huffman_decompress_core(freq: List[int], bitstream: bytes, N: int, lastbits: int) -> bytes:
    """
    Core riusabile: (freq, bitstream, N, lastbits) -> data
    """
    root = build_huffman_tree(freq)
    if root is None or N == 0:
        return b""
    return decode_bitstream(root, bitstream, N, lastbits)

def huffman_compress_ids(id_stream: List[int], vocab_size: int) -> Tuple[List[int], int, bytes]:
    """
    Variante di huffman_compress_core, ma per una sequenza di ID interi 0..vocab_size-1.
    Restituisce:
      - freq: frequenze per ciascun ID (len = vocab_size)
      - lastbits: numero di bit validi nell'ultimo byte del bitstream
      - bitstream: bytes con i bit Huffman MSB-first.
    """
    if vocab_size <= 0:
        return [], 0, b""

    # Frequenze sugli ID
    freq = [0] * vocab_size
    for sid in id_stream:
        if sid < 0 or sid >= vocab_size:
            raise ValueError(f"ID fuori range per huffman_compress_ids: {sid}")
        freq[sid] += 1

    # Se non c'è nessun simbolo, niente bitstream
    if all(f == 0 for f in freq):
        return freq, 0, b""

    root = build_huffman_tree(freq)
    if root is None:
        return freq, 0, b""

    codes = build_code_table(root)

    out_bytes = bytearray()
    current_byte = 0
    bit_count = 0

    for sid in id_stream:
        code_bits = codes[sid]
        for bit in code_bits:
            current_byte = (current_byte << 1) | bit
            bit_count += 1
            if bit_count == 8:
                out_bytes.append(current_byte)
                current_byte = 0
                bit_count = 0

    lastbits = bit_count if bit_count > 0 else 0
    if bit_count > 0:
        current_byte <<= (8 - bit_count)
        out_bytes.append(current_byte)

    return freq, lastbits, bytes(out_bytes)

def huffman_decompress_ids(freq: List[int], N_symbols: int, lastbits: int, bitstream: bytes) -> List[int]:
    """
    Decodifica una sequenza di ID (0..K-1) da un bitstream Huffman,
    dato l'array di frequenze freq (len = K).
    """
    if N_symbols == 0:
        return []

    if not freq:
        raise ValueError("freq vuoto in huffman_decompress_ids")

    root = build_huffman_tree(freq)
    if root is None:
        return []

    ids: List[int] = []
    node = root
    total_symbols = 0
    total_bytes = len(bitstream)

    for i, byte in enumerate(bitstream):
        bits_in_this_byte = 8
        if i == total_bytes - 1 and lastbits != 0:
            bits_in_this_byte = lastbits

        for bit_index in range(bits_in_this_byte):
            bit = (byte >> (7 - bit_index)) & 1
            node = node.left if bit == 0 else node.right
            if node.symbol is not None and node.left is None and node.right is None:
                ids.append(node.symbol)
                total_symbols += 1
                node = root
                if total_symbols == N_symbols:
                    break
        if total_symbols == N_symbols:
            break

    if total_symbols != N_symbols:
        raise ValueError(
            f"huffman_decompress_ids: attesi {N_symbols} simboli, decodificati {total_symbols}"
        )

    return ids

class CodecHuffman(Codec):
    codec_id = "huffman"

    def compress_bytes(self, data: bytes):
        return huffman_compress_core(data)

    def decompress_bytes(self, freq, bitstream: bytes, n: int, lastbits: int):
        return huffman_decompress_core(freq, bitstream, n, lastbits)

    def compress_ids(self, id_stream, vocab_size: int):
        return huffman_compress_ids(list(id_stream), vocab_size)

    def decompress_ids(self, freq, n_symbols: int, lastbits: int, bitstream: bytes):
        return huffman_decompress_ids(freq, n_symbols, lastbits, bitstream)

# ============================================================
# Huffman Bundle v1 (multi-stream)
# ============================================================


from typing import List, Tuple, Optional

from gcc_ocf.core.bundle import SymbolStream, EncodedStream

BUNDLE_MAGIC = b"HBN1"  # Huffman Bundle v1


def _norm_triplet(ret) -> tuple[list[int], int, bytes]:
    """
    Normalizza output di compress_* che potrebbe essere in ordine diverso.
    Ritorna: (freq_list, lastbits_int, bitstream_bytes)
    """
    if not isinstance(ret, tuple) or len(ret) != 3:
        raise TypeError("compress_* deve ritornare una tupla (freq, lastbits, bitstream)")

    freq = None
    lastbits = None
    bitstream = None

    for x in ret:
        if isinstance(x, list):
            freq = x
        elif isinstance(x, int):
            lastbits = x
        elif isinstance(x, (bytes, bytearray)):
            bitstream = bytes(x)

    if freq is None or lastbits is None or bitstream is None:
        raise TypeError("impossibile normalizzare tripla Huffman (tipi inattesi)")

    return freq, int(lastbits), bitstream


def _freq_to_used(freq: List[int]) -> List[Tuple[int, int]]:
    return [(i, f) for i, f in enumerate(freq) if f > 0]


def _used_to_freq(used: List[Tuple[int, int]], alphabet_size: int) -> List[int]:
    freq = [0] * alphabet_size
    for sym, f in used:
        if sym < 0 or sym >= alphabet_size:
            raise ValueError("freq_used contiene sym fuori range")
        freq[sym] = f
    return freq


def huffman_encode_stream(stream: SymbolStream, codec: Optional["CodecHuffman"] = None) -> EncodedStream:
    if codec is None:
        codec = CodecHuffman()

    if stream.kind == "bytes":
        data_b = stream.data
        if not isinstance(data_b, (bytes, bytearray)):
            raise TypeError("SymbolStream bytes ma data non bytes")
        freq, lastbits, bitstream = _norm_triplet(codec.compress_bytes(bytes(data_b)))
        return EncodedStream(
            name=stream.name,
            kind="bytes",
            alphabet_size=256,
            n=stream.n,
            encoding="huffman",
            freq_used=_freq_to_used(freq),
            lastbits=lastbits,
            bitstream=bitstream,
        )

    if stream.kind == "ids":
        ids = stream.data
        if not isinstance(ids, list):
            raise TypeError("SymbolStream ids ma data non list[int]")
        vocab_size = stream.alphabet_size
        freq, lastbits, bitstream = _norm_triplet(codec.compress_ids(ids, vocab_size))
        return EncodedStream(
            name=stream.name,
            kind="ids",
            alphabet_size=vocab_size,
            n=stream.n,
            encoding="huffman",
            freq_used=_freq_to_used(freq),
            lastbits=lastbits,
            bitstream=bitstream,
        )

    raise NotImplementedError(f"kind non supportato: {stream.kind}")


def huffman_decode_stream(enc: EncodedStream, codec: Optional["CodecHuffman"] = None) -> SymbolStream:
    if codec is None:
        codec = CodecHuffman()

    if enc.encoding == "raw":
        raw = enc.raw or b""
        if enc.kind != "bytes":
            raise ValueError("raw supportato solo per bytes")
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(raw), data=raw)

    if enc.freq_used is None or enc.lastbits is None or enc.bitstream is None:
        raise ValueError("EncodedStream huffman incompleto")

    freq = _used_to_freq(enc.freq_used, enc.alphabet_size)

    if enc.kind == "bytes":
        data = codec.decompress_bytes(freq, enc.bitstream, enc.n, enc.lastbits)
        return SymbolStream(name=enc.name, kind="bytes", alphabet_size=256, n=len(data), data=data)

    if enc.kind == "ids":
        ids = codec.decompress_ids(freq, enc.n, enc.lastbits, enc.bitstream)
        return SymbolStream(name=enc.name, kind="ids", alphabet_size=enc.alphabet_size, n=len(ids), data=ids)

    raise NotImplementedError(f"kind non supportato: {enc.kind}")


def _pack_encoded_stream(enc: EncodedStream) -> bytes:
    name_b = enc.name.encode("utf-8")
    if len(name_b) > 0xFF:
        raise ValueError("stream name troppo lungo (max 255)")

    out = bytearray()
    out.append(0 if enc.encoding == "raw" else 1)          # encoding flag
    out.append(0 if enc.kind == "bytes" else 1)            # kind flag
    out.append(len(name_b))
    out += name_b
    out += enc.alphabet_size.to_bytes(4, "big")
    out += enc.n.to_bytes(4, "big")

    if enc.encoding == "raw":
        raw = enc.raw or b""
        out += len(raw).to_bytes(4, "big")
        out += raw
        return bytes(out)

    used = enc.freq_used or []
    out += len(used).to_bytes(4, "big")
    for sym, f in used:
        out += sym.to_bytes(4, "big")
        out += f.to_bytes(4, "big")

    out.append(int(enc.lastbits or 0) & 0xFF)
    bs = enc.bitstream or b""
    out += len(bs).to_bytes(4, "big")
    out += bs
    return bytes(out)


def _unpack_encoded_stream(blob: bytes, idx: int) -> tuple[EncodedStream, int]:
    if idx + 1 + 1 + 1 + 4 + 4 > len(blob):
        raise ValueError("bundle troncato (header stream)")

    enc_flag = blob[idx]; idx += 1
    kind_flag = blob[idx]; idx += 1
    name_len = blob[idx]; idx += 1

    if idx + name_len > len(blob):
        raise ValueError("bundle troncato (name)")
    name = blob[idx:idx+name_len].decode("utf-8")
    idx += name_len

    alphabet_size = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    n = int.from_bytes(blob[idx:idx+4], "big"); idx += 4

    encoding = "raw" if enc_flag == 0 else "huffman"
    kind = "bytes" if kind_flag == 0 else "ids"

    if encoding == "raw":
        raw_len = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        if idx + raw_len > len(blob):
            raise ValueError("bundle troncato (raw)")
        raw = blob[idx:idx+raw_len]; idx += raw_len
        return EncodedStream(name=name, kind=kind, alphabet_size=alphabet_size, n=n, encoding="raw", raw=raw), idx

    num_used = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    used: List[Tuple[int, int]] = []
    for _ in range(num_used):
        if idx + 8 > len(blob):
            raise ValueError("bundle troncato (freq entries)")
        sym = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        f = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
        used.append((sym, f))

    if idx >= len(blob):
        raise ValueError("bundle troncato (lastbits)")
    lastbits = blob[idx]; idx += 1

    bs_len = int.from_bytes(blob[idx:idx+4], "big"); idx += 4
    if idx + bs_len > len(blob):
        raise ValueError("bundle troncato (bitstream)")
    bitstream = blob[idx:idx+bs_len]; idx += bs_len

    return EncodedStream(
        name=name,
        kind=kind,
        alphabet_size=alphabet_size,
        n=n,
        encoding="huffman",
        freq_used=used,
        lastbits=lastbits,
        bitstream=bitstream,
    ), idx


def pack_huffman_bundle(encoded_streams: List[EncodedStream]) -> bytes:
    if len(encoded_streams) > 0xFF:
        raise ValueError("troppi stream (max 255)")
    out = bytearray()
    out += BUNDLE_MAGIC
    out.append(len(encoded_streams))
    for s in encoded_streams:
        sb = _pack_encoded_stream(s)
        out += len(sb).to_bytes(4, "big")
        out += sb
    return bytes(out)


def unpack_huffman_bundle(payload: bytes) -> List[EncodedStream]:
    if len(payload) < 5 or payload[:4] != BUNDLE_MAGIC:
        raise ValueError("payload non è un Huffman bundle")
    idx = 4
    n_streams = payload[idx]; idx += 1
    streams: List[EncodedStream] = []
    for _ in range(n_streams):
        if idx + 4 > len(payload):
            raise ValueError("bundle troncato (len)")
        L = int.from_bytes(payload[idx:idx+4], "big"); idx += 4
        if idx + L > len(payload):
            raise ValueError("bundle troncato (stream blob)")
        s_blob = payload[idx:idx+L]; idx += L
        s, _ = _unpack_encoded_stream(s_blob, 0)
        streams.append(s)
    return streams
