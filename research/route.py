"""Pick the optimization route with the technique models: hybrid retrieval (RAG) ranks published
techniques and our measurements for each measured bottleneck, and answer verification grounds
each route claim in that evidence. Offline (no API key): BM25 + hashed dense retrieval and
lexical grounding, both deterministic. Usage: python research/route.py"""
import asyncio
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent / "vendor" / "techniques" / "repo"
for p in ("common", "hybrid_retrieval_model", "answer_verification_model"):
    sys.path.insert(0, str(REPO / p))

from pipeline_core import Evidence, make_llm  # noqa: E402
from hybrid_retrieval import HybridRetrievalModel  # noqa: E402
from answer_verification import AnswerVerificationModel  # noqa: E402

BOTTLENECKS = {
    "kernel": "kernel page fault zeroing of fresh memory grows with thread count",
    "fanout": "partition scatter fanout TLB misses many buckets per tuple",
    "parse": "parse loop instructions per event SIMD structural index branch",
    "phases": "materializing intermediate results between phases pipeline fusion",
    "fixed": "fixed cost container startup outside the program",
    "scaling": "throughput stops growing at physical core count hyperthreads",
}

CLAIMS = [
    ("Fanout near 64 per pass avoids a TLB miss for nearly every tuple; 1024 buckets is far above it.", "fanout"),
    ("Kernel time for fresh memory grows with thread count; reusing faulted memory avoids the fault and the zeroing.", "kernel"),
    ("A full docker run on native Linux costs hundreds of milliseconds, outside the program's control.", "fixed"),
    ("Throughput stops growing at the physical core count, so more threads cannot give the speedup.", "scaling"),
    ("Fusing operators into one pipeline avoids materializing intermediate results between phases.", "phases"),
]


async def main() -> None:
    docs = json.loads((HERE / "corpus.json").read_text(encoding="utf-8"))
    llm = make_llm()
    rag = HybridRetrievalModel(llm)
    rag.index(docs)
    by_id = {d["id"]: d for d in docs}
    print(f"indexed {len(docs)} documents ({type(llm).__name__})\n")
    ranked = {}
    for name, query in BOTTLENECKS.items():
        hits = await rag.retrieve(query, k=3)
        ranked[name] = [h.id.split("#")[0] for h in hits]
        print(f"[{name}] {query}")
        for h in hits:
            print(f"   {h.score:.3f}  {h.id}")
    print()
    verifier = AnswerVerificationModel(llm)
    for claim, topic in CLAIMS:
        evidence = [
            Evidence(id=i, content=by_id[i]["text"], source=by_id[i]["metadata"].get("source", by_id[i]["metadata"].get("where", "")))
            for i in dict.fromkeys(ranked[topic])
        ]
        verdict = verifier.verify_sync(claim, task=f"Is this supported? ({topic})", evidence=evidence)
        print(f"{'SUPPORTED' if verdict.passed else 'NOT SUPPORTED':13s} score {verdict.score:.2f}  {claim}")
        print(f"              evidence: {verdict.evidence_ids}")


asyncio.run(main())
