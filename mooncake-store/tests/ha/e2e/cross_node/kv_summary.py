import collections, sqlite3, sys
db = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
groups = collections.Counter()
for (key,) in db.execute("SELECT key FROM kv"):
    k = key.decode(errors="replace") if isinstance(key, bytes) else key
    parts = k.strip("/").split("/")
    groups["/".join(parts[:3])] += 1
for g, n in sorted(groups.items()):
    print(f"{g}={n}")
print("repl_meta=" + str(db.execute("SELECT seq, epoch FROM repl_meta").fetchone()))
