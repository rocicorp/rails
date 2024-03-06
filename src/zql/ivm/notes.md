- Allow the user to declare what Rails collections to pull into memory
- Use experimental watch to keep those collections up to date.
- Use those collections as sources to IVM

- On update from watch, start materialite tx
- apply update

---

Query preparation allows us to:

1. Manage stmt lifetime
2. De-duplicate paths
3. Reference count items in the graph
4. Cleanup an entire query's graph

---

Still need:

- IvmDb abstraction to manage registered sources and maintain them in-memory
