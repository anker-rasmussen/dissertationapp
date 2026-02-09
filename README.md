# dissertationapp
Application development using the Veilid Framework submodule repo

Registry ownership model:
 - Bootstrap node owns the master registry (a directory of sellers)
 - Each seller owns their own catalog of listings (a separate DHT record)
 - New nodes discover the registry key via AppMessage handshake
 - Listings are discovered via two-hop traversal: master registry → seller catalogs → listings
 - DHT-level ownership prevents users from modifying others' data

 A new devnet = a new bootstrap node = a new registry = a new market instance. This standardizes
 toward a real network where one registry serves all.


