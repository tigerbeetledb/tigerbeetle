# OLTP

Note: this section is the focus of the entire composition

* Start with history and the definition: how OLTP is _actually_ about business transaction
* Define OLGP as separate from OLTP. Hammer that OLTP != SQL
* Can SQL do OLTP? Yes, but not very good:
    - row locks over the network --- fundamental limitation
    - careless handling of durability --- ok for some domains, but not for accounting
* Derive TigerBeetle from the axiom: provide strong durability at the speed-of-light (as fast as
  possible) performance
* Hook the next two sections, safety and performance
