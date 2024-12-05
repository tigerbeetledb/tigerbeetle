# Docs Style Guide

## Macro

Docs are separated into user docs and developer docs. Right now, you are reading a developer
document describing how to write user docs! All docs are in the `./docs` folder in the repository.
Developer docs are in the `./docs/internals` subfolder.

Both user and developer docs are considered to be integral parts of TigerBeetle database.
TIGER_STYLE applies to documentation as well!

Documentation exists independent of presentation. In particular, the following three different
presentations are considered first-class:

- Docs as rendered on <https://docs.tigerbeetle.com> by our own static site generator.
- Docs as viewable on <https://github.com/tigerbeetle/tigerbeetle> by using GitHub built-in Markdown
  renderer.
- Docs as raw markdown viewable in a local text editor.

User docs roughly follow the Django-style organization. Specifically, they come in three flavors:

- Tutorial is an end-to-end quick tour which focuses on getting the user to do specific things to
  achieve a specific goal, without necessary explaining what exactly is going on. Tutorial are
  aimed at beginners and non-users.
- Guides gives an in-depth explanation of a particular area. Guides are used by new users, who know
  the basics, and what to get a specific thing done. Unlike tutorials, guides should _always_
  explain the why.
- Reference is the API-docs level documentation. It specifies behaviors with maximum level of
  precision. A reference is not good for reading start to finish, it is a random-access document.

Finally, we add our own TigerBeetle twist to this Django structure:

- Concepts and principles explain why TigerBeetle is the way it is. From principles, the rest
  follows. Tutorials, Guides, and the Reference are documents about TigerBeetle as implemented,
  while the concepts speak to the Platonic ideal of the beetle.

Unlike user docs, internal docs do not follow any specific structure. Internal docs are "ingest
optimized" --- it's more important to have something documented, then for the documentation to be in
a consistent style. If you are unsure where something needs to be documented, just add a new file
into the `./internals` folder: it will get properly reorganized&compacted with time!

## Micro

- Because docs are viewable on GitHub, GitHub Flavored Markdown is used for all the content.
- Hard wrap long lines to keep them readable as source.
- Hard warp at 100 because that's what TIGER_STYLE says.
- Use Oxford comma (A, B, and C), for consistency.
- Use Standard American English for consistency.
- Cool URIs don't change! Think hard about file and section names, as they form parts of URLs
- As Dijkstra said, we should regard every line of documentation not as a line produced, but as a
  line spend. The value of the docs is not in the number of words, but in the number of concepts
  covered. Number of words is the cost. A good process for documentation writing is:

  - list all the facts that you want to communicate,
  - find the shortest set of words that explain _all_ listed ideas, clearly and concisely.
