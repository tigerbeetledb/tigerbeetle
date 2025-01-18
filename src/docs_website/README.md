# docs.tigerbeetle.com

Documentation generator for <docs.tigerbeetle.com>. Static website is generated via `zig build`
and is pushed to <https://github.com/tigerbeetle/docs>, which is then hosted on GitHub pages.

Overview of the build process:

* Copy over markdown files from `/docs` and `/src/clients/$lang/README.md`
* Massage markdown links in place to use relative links for things hosted on the docs website, and
  `https://github.com/tigerbeetle/tigerbeetle` links for everything else.
* Run link checker on the resulting markdown files
* Run `zig build` to produce the static HTML files in the `./build` directory.

This process is triggered by `ci.zig` in our merge queue (mostly to detect broken links) and by
`release.zig` to push the rendered docs to <https://github.com/tigerbeetle/docs>.
