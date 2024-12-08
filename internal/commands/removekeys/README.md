= S3 Obliterator

Time and time again we have run into the need to purge large chunks of S3.

There are simple ways (using expiry), but they're often not granular enough.

Clicking on the UI / running AWS commands works, but it often doesn't have enough safety checks.

Enter the obliterator.

. Uses AWS SDK for credentials
. Validates that what you want to do is sensible
. Runs at maximum speed
.. Multi-threaded
.. Handles rate limiting
.. Can chunk a key prefix into parts to run faster




== Key format

The `remove-keys` sub-command takes a file filled with key prefixes to remove.

* `a` - removes everything with a prefix of `a/`
* `a/b` - removes everything with a prefix of `a/b/`
* `a/b*` - removes everything with a prefix of `a/b` (this will remove a/b/c.txt for example)

The reason for this syntax is to make prefixing more predictable.  We ran into cases in testing where we would say "remove everything starting with 123" and inadvertently remove "1234".  Thus we make that behaviour explicit - `123` becomes `123/` and if you actually wanted to remove `1234` you would request removal of `123*`

=== What if I want to remove a specific file

You don't need this program.

== S3 versioning

Not handled at present, we just check that versioning is disabled on the bucket before we run.

At a point in the future, removing all versions will be possible.