# `rsf` - respository snapshot format

## Purpose

The repository snapshot format used by Posit Package Manager. This format is
still in development, and will support an upcoming Posit Package Manager
release.

## About

The repository snapshot format, or `rsf`, is a simple, flexible file format
that is ideal for storing repository snapshot information. We created this
format for several reasons:

* **Speed**. We previously used JSON and then partially copied the data into
  database tables as part of the syncronization process. We decided to
  eliminate database duplication since all the required data already
  resides in the JSON manifest. However, JSON deserialization proved too slow,
  even with faster parsers like
  [github.com/pkg/json](https://github.com/pkg/json).
* **Indexing**. Iterating a large list of snapshots and packages efficiently
  requires indexing to eliminate fully deserializing all unwanted data. The
  `rsf` format prefixes each record with a size to support quickly skipping
  irrelevant records. Additionally, `rsf` supports array indexing to support
  quickly finding the correct record in an array.
* **Streaming**. While it is easy to encode variables efficiently with formats
  like Go's gob encoding, decoding often requires fully reading an object
  into memory. Since we require encoding large data sets in a single file,
  we needed support for seeking relevant data, while streaming information
  to a consumer without storing all records in memory.
