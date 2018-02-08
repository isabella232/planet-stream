# Planet-Stream

Works with the Protocol Buffer Binary Format (PBF) of [OpenStreetMap 
planetfiles](https://planet.openstreetmap.org/) to break up a large file
into many smaller, standalone files that can be used like their larger
counterparts for import and other data pipeline uses.

This can greatly speed up planetfile import, as it allows for multiple
chunks to be imported through several different threads or on different
machines running parallel operations.

Additionally, this package provides multiple streaming modes for remote
servers which provide 
[HTTP Range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range)
support, allowing for data to be imported concurrently with its transfer.

This package only deals with PBFs at the Block level, and does not
directly interact with or modify more sophisticated datatypes of the
OSMPBF format corresponding to the map data itself, such as
PrimitiveGroups, Nodes, Ways, or Relations.

If you'd like to interact with the map-related data using Go, you
can do so using the [OSMPBF library](https://github.com/qedus/osmpbf).

For more information on the Protocol Buffer Binary Format for OSM
Planetfiles, check out the [OSM wiki](https://wiki.openstreetmap.org/wiki/PBF_Format).

## Project Status

This is very much a Work-in-Progress designed for a very specific use case.