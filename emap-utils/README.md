# Emap Utils
A place for code that needs to be shared between two or more modules and doesn't belong
elsewhere.

If possible, code should go in more specific modules such as emap-interchange or emap-star.
We managed without emap-utils for several years, so I don't expect much stuff to
go here!

This module is intended to be lightweight without too many dependencies.

You can still use package names to group things. Eg. LocationMapping is only
likely to be shared between waveform-reader and waveform-generator, so that can go in
`uk.ac.ucl.rits.inform.datasources.waveform`.
