# Partition Index

_Partition Index_ is a persistent data structure to efficiently identify
candidate _data partitions_ for point queries (aka needle-in-a-haystack
queries) in a data lake with millions of partitions, based on size-aligned
growable Cuckoo Filters.

