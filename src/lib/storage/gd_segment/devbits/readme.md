What is this folder?
===

It's an attempt to use compile-time optimized compact vectors in a GD Segment. 
Since this requires a template parameter and Hyrise segments cannot define new ones (additionally to the data type),
we had to hide the new DevBits template parameter in an inner class (GdSegmentV1DevBits)