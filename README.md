# streambuffer
Utility to allow for buffering Java Streams

This is a simple library that allows you to take a stream of incoming objects, and group them into Lists, to allow them to be processed in batches.

The public API is one method (in the class `io.github.andrew_ie.buffer.StreamBuffer`):

    public static <V> Stream<List<V>> buffer(Stream<V> input, int minSize, int bufferLength);
    
The three arguments are:

1. The source stream
2. The minimum size of any list (other than the first/only one if the buffer is too small)
3. The preferred buffer length

In general, Lists of the size of the preferred buffer length will be returned. However, if, at the end of processing a buffer, there are fewer than minSize elements remaining, they will be added to the end of the buffer.

If the input source is parallel, the output can also be parallel. The parallelism will not attempt to divide the units beyond the preferred buffer length.
