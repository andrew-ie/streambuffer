package dev.acraig.util.streambuffer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Buffer an input stream into a batch group.
 * This allows for applying operations on a chunk of data from a stream without having to read the entire stream.
 * All of the characteristics of the parent stream are applicable to this stream
 * @param <T> the datatype
 */
public class StreamBuffer<T> extends Spliterators.AbstractSpliterator<List<T>> {
    /**
     * The source supplier that's being used
     */
    private final Spliterator<T> source;
    /**
     * The maximum preferredBufferLength of each buffer
     */
    private final int preferredBufferLength;
    /**
     * The minimum size to return. If there are fewer elements than this remaining, then
     * they will be added to the previous list.
     */
    private final int minSize;
    /**
     * Pre-buffer to allow for checking that the minimum size condition is kept
     */
    private final BlockingQueue<T> preBuffer;

    /**
     * Constructor
     * @param source the source of the stream
     * @param minSize the size that it will absorb extra at the end if there's only a few remaining.
     *                Note - if there are fewer elements in the stream than this minimum size, then a single
     *                list will be returned of that size. It is not deemed an error.
     * @param estimatedSize the estimated preferredBufferLength of the new Stream
     * @param preferredBufferLength the maximum buffer preferredBufferLength
     */
    private StreamBuffer(final Spliterator<T> source, final long estimatedSize, final int minSize, final int preferredBufferLength) {
        super(estimatedSize, source.characteristics());
        this.source = source;
        this.minSize = minSize;
        if (minSize > 1) {
            preBuffer = new ArrayBlockingQueue<>(minSize);
        } else {
            preBuffer = null;
        }
        this.preferredBufferLength = preferredBufferLength;
    }

    /**
     * Try to advance to the next element in the stream
     * @param action the action to send the next element to
     * @return true if there were elements to send, false otherwise
     */
    @Override
    public boolean tryAdvance(final Consumer<? super List<T>> action) {
        List<T> elements = new ArrayList<>(preferredBufferLength);
        if (preBuffer != null) {
            preBuffer.drainTo(elements);
        }
        boolean hasElements;
        if (elements.size() < preferredBufferLength) { // if minSize == preferred length
            do {
                hasElements = source.tryAdvance(elements::add);
            } while (hasElements && elements.size() < preferredBufferLength);
        } else {
            hasElements = true;
        }
        if (preBuffer != null) {
            //check to see if there are any additional elements present
            //that may result in too small a list being formed.
            for (int i = 0 ; hasElements && i < minSize ; i++) {
                hasElements = source.tryAdvance(preBuffer::offer);
            }
            if (!hasElements) { //fewer than minimum size returned
                preBuffer.drainTo(elements);
            }
        }
        if (!elements.isEmpty()) {
            action.accept(elements);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Comparator - compare the first elements in each list to each other
     * @return the comparison
     */
    @Override
    public Comparator<? super List<T>> getComparator() {
        Comparator<? super T> sourceComparator = source.getComparator();
        return (o1, o2) -> sourceComparator.compare(o1.get(0), o2.get(0));
    }

    /**
     * Try and split this iterator. It will only succeed in splitting the iterator
     * if the source stream can also be split - as the split by this buffer will split based on
     * however the parent split works
     * @return the split version
     */
    @Override
    public Spliterator<List<T>> trySplit() {
        if (source.estimateSize() <= preferredBufferLength * 2) { //Don't split if we think the size will be too small
            return null;
        }
        else {
            Spliterator<T> candidate = source.trySplit();
            if (candidate != null) {
                return new StreamBuffer<>(candidate, estimateSize(this.preferredBufferLength, candidate), this.minSize,
                        this.preferredBufferLength);
            }
            else {
                return null;
            }
        }
    }

    /**
     * Convert this buffer into a stream
     * @return the stream representation of this buffer
     */
    private Stream<List<T>> stream() {
        return StreamSupport.stream(this, false);
    }

    /**
     * Factory method to generate a buffer from an input source
     * @param input the input to read through
     * @param minSize the minimum size of the lists to return (except for the first list).
     * @param bufferLength the maximum preferred buffer length of the buffer
     * @param <V> the object type
     * @return the new stream of buffered elements.
     */
    public static <V> Stream<List<V>> buffer(Stream<V> input, int minSize, int bufferLength) {
        Spliterator<V> spliterator = input.spliterator();
        long estimatedSize = estimateSize(bufferLength, spliterator);
        return new StreamBuffer<>(spliterator, estimatedSize, minSize, bufferLength).stream();
    }

    /**
     * Estimate the bufferLength of this spliterator.  This uses the information
     * from the source spliterator, and divides it by the preferred buffer length.
     * @param bufferLength the length of the buffer that will be used
     * @param spliterator the spliterator being used
     * @return the estimated preferredBufferLength, or {@link Long#MAX_VALUE} if it can't be determined
     */
    private static long estimateSize(final int bufferLength, final Spliterator<?> spliterator) {
        long estimatedSize = spliterator.estimateSize();
        if (estimatedSize != Long.MAX_VALUE) {
            estimatedSize = estimatedSize / bufferLength;
        }
        return estimatedSize;
    }

}
