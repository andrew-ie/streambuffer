package io.github.andrew_ie.buffer.test;

import io.github.andrew_ie.buffer.StreamBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Unit test for the stream buffer
 */
public class StreamBufferTest {
    /**
     * Test that no error occurs if an empty list is returned
     */
    @Test
    public void testEmpty() {
        Stream<String> stream = Stream.empty();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 1,5);
        Assert.assertTrue(listStream.collect(Collectors.toList()).isEmpty());
    }

    /**
     * Test that if we do not provide enough elements to split at, only one list is returned
     */
    @Test
    public void testNoSplit() {
        List<String> input = Arrays.asList("E1", "E2", "E3", "E4");
        Stream<String> stream = input.stream();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 1, 5);
        List<List<String>> collected = listStream.collect(Collectors.toList());
        Assert.assertEquals(1, collected.size());
        Assert.assertEquals(input.size(), collected.get(0).size());
        Assert.assertEquals(input, collected.get(0));
    }

    /**
     * Test that if we provide exactly the maximum elements for one element, it correctly
     * just returns one element
     */
    @Test
    public void testNoSplitMaxSize() {
        List<String> input = Arrays.asList("E1", "E2", "E3", "E4");
        Stream<String> stream = input.stream();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 1, 4);
        List<List<String>> collected = listStream.collect(Collectors.toList());
        Assert.assertEquals(1, collected.size());
        Assert.assertEquals(input.size(), collected.get(0).size());
        Assert.assertEquals(input, collected.get(0));
    }

    /**
     * Test that we correctly split the elements if the input stream is too large
     */
    @Test
    public void testSplit() {
        List<String> input = Arrays.asList("E1", "E2", "E3", "E4", "E5", "E6");
        Stream<String> stream = input.stream();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 1,5);
        List<List<String>> collected = listStream.collect(Collectors.toList());
        Assert.assertEquals(2, collected.size());
        Assert.assertEquals(5, collected.get(0).size());
        Assert.assertEquals(1, collected.get(1).size());
        Assert.assertEquals(input.subList(0, 5), collected.get(0));
        Assert.assertEquals(input.subList(5, 6), collected.get(1));
    }

    /**
     * Test that we absorb any overflow in a minimum size scenario
     */
    @Test
    public void testMinSize() {
        List<String> input = Arrays.asList("E1", "E2", "E3", "E4", "E5", "E6");
        Stream<String> stream = input.stream();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 2,5);
        List<List<String>> collected = listStream.collect(Collectors.toList());
        Assert.assertEquals(1, collected.size());
        Assert.assertEquals(6, collected.get(0).size());
        Assert.assertEquals(input, collected.get(0));
    }

    /**
     * Test that if we specify a minimum size, that we split correctly
     */
    @Test
    public void testSplitMinSize() {
        List<String> input = Arrays.asList("E1", "E2", "E3", "E4", "E5", "E6");
        Stream<String> stream = input.stream();
        Stream<List<String>> listStream = StreamBuffer.buffer(stream, 2,4);
        List<List<String>> collected = listStream.collect(Collectors.toList());
        Assert.assertEquals(2, collected.size());
        Assert.assertEquals(4, collected.get(0).size());
        Assert.assertEquals(2, collected.get(1).size());
        Assert.assertEquals(input.subList(0, 4), collected.get(0));
        Assert.assertEquals(input.subList(4, 6), collected.get(1));
    }

    /**
     * Verify that the parallel application works as expected
     */
    @Test
    public void testParallel() {
        final int expectedSize = 5000;
        Stream<Long> largeArray = LongStream.range(0, expectedSize).boxed().parallel();
        Stream<List<Long>> listStream = StreamBuffer.buffer(largeArray, 5, 5);
        Stream<List<Long>> parallel = listStream.parallel();
        List<List<Long>> groups = parallel.collect(Collectors.toList());
        Assert.assertTrue(groups.stream().allMatch(this::isConsecutive));
        List<Long> resultingValues = groups.stream().flatMap(List::stream).collect(Collectors.toList());
        Assert.assertEquals(expectedSize, resultingValues.size());
    }

    /**
     * Verify that the groups provided are all consecutive (even if the resulting end stream isn't)
     * @param values the values
     * @return the consecutive result
     */
    private boolean isConsecutive(List<Long> values) {
        long start = values.get(0);
        boolean valid = true;
        for (int i = 1 ; valid && i < values.size() ; i++) {
            valid = values.get(i) == start + i;
        }
        return valid;
    }
}
