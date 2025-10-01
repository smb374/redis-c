//
// Created by poyehchen on 9/26/25.
//
#include "ringbuf.h"

#include <cstdint>
#include <gtest/gtest.h>
#include <numeric>
#include <vector>

#define DEFAULT_CAPACITY 16
// Test fixture for the ring buffer. This class provides a fresh
// ring buffer (`rb`) for each test case.
class RingBufTest : public ::testing::Test {
protected:
    RingBuf rb = {};

    // SetUp() is called before each test case.
    void SetUp() override { rb_init(&rb, DEFAULT_CAPACITY); }

    // TearDown() is called after each test case.
    void TearDown() override { rb_destroy(&rb); }
};

// --- Test Cases ---

// Initialization and State

TEST_F(RingBufTest, Initialization) {
    ASSERT_NE(rb.data, nullptr);
    EXPECT_EQ(rb.cap, DEFAULT_CAPACITY);
    EXPECT_EQ(rb.head, 0);
    EXPECT_EQ(rb.tail, 0);
    EXPECT_TRUE(rb_empty(&rb));
    EXPECT_FALSE(rb_full(&rb));
    EXPECT_EQ(rb_size(&rb), 0);
}

TEST_F(RingBufTest, IsFull) {
    // A buffer with capacity N can hold N-1 items.
    std::vector<uint8_t> write_buf(DEFAULT_CAPACITY - 1);
    size_t written = rb_write(&rb, write_buf.data(), write_buf.size());

    ASSERT_EQ(written, DEFAULT_CAPACITY - 1);
    EXPECT_TRUE(rb_full(&rb));
    EXPECT_EQ(rb_size(&rb), DEFAULT_CAPACITY - 1);
}

// Write and Read Operations

TEST_F(RingBufTest, SimpleWriteRead) {
    std::vector<uint8_t> write_buf(8);
    std::iota(write_buf.begin(), write_buf.end(), 0); // Fill with 0, 1, 2...

    size_t written = rb_write(&rb, write_buf.data(), write_buf.size());
    ASSERT_EQ(written, write_buf.size());
    EXPECT_EQ(rb_size(&rb), write_buf.size());
    EXPECT_FALSE(rb_empty(&rb));

    std::vector<uint8_t> read_buf(8);
    size_t read = rb_read(&rb, read_buf.data(), read_buf.size());
    ASSERT_EQ(read, read_buf.size());
    EXPECT_EQ(rb_size(&rb), 0);
    EXPECT_TRUE(rb_empty(&rb));

    EXPECT_EQ(write_buf, read_buf);
}

TEST_F(RingBufTest, WriteToFullBuffer) {
    // Capacity is 16, so it can hold 15 items.
    std::vector<uint8_t> write_buf(20);
    std::iota(write_buf.begin(), write_buf.end(), 0);

    size_t written = rb_write(&rb, write_buf.data(), write_buf.size());
    // Should only write capacity - 1 bytes.
    ASSERT_EQ(written, DEFAULT_CAPACITY - 1);
    EXPECT_TRUE(rb_full(&rb));

    // A subsequent write to a full buffer should write 0 bytes.
    uint8_t extra_byte = 100;
    written = rb_write(&rb, &extra_byte, 1);
    EXPECT_EQ(written, 0);
}

TEST_F(RingBufTest, ReadFromEmptyBuffer) {
    std::vector<uint8_t> read_buf(8, 0);
    size_t read = rb_read(&rb, read_buf.data(), read_buf.size());
    ASSERT_EQ(read, 0);
}

TEST_F(RingBufTest, WriteWrapAround) {
    // Manually move head and tail to test wrap-around logic.
    rb.head = rb.tail = 10;

    std::vector<uint8_t> write_buf(10); // Writing 10 bytes will wrap around.
    std::iota(write_buf.begin(), write_buf.end(), 0);

    size_t written = rb_write(&rb, write_buf.data(), write_buf.size());
    ASSERT_EQ(written, write_buf.size());

    // Check state after wrap-around.
    EXPECT_EQ(rb_size(&rb), write_buf.size());
    EXPECT_EQ(rb.head, 10);
    EXPECT_EQ(rb.tail, (10 + 10) % DEFAULT_CAPACITY); // tail should be 4.

    // Read back the data to verify correctness.
    std::vector<uint8_t> read_buf(10);
    size_t read = rb_read(&rb, read_buf.data(), read_buf.size());
    ASSERT_EQ(read, read_buf.size());

    EXPECT_EQ(write_buf, read_buf);
    EXPECT_TRUE(rb_empty(&rb));
}

TEST_F(RingBufTest, ReadWrapAround) {
    // Fill the buffer completely.
    std::vector<uint8_t> full_buf(DEFAULT_CAPACITY - 1);
    std::iota(full_buf.begin(), full_buf.end(), 0);
    rb_write(&rb, full_buf.data(), full_buf.size());

    // Read half of it, moving the head pointer.
    std::vector<uint8_t> temp_read(8);
    rb_read(&rb, temp_read.data(), temp_read.size());
    ASSERT_EQ(rb.head, 8);

    // Write more data, which will wrap the tail pointer.
    std::vector<uint8_t> wrap_write = {100, 101, 102, 103};
    rb_write(&rb, wrap_write.data(), wrap_write.size());

    // Now, reading all data will force the head pointer to wrap around.
    size_t current_size = rb_size(&rb);
    std::vector<uint8_t> final_read_buf(current_size);
    size_t read = rb_read(&rb, final_read_buf.data(), final_read_buf.size());
    ASSERT_EQ(read, current_size);

    // Construct the expected data sequence.
    std::vector<uint8_t> expected_data;
    for (uint8_t i = 8; i < 15; ++i)
        expected_data.push_back(i); // Data before wrap
    expected_data.insert(expected_data.end(), wrap_write.begin(), wrap_write.end()); // Data after wrap

    EXPECT_EQ(final_read_buf, expected_data);
    EXPECT_TRUE(rb_empty(&rb));
}

// Peek and Consume Operations

TEST_F(RingBufTest, PeekWithOffset) {
    std::vector<uint8_t> write_buf(10);
    std::iota(write_buf.begin(), write_buf.end(), 50);
    rb_write(&rb, write_buf.data(), write_buf.size());

    size_t initial_size = rb_size(&rb);
    size_t initial_head = rb.head;

    // Peek 5 bytes, starting from an offset of 2.
    std::vector<uint8_t> peek_buf(5);
    size_t peeked = rb_peek(&rb, peek_buf.data(), peek_buf.size(), 2);
    ASSERT_EQ(peeked, 5);

    // Verify that peeking does not change the buffer's state.
    EXPECT_EQ(rb_size(&rb), initial_size);
    EXPECT_EQ(rb.head, initial_head);

    // Verify the peeked data is correct.
    std::vector<uint8_t> expected_peek(write_buf.begin() + 2, write_buf.begin() + 7);
    EXPECT_EQ(peek_buf, expected_peek);
}

TEST_F(RingBufTest, PeekWrapAround) {
    // Manually set a wrapped state.
    rb.head = 12;
    rb.tail = 6;
    // Data is at indices: 12, 13, 14, 15, 0, 1, 2, 3, 4, 5. Size = 10.
    std::vector<uint8_t> original_data(10);
    std::iota(original_data.begin(), original_data.end(), 100);
    memcpy(&rb.data[12], original_data.data(), 4);
    memcpy(&rb.data[0], original_data.data() + 4, 6);

    // Peek 6 bytes starting from offset 2. This will require wrapping.
    // The peek should start at index 14.
    std::vector<uint8_t> peek_buf(6);
    size_t peeked = rb_peek(&rb, peek_buf.data(), peek_buf.size(), 2);
    ASSERT_EQ(peeked, 6);

    std::vector<uint8_t> expected_peek(original_data.begin() + 2, original_data.begin() + 8);
    EXPECT_EQ(peek_buf, expected_peek);
}


TEST_F(RingBufTest, Consume) {
    std::vector<uint8_t> write_buf(10);
    std::iota(write_buf.begin(), write_buf.end(), 0);
    rb_write(&rb, write_buf.data(), write_buf.size());

    rb_consume(&rb, 4);

    EXPECT_EQ(rb_size(&rb), 6);
    EXPECT_EQ(rb.head, 4);

    // Check that the next read starts from the new head position.
    std::vector<uint8_t> read_buf(6);
    rb_read(&rb, read_buf.data(), read_buf.size());

    std::vector<uint8_t> expected_data(write_buf.begin() + 4, write_buf.end());
    EXPECT_EQ(read_buf, expected_data);
}

// Utility Operations

TEST_F(RingBufTest, Clear) {
    rb_write(&rb, std::vector<uint8_t>(10).data(), 10);
    ASSERT_FALSE(rb_empty(&rb));

    rb_clear(&rb);

    EXPECT_TRUE(rb_empty(&rb));
    EXPECT_EQ(rb_size(&rb), 0);
    EXPECT_EQ(rb.head, 0);
    EXPECT_EQ(rb.tail, 0);
}

TEST_F(RingBufTest, ResizeLarger) {
    std::vector<uint8_t> write_buf(10);
    std::iota(write_buf.begin(), write_buf.end(), 0);
    rb_write(&rb, write_buf.data(), write_buf.size());

    rb_resize(&rb, 32);

    ASSERT_EQ(rb.cap, 32);
    EXPECT_EQ(rb_size(&rb), 10);
    // Data should be linearized after resize.
    EXPECT_EQ(rb.head, 0);
    EXPECT_EQ(rb.tail, 10);

    // Verify data integrity.
    std::vector<uint8_t> read_buf(10);
    rb_read(&rb, read_buf.data(), read_buf.size());
    EXPECT_EQ(write_buf, read_buf);
}

TEST_F(RingBufTest, ResizeLargerWithWrap) {
    // Manually set a wrapped state.
    rb.head = 12;
    rb.tail = 6;
    std::vector<uint8_t> original_data(10);
    std::iota(original_data.begin(), original_data.end(), 100);
    memcpy(&rb.data[12], original_data.data(), 4);
    memcpy(&rb.data[0], original_data.data() + 4, 6);
    ASSERT_EQ(rb_size(&rb), 10);

    rb_resize(&rb, 32);

    ASSERT_EQ(rb.cap, 32);
    EXPECT_EQ(rb_size(&rb), 10);
    // Data should be linearized.
    EXPECT_EQ(rb.head, 0);
    EXPECT_EQ(rb.tail, 10);

    // Verify data integrity.
    std::vector<uint8_t> read_buf(10);
    rb_read(&rb, read_buf.data(), read_buf.size());
    EXPECT_EQ(original_data, read_buf);
}

TEST_F(RingBufTest, ResizeSmallerInvalid) {
    rb_write(&rb, std::vector<uint8_t>(10).data(), 10);
    ASSERT_EQ(rb_size(&rb), 10);

    // Resize should fail because new_cap must be >= size + 1.
    // 10 is not >= 10 + 1.
    rb_resize(&rb, 10);

    // Verify that the buffer is unchanged.
    EXPECT_EQ(rb.cap, DEFAULT_CAPACITY);
    EXPECT_EQ(rb_size(&rb), 10);
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
