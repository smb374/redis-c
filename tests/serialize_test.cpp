//
// Created by poyehchen on 9/29/25.
//
#include "ringbuf.h"
#include "utils.h" // For vstr and tlv_tag
#include "serialize.h"

#include <gtest/gtest.h>
#include <vector>
#include <string>

// --- Helper function to verify ring buffer contents ---
void verify_buffer(RingBuf* rb, const std::vector<uint8_t>& expected) {
    ASSERT_EQ(rb_size(rb), expected.size());
    std::vector<uint8_t> actual(expected.size());
    rb_read(rb, actual.data(), actual.size());
    EXPECT_EQ(actual, expected);
}

// --- Test Fixture ---
class SerializeTest : public ::testing::Test {
protected:
    RingBuf rb = {};

    void SetUp() override {
        // Start with a small capacity to ensure resizing logic is triggered.
        rb_init(&rb, 8);
    }

    void TearDown() override {
        rb_destroy(&rb);
    }
};

// --- Test Cases ---

TEST_F(SerializeTest, OutNil) {
    out_nil(&rb);
    verify_buffer(&rb, {TAG_NIL});
}

TEST_F(SerializeTest, OutStr) {
    const char* test_str = "hello";
    size_t test_len = 5;
    out_str(&rb, test_str, test_len);

    std::vector<uint8_t> expected = {
        TAG_STR,
        0x05, 0x00, 0x00, 0x00, // length (5)
        'h', 'e', 'l', 'l', 'o' // data
    };
    verify_buffer(&rb, expected);
    EXPECT_GT(rb.cap, 8) << "Buffer should have resized";
}

TEST_F(SerializeTest, OutVStr) {
    const char* test_str = "world";
    vstr* v = vstr_new(test_str, 5);
    ASSERT_NE(v, nullptr);
    out_vstr(&rb, v);
    free(v);

    std::vector<uint8_t> expected = {
        TAG_STR,
        0x05, 0x00, 0x00, 0x00, // length (5)
        'w', 'o', 'r', 'l', 'd' // data
    };
    verify_buffer(&rb, expected);
}

TEST_F(SerializeTest, OutInt) {
    int64_t val = 0x11223344AABBCCDDLL;
    out_int(&rb, val);

    std::vector<uint8_t> expected = {
        TAG_INT,
        0xDD, 0xCC, 0xBB, 0xAA, 0x44, 0x33, 0x22, 0x11
    };
    verify_buffer(&rb, expected);
    EXPECT_GT(rb.cap, 8) << "Buffer should have resized";
}

TEST_F(SerializeTest, OutDbl) {
    double val = 3.14159;
    out_dbl(&rb, val);

    // Convert double to byte representation for comparison
    std::vector<uint8_t> expected_bytes;
    expected_bytes.push_back(TAG_DBL);
    uint8_t dbl_bytes[8];
    memcpy(dbl_bytes, &val, 8);
    expected_bytes.insert(expected_bytes.end(), dbl_bytes, dbl_bytes + 8);

    verify_buffer(&rb, expected_bytes);
    EXPECT_GT(rb.cap, 8) << "Buffer should have resized";
}

TEST_F(SerializeTest, OutErr) {
    uint32_t err_code = 404;
    out_err(&rb, err_code, "Not Found");

    std::vector<uint8_t> expected = {
        TAG_ERR,
        0x94, 0x01, 0x00, 0x00, // error code (404)
        0x09, 0x00, 0x00, 0x00, // message length (9)
        'N', 'o', 't', ' ', 'F', 'o', 'u', 'n', 'd'
    };
    verify_buffer(&rb, expected);
    EXPECT_GT(rb.cap, 8) << "Buffer should have resized";
}

TEST_F(SerializeTest, OutArrAndBuf) {
    // This test demonstrates how to build a complex type like an array.

    // 1. Write the array header to the main buffer
    out_arr(&rb, 2); // An array of 2 items

    // 2. Serialize the first item (a string) into a temporary buffer
    RingBuf temp_rb;
    rb_init(&temp_rb, 16);
    out_str(&temp_rb, "item1", 5);

    // 3. Append the serialized item to the main buffer
    out_buf(&rb, &temp_rb);

    // 4. Serialize the second item (an integer) into the temp buffer
    rb_clear(&temp_rb);
    out_int(&temp_rb, 12345);

    // 5. Append the second item
    out_buf(&rb, &temp_rb);
    rb_destroy(&temp_rb);

    // 6. Verify the final combined buffer
    std::vector<uint8_t> expected = {
        // Array header
        TAG_ARR,
        0x02, 0x00, 0x00, 0x00, // n=2
        // First item (string)
        TAG_STR,
        0x05, 0x00, 0x00, 0x00, // len=5
        'i', 't', 'e', 'm', '1',
        // Second item (integer)
        TAG_INT,
        0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 // 12345
    };
    verify_buffer(&rb, expected);
    EXPECT_GT(rb.cap, 8) << "Buffer should have resized";
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}