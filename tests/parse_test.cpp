#include "ringbuf.h"
#include "utils.h"
#include "parse.h"

#include <cmath> // For isnan
#include <gtest/gtest.h>
#include <string>
#include <vector>

// --- Tests for Helper Functions ---

TEST(StrConvTest, Str2Int) {
    int64_t out;
    vstr* v = vstr_new("12345", 5);
    ASSERT_TRUE(str2int(v, &out));
    EXPECT_EQ(out, 12345);
    vstr_destroy(v);

    v = vstr_new("-987", 4);
    ASSERT_TRUE(str2int(v, &out));
    EXPECT_EQ(out, -987);
    vstr_destroy(v);

    // strtoll returns 0 for non-numeric strings
    v = vstr_new("hello", 5);
    ASSERT_TRUE(str2int(v, &out));
    EXPECT_EQ(out, 0);
    vstr_destroy(v);
}

TEST(StrConvTest, Str2Dbl) {
    double out;
    vstr* v = vstr_new("123.45", 6);
    ASSERT_TRUE(str2dbl(v, &out));
    EXPECT_DOUBLE_EQ(out, 123.45);
    vstr_destroy(v);

    v = vstr_new("-9.87e2", 7);
    ASSERT_TRUE(str2dbl(v, &out));
    EXPECT_DOUBLE_EQ(out, -987.0);
    vstr_destroy(v);

    // The function should detect NaN and return false
    v = vstr_new("nan", 3);
    ASSERT_FALSE(str2dbl(v, &out));
    EXPECT_TRUE(std::isnan(out));
    vstr_destroy(v);
}


// --- Test Fixture for parse_simple_req ---

// Helper to build the raw byte buffer for a request
void build_req_buffer(const std::vector<std::string>& cmds, std::vector<uint8_t>& out) {
    out.clear();
    uint32_t nstr = cmds.size();
    out.insert(out.end(), (uint8_t*)&nstr, (uint8_t*)&nstr + 4);

    for (const auto& cmd : cmds) {
        uint32_t len = cmd.length();
        out.insert(out.end(), (uint8_t*)&len, (uint8_t*)&len + 4);
        out.insert(out.end(), cmd.begin(), cmd.end());
    }
}

class ParseSimpleReqTest : public ::testing::Test {
protected:
    RingBuf rb;
    simple_req parsed_req = {0, nullptr};

    void SetUp() override {
        rb_init(&rb, 1024); // Init with a reasonable capacity
    }

    void TearDown() override {
        // Must clean up memory allocated by parse_simple_req
        if (parsed_req.argv) {
            for (size_t i = 0; i < parsed_req.argc; ++i) {
                vstr_destroy(parsed_req.argv[i]);
            }
            free(parsed_req.argv);
        }
        rb_destroy(&rb);
    }
};

// --- Test Cases for parse_simple_req ---

TEST_F(ParseSimpleReqTest, ParseValidRequest) {
    std::vector<std::string> cmds = {"SET", "mykey", "myvalue"};
    std::vector<uint8_t> buffer;
    build_req_buffer(cmds, buffer);
    rb_write(&rb, buffer.data(), buffer.size());

    ssize_t ret = parse_simple_req(&rb, buffer.size(), &parsed_req);

    ASSERT_EQ(ret, 0);
    ASSERT_EQ(parsed_req.argc, 3);
    ASSERT_NE(parsed_req.argv, nullptr);

    EXPECT_EQ(parsed_req.argv[0]->len, 3);
    EXPECT_EQ(strncmp(parsed_req.argv[0]->dat, "SET", 3), 0);

    EXPECT_EQ(parsed_req.argv[1]->len, 5);
    EXPECT_EQ(strncmp(parsed_req.argv[1]->dat, "mykey", 5), 0);
}

TEST_F(ParseSimpleReqTest, ParseEmptyRequest) {
    std::vector<std::string> cmds = {};
    std::vector<uint8_t> buffer;
    build_req_buffer(cmds, buffer);
    rb_write(&rb, buffer.data(), buffer.size());

    ssize_t ret = parse_simple_req(&rb, buffer.size(), &parsed_req);

    ASSERT_EQ(ret, 0);
    EXPECT_EQ(parsed_req.argc, 0);
    EXPECT_EQ(parsed_req.argv, nullptr);
}

TEST_F(ParseSimpleReqTest, FailOnIncompleteData) {
    std::vector<std::string> cmds = {"DEL", "a_key"};
    std::vector<uint8_t> buffer;
    build_req_buffer(cmds, buffer);

    // Truncate the buffer to simulate an incomplete read
    size_t truncated_size = buffer.size() - 2;
    rb_write(&rb, buffer.data(), truncated_size);

    ssize_t ret = parse_simple_req(&rb, truncated_size, &parsed_req);
    ASSERT_EQ(ret, -1);
}

TEST_F(ParseSimpleReqTest, FailOnTrailingData) {
    std::vector<std::string> cmds = {"PING"};
    std::vector<uint8_t> buffer;
    build_req_buffer(cmds, buffer);
    rb_write(&rb, buffer.data(), buffer.size());

    // Write extra junk data to the buffer
    uint8_t junk[] = {0xDE, 0xAD};
    rb_write(&rb, junk, 2);

    ssize_t ret = parse_simple_req(&rb, buffer.size() + 2, &parsed_req);
    ASSERT_EQ(ret, -1);
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}