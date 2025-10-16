// /home/poyehchen/Projects/c/build_your_own_redis/tests/kvstore_test.cpp
#include "kvstore.h"

#include <cstdio>
#include <gtest/gtest.h>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include "debra.h"
#include "parse.h"
#include "ringbuf.h"
#include "serialize.h"
#include "utils.h"


// --- Test Fixture ---
class KVStoreTest : public ::testing::Test {
protected:
    KVStore *kv = nullptr;
    RingBuf out;

    void SetUp() override {
        gc_init();
        gc_reg();
        kv = kv_new(nullptr);
        rb_init(&out, 1024);
    }

    void TearDown() override {
        kv_clear(kv);
        rb_destroy(&out);
        gc_unreg();
    }

    // Helper to create a simple_req from a vector of strings
    static OwnedRequest create_req(const std::vector<std::string> &args) {
        OwnedRequest oreq;
        oreq.is_alloc = false;
        oreq.base.argc = args.size();
        oreq.base.argv = (vstr **) malloc(oreq.base.argc * sizeof(vstr *));
        for (size_t i = 0; i < oreq.base.argc; ++i) {
            oreq.base.argv[i] = vstr_new(args[i].c_str(), args[i].length());
        }
        simple2req(&oreq.base, &oreq.req);
        return oreq;
    }

    static void free_req(OwnedRequest &req) { owned_req_destroy(&req); }

    // Helper to read and verify a string from the RingBuf
    std::string read_out_str() {
        uint8_t tag;
        rb_read(&out, &tag, 1);
        EXPECT_EQ(tag, TAG_STR);

        uint32_t len;
        rb_read(&out, (uint8_t *) &len, 4);

        std::vector<char> buf(len);
        rb_read(&out, (uint8_t *) buf.data(), len);
        return std::string(buf.begin(), buf.end());
    }

    void verify_out_str(const std::string &expected) {
        uint8_t tag;
        rb_read(&out, &tag, 1);
        ASSERT_EQ(tag, TAG_STR);

        uint32_t len;
        rb_read(&out, (uint8_t *) &len, 4);
        ASSERT_EQ(len, expected.length());

        std::vector<char> buf(len);
        rb_read(&out, (uint8_t *) buf.data(), len);
        ASSERT_EQ(std::string(buf.begin(), buf.end()), expected);
    }

    // Helper to read and verify a nil from the RingBuf
    void verify_out_nil() {
        uint8_t tag;
        rb_read(&out, &tag, 1);
        ASSERT_EQ(tag, TAG_NIL);
    }

    // Helper to read and verify an integer from the RingBuf
    void verify_out_int(int64_t expected) {
        uint8_t tag;
        rb_read(&out, &tag, 1);
        ASSERT_EQ(tag, TAG_INT);

        int64_t val;
        rb_read(&out, (uint8_t *) &val, 8);
        ASSERT_EQ(val, expected);
    }

    double read_out_dbl() {
        uint8_t tag;
        rb_read(&out, &tag, 1);
        EXPECT_EQ(tag, TAG_DBL);
        double val;
        rb_read(&out, (uint8_t *) &val, 8);
        return val;
    }

    // Helper to create a heap-allocated OwnedRequest
    static OwnedRequest *create_heap_req(const std::vector<std::string> &args) {
        auto *oreq = (OwnedRequest *) calloc(1, sizeof(OwnedRequest));
        oreq->is_alloc = true; // Mark for freeing by the worker
        oreq->base.argc = args.size();
        oreq->base.argv = (vstr **) malloc(oreq->base.argc * sizeof(vstr *));
        for (size_t i = 0; i < oreq->base.argc; ++i) {
            oreq->base.argv[i] = vstr_new(args[i].c_str(), args[i].length());
        }
        simple2req(&oreq->base, &oreq->req);
        return oreq;
    }
};

TEST_F(KVStoreTest, GetSetDel) {
    // SET key value
    OwnedRequest set_req = create_req({"set", "mykey", "myvalue"});
    do_owned_req(kv, &set_req, &out);
    verify_out_nil();
    free_req(set_req);

    // GET key
    OwnedRequest get_req = create_req({"get", "mykey"});
    do_owned_req(kv, &get_req, &out);
    verify_out_str("myvalue");
    free_req(get_req);

    // DEL key
    OwnedRequest del_req = create_req({"del", "mykey"});
    do_owned_req(kv, &del_req, &out);
    verify_out_int(1);
    free_req(del_req);

    // GET deleted key
    OwnedRequest get_after_del_req = create_req({"get", "mykey"});
    do_owned_req(kv, &get_after_del_req, &out);
    verify_out_nil();
    free_req(get_after_del_req);
}

TEST_F(KVStoreTest, KeysCommand) {
    OwnedRequest set_req1 = create_req({"set", "key1", "val1"});
    do_owned_req(kv, &set_req1, &out);
    rb_clear(&out);
    free_req(set_req1);

    OwnedRequest set_req2 = create_req({"set", "key2", "val2"});
    do_owned_req(kv, &set_req2, &out);
    rb_clear(&out);
    free_req(set_req2);

    OwnedRequest zadd_req = create_req({"zadd", "zkey1", "10", "member1"});
    do_owned_req(kv, &zadd_req, &out);
    rb_clear(&out);
    free_req(zadd_req);

    OwnedRequest keys_req = create_req({"keys"});
    do_owned_req(kv, &keys_req, &out);

    uint8_t tag;
    rb_read(&out, &tag, 1);
    ASSERT_EQ(tag, TAG_ARR);

    uint32_t count;
    rb_read(&out, (uint8_t *) &count, 4);
    ASSERT_EQ(count, 3);

    std::set<std::string> keys;
    for (uint32_t i = 0; i < count; ++i) {
        keys.insert(read_out_str());
    }

    ASSERT_EQ(keys.count("key1"), 1);
    ASSERT_EQ(keys.count("key2"), 1);
    ASSERT_EQ(keys.count("zkey1"), 1);

    free_req(keys_req);
}

TEST_F(KVStoreTest, ZSetOperations) {
    // ZADD zkey 100 member1
    OwnedRequest zadd_req = create_req({"zadd", "myzset", "100", "member1"});
    do_owned_req(kv, &zadd_req, &out);
    verify_out_int(1);
    free_req(zadd_req);

    // ZSCORE zkey member1
    OwnedRequest zscore_req = create_req({"zscore", "myzset", "member1"});
    do_owned_req(kv, &zscore_req, &out);
    uint8_t tag;
    rb_read(&out, &tag, 1);
    ASSERT_EQ(tag, TAG_DBL);
    double score;
    rb_read(&out, (uint8_t *) &score, 8);
    ASSERT_EQ(score, 100.0);
    free_req(zscore_req);

    // ZREM zkey member1
    OwnedRequest zrem_req = create_req({"zrem", "myzset", "member1"});
    do_owned_req(kv, &zrem_req, &out);
    verify_out_int(1);
    free_req(zrem_req);

    // ZSCORE of removed member
    OwnedRequest zscore_after_rem_req = create_req({"zscore", "myzset", "member1"});
    do_owned_req(kv, &zscore_after_rem_req, &out);
    verify_out_nil();
    free_req(zscore_after_rem_req);
}

TEST_F(KVStoreTest, ZQueryCommand) {
    OwnedRequest zadd_req1 = create_req({"zadd", "zquerykey", "10", "a"});
    do_owned_req(kv, &zadd_req1, &out);
    rb_clear(&out);
    free_req(zadd_req1);

    OwnedRequest zadd_req2 = create_req({"zadd", "zquerykey", "20", "b"});
    do_owned_req(kv, &zadd_req2, &out);
    rb_clear(&out);
    free_req(zadd_req2);

    OwnedRequest zadd_req3 = create_req({"zadd", "zquerykey", "20", "c"});
    do_owned_req(kv, &zadd_req3, &out);
    rb_clear(&out);
    free_req(zadd_req3);

    OwnedRequest zadd_req4 = create_req({"zadd", "zquerykey", "30", "d"});
    do_owned_req(kv, &zadd_req4, &out);
    rb_clear(&out);
    free_req(zadd_req4);

    // zquery zquerykey 20 c 0 2
    // limit is 2, so it should return 1 pair (c, 20)
    OwnedRequest zquery_req = create_req({"zquery", "zquerykey", "20", "c", "0", "2"});
    do_owned_req(kv, &zquery_req, &out);

    uint8_t tag;
    rb_read(&out, &tag, 1);
    ASSERT_EQ(tag, TAG_ARR);

    uint32_t count;
    rb_read(&out, (uint8_t *) &count, 4);
    ASSERT_EQ(count, 2); // 1 pair of name/score

    ASSERT_EQ(read_out_str(), "c");
    ASSERT_EQ(read_out_dbl(), 20.0);

    free_req(zquery_req);

    rb_clear(&out);

    // zquery zquerykey 10 a 1 4
    // start from (10, a), offset 1 -> (20, b)
    // limit is 4, so it should return 2 pairs (b, 20), (c, 20)
    OwnedRequest zquery_req2 = create_req({"zquery", "zquerykey", "10", "a", "1", "4"});
    do_owned_req(kv, &zquery_req2, &out);

    rb_read(&out, &tag, 1);
    ASSERT_EQ(tag, TAG_ARR);

    rb_read(&out, (uint8_t *) &count, 4);
    ASSERT_EQ(count, 4); // 2 pairs of name/score

    ASSERT_EQ(read_out_str(), "b");
    ASSERT_EQ(read_out_dbl(), 20.0);
    ASSERT_EQ(read_out_str(), "c");
    ASSERT_EQ(read_out_dbl(), 20.0);

    free_req(zquery_req2);
}

TEST_F(KVStoreTest, Expiration) {
    // SET key value
    OwnedRequest set_req = create_req({"set", "tempkey", "tempvalue"});
    do_owned_req(kv, &set_req, &out);
    verify_out_nil();
    free_req(set_req);

    // PEXPIRE key 50
    OwnedRequest expire_req = create_req({"pexpire", "tempkey", "50"});
    do_owned_req(kv, &expire_req, &out);
    verify_out_int(1);
    free_req(expire_req);

    // PTTL key
    OwnedRequest pttl_req = create_req({"pttl", "tempkey"});
    do_owned_req(kv, &pttl_req, &out);
    uint8_t tag;
    rb_read(&out, &tag, 1);
    ASSERT_EQ(tag, TAG_INT);
    int64_t ttl;
    rb_read(&out, (uint8_t *) &ttl, 8);
    ASSERT_GT(ttl, 0);
    ASSERT_LE(ttl, 50);
    free_req(pttl_req);

    // Wait for key to expire
    usleep(60 * 1000); // 60ms

    // Manually process expired keys
    kv_clean_expired(kv);

    // GET expired key
    OwnedRequest get_expired_req = create_req({"get", "tempkey"});
    do_owned_req(kv, &get_expired_req, &out);
    verify_out_nil();
    free_req(get_expired_req);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
